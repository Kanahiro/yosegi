import argparse
import json
from dataclasses import dataclass

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class Args:
    input_file: str
    output_file: str
    minzoom: int
    maxzoom: int
    resolution_base: float
    resolution_multiplier: float
    geometry_column: str
    parquet_row_group_size: int
    parquet_partition_by_zoomlevel: bool = False


def parse_arguments() -> Args:
    parser = argparse.ArgumentParser(description="Yosegi: Pyramid Parquet Generator")
    parser.add_argument("input_file", type=str, help="Path to the input file")
    parser.add_argument("output_file", type=str, help="Path to the output file")
    parser.add_argument(
        "--minzoom", type=int, default=0, help="Minimum zoom level (default: 0)"
    )
    parser.add_argument(
        "--maxzoom", type=int, default=16, help="Maximum zoom level (default: 16)"
    )
    parser.add_argument(
        "--resolution-base",
        type=float,
        default=2.5,
        help="Base resolution (default: 2.5)",
    )
    parser.add_argument(
        "--resolution-multiplier",
        type=float,
        default=2.0,
        help="Resolution multiplier (default: 2.0)",
    )
    parser.add_argument(
        "--geometry-column",
        type=str,
        default="geometry",
        help="Geometry column name (optional)",
    )
    parser.add_argument(
        "--parquet-row-group-size",
        type=int,
        default=10240,
        help="Parquet row group size (default: 10240)",
    )
    parser.add_argument(
        "--parquet-partition-by-zoomlevel",
        action="store_true",
        help="Enable Parquet partitioning by zoomlevel (default: False)",
    )

    args = parser.parse_args()

    return Args(
        input_file=args.input_file,
        output_file=args.output_file,
        minzoom=args.minzoom,
        maxzoom=args.maxzoom,
        resolution_base=args.resolution_base,
        resolution_multiplier=args.resolution_multiplier,
        geometry_column=args.geometry_column,
        parquet_row_group_size=args.parquet_row_group_size,
        parquet_partition_by_zoomlevel=args.parquet_partition_by_zoomlevel,
    )


def build_output_query(geom_col: str, maxzoom: int, zoomlevel_filter: int | None = None) -> str:
    """出力クエリを生成"""
    where_clause = f"WHERE a.zoomlevel = {zoomlevel_filter}" if zoomlevel_filter is not None else ""
    return f"""
        SELECT
            b.* EXCLUDE (_rep_geom, _uid, {geom_col}),
            ST_AsWKB(b.{geom_col}) AS {geom_col},
            a.zoomlevel,
            ST_Quadkey(b._rep_geom, {maxzoom}) AS quadkey
        FROM base b
        JOIN assigned a USING (_uid)
        {where_clause}
        ORDER BY a.zoomlevel, quadkey
    """


def write_geoparquet(
    conn: duckdb.DuckDBPyConnection,
    args: Args,
    geom_col: str,
) -> None:
    """GeoParquetを書き込む（最大サイズを超えない限り複数zoomlevelを1つのRow Groupに詰める）"""

    # スキーマ取得
    schema_query = build_output_query(geom_col, args.maxzoom) + " LIMIT 1"
    schema = conn.execute(schema_query).fetch_arrow_table().schema

    # GeoParquetメタデータ設定
    geo_metadata = {
        "version": "1.1.0",
        "primary_column": geom_col,
        "columns": {
            geom_col: {
                "encoding": "WKB",
                "geometry_types": [],
            }
        },
    }
    schema = schema.with_metadata({b"geo": json.dumps(geo_metadata).encode("utf-8")})

    # zoomlevelごとの行数を取得
    zoomlevels = conn.execute("""
        SELECT zoomlevel, COUNT(*) as cnt
        FROM assigned
        GROUP BY zoomlevel
        ORDER BY zoomlevel
    """).fetchall()

    max_rows = args.parquet_row_group_size

    with pq.ParquetWriter(args.output_file, schema) as writer:
        accumulated_tables: list[pa.Table] = []
        accumulated_rows = 0

        for zoomlevel, zl_count in zoomlevels:
            # このzoomlevelを追加すると最大を超える場合、先に書き出し
            if accumulated_rows > 0 and accumulated_rows + zl_count > max_rows:
                writer.write_table(pa.concat_tables(accumulated_tables))
                accumulated_tables = []
                accumulated_rows = 0

            # このzoomlevelのデータを取得
            query = build_output_query(geom_col, args.maxzoom, zoomlevel)
            zl_table = conn.execute(query).fetch_arrow_table()

            # 単体で最大を超える場合は分割
            if zl_table.num_rows > max_rows:
                if accumulated_tables:
                    writer.write_table(pa.concat_tables(accumulated_tables))
                    accumulated_tables = []
                    accumulated_rows = 0

                offset = 0
                while offset < zl_table.num_rows:
                    chunk_size = min(max_rows, zl_table.num_rows - offset)
                    chunk = zl_table.slice(offset, chunk_size)
                    if chunk_size == max_rows:
                        writer.write_table(chunk)
                    else:
                        accumulated_tables.append(chunk)
                        accumulated_rows = chunk_size
                    offset += chunk_size
            else:
                accumulated_tables.append(zl_table)
                accumulated_rows += zl_table.num_rows

        if accumulated_tables:
            writer.write_table(pa.concat_tables(accumulated_tables))


def process(args: Args) -> None:
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    # 入力ファイル読み込み（GDAL形式を優先、失敗したらParquet）
    try:
        input_query = f"SELECT * FROM ST_Read('{args.input_file}')"
        conn.execute(f"SELECT 1 FROM ({input_query}) LIMIT 1")
    except duckdb.IOException:
        input_query = f"SELECT * FROM read_parquet('{args.input_file}')"

    # geometry column検出
    cols = conn.execute(f"DESCRIBE ({input_query})").fetchall()
    geom_cols = [c[0] for c in cols if c[1] == "GEOMETRY"]
    if not geom_cols:
        raise ValueError("No geometry column found")
    geom_col = args.geometry_column if args.geometry_column in geom_cols else geom_cols[0]

    # baseテーブル作成（入力データ + _uid + _rep_geom）
    conn.execute(f"""
        CREATE TABLE base AS
        SELECT
            *,
            row_number() OVER () AS _uid,
            CASE
                WHEN ST_GeometryType({geom_col}) IN ('POINT', 'MULTIPOINT')
                    THEN {geom_col}
                ELSE ST_PointOnSurface({geom_col})
            END AS _rep_geom
        FROM ({input_query});
    """)

    # assignedテーブル作成
    conn.execute("""
        CREATE TABLE assigned (
            _uid BIGINT PRIMARY KEY,
            zoomlevel INTEGER
        );
    """)

    # zoomlevelループ: 各zoomで代表点を1つ選んでassign
    for z in range(args.minzoom, args.maxzoom):
        prec = args.resolution_base / (args.resolution_multiplier ** z)

        conn.execute(f"""
            INSERT INTO assigned
            SELECT b._uid, {z} AS zoomlevel
            FROM base b
            WHERE NOT EXISTS (SELECT 1 FROM assigned a WHERE a._uid = b._uid)
            QUALIFY row_number() OVER (
                PARTITION BY ST_ReducePrecision(b._rep_geom, {prec})
                ORDER BY b._uid
            ) = 1;
        """)

        # 全て割り当て済みなら終了
        unassigned_count = conn.execute("""
            SELECT COUNT(*) FROM base b
            WHERE NOT EXISTS (SELECT 1 FROM assigned a WHERE a._uid = b._uid)
        """).fetchone()
        if unassigned_count and unassigned_count[0] == 0:
            break

    # 残りはmaxzoomに割り当て
    conn.execute(f"""
        INSERT INTO assigned
        SELECT b._uid, {args.maxzoom} AS zoomlevel
        FROM base b
        WHERE NOT EXISTS (SELECT 1 FROM assigned a WHERE a._uid = b._uid);
    """)

    # 出力
    if args.parquet_partition_by_zoomlevel:
        conn.execute(f"""
            COPY (
                SELECT
                    b.* EXCLUDE (_rep_geom, _uid),
                    a.zoomlevel,
                    ST_Quadkey(b._rep_geom, {args.maxzoom}) AS quadkey
                FROM base b
                JOIN assigned a USING (_uid)
                ORDER BY zoomlevel, quadkey
            )
            TO '{args.output_file}'
            (FORMAT PARQUET,
             ROW_GROUP_SIZE {args.parquet_row_group_size},
             PARTITION_BY zoomlevel);
        """)
    else:
        write_geoparquet(conn, args, geom_col)

    conn.close()


def main() -> None:
    args = parse_arguments()
    process(args)


if __name__ == "__main__":
    main()
