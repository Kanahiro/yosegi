import argparse
import json
import math
import sys
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
    geometry_column: str | None
    parquet_row_group_size: int
    sort_by: str | None = None


def _sql_escape(value: str) -> str:
    """シングルクォートをエスケープしてSQL文字列リテラルに安全に埋め込む"""
    return value.replace("'", "''")


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
        default=None,
        help="Geometry column name (auto-detected if not specified)",
    )
    parser.add_argument(
        "--parquet-row-group-size",
        type=int,
        default=10240,
        help="Parquet row group size (default: 10240)",
    )
    parser.add_argument(
        "--sort-by",
        type=str,
        default=None,
        help="Sort key for feature thinning (default: hash(_uid), i.e. random)",
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
        sort_by=args.sort_by,
    )


def build_output_query(
    geom_col: str, maxzoom: int, zoomlevel_filter: int | None = None
) -> str:
    """出力クエリを生成"""
    where_clause = (
        f"WHERE a.zoomlevel = {zoomlevel_filter}"
        if zoomlevel_filter is not None
        else ""
    )
    return f"""
        SELECT
            b.* EXCLUDE (_rep_geom, _uid, {geom_col}),
            ST_AsWKB(b.{geom_col}) AS {geom_col},
            {{
                'xmin': ST_XMin(b.{geom_col}),
                'ymin': ST_YMin(b.{geom_col}),
                'xmax': ST_XMax(b.{geom_col}),
                'ymax': ST_YMax(b.{geom_col})
            }} AS bbox,
            a.zoomlevel,
            ST_Quadkey(b._rep_geom, {maxzoom}) AS quadkey
        FROM base b
        JOIN assigned a USING (_uid)
        {where_clause}
        ORDER BY a.zoomlevel, quadkey
    """


_GEOMETRY_TYPE_MAP = {
    "POINT": "Point",
    "LINESTRING": "LineString",
    "POLYGON": "Polygon",
    "MULTIPOINT": "MultiPoint",
    "MULTILINESTRING": "MultiLineString",
    "MULTIPOLYGON": "MultiPolygon",
    "GEOMETRYCOLLECTION": "GeometryCollection",
}


def _split_at_quadkey_prefix(
    table: pa.Table,
    max_rows: int,
    min_rows: int,
    prefix_len: int,
):
    """tableをquadkey prefixの境界でスライスして yield する。

    分割条件:
    - rg_size >= max_rows: 強制分割
    - prefix-1 が変化したら常に分割 (chunkがprefix-1を跨ぐのを防ぎ、各chunkを
      ある親タイル内に閉じる)
    - prefix-K (1 < K <= prefix_len) では rg_size >= threshold(K) のときのみ分割
      これにより小さな深層バケットは隣接バケットと同じchunkにまとまる
    """
    n = table.num_rows
    if n == 0:
        return

    quadkeys = table.column("quadkey").to_pylist()
    coarse_threshold = max(1, min_rows // 4)
    start = 0
    for i in range(1, n):
        rg_size = i - start
        if rg_size >= max_rows:
            yield table.slice(start, rg_size)
            start = i
            continue

        a = quadkeys[i - 1]
        b = quadkeys[i]
        common = 0
        for k in range(prefix_len):
            if k >= len(a) or k >= len(b) or a[k] != b[k]:
                break
            common += 1
        if common >= prefix_len:
            continue

        change_level = common + 1
        if change_level == 1:
            yield table.slice(start, rg_size)
            start = i
            continue

        threshold = min_rows if change_level == prefix_len else coarse_threshold
        if rg_size >= threshold:
            yield table.slice(start, rg_size)
            start = i

    if start < n:
        yield table.slice(start, n - start)


def write_geoparquet(
    conn: duckdb.DuckDBPyConnection,
    args: Args,
    geom_col: str,
) -> None:
    """GeoParquetを書き込む。

    RowGroup分割戦略:
    - 全データを (zoomlevel, quadkey) でソート済み
    - quadkey prefix-N の境界で RowGroup を切り、各 RG の quadkey min/max を
      タイト化してプッシュダウン効率を上げる
    - 低zoomは閾値到達せず自然に複数zoomがパックされる
    - 高zoomは zl_count >= max_rows のとき先にflushして混在を防ぐ
    """

    # スキーマ取得
    schema_query = build_output_query(geom_col, args.maxzoom) + " LIMIT 1"
    schema = conn.execute(schema_query).fetch_arrow_table().schema

    # 全体bboxとgeometry_typesを取得
    bbox_row = conn.execute(f"""
        SELECT
            MIN(ST_XMin({geom_col})),
            MIN(ST_YMin({geom_col})),
            MAX(ST_XMax({geom_col})),
            MAX(ST_YMax({geom_col}))
        FROM base
    """).fetchone()
    geom_type_rows = conn.execute(f"""
        SELECT DISTINCT ST_GeometryType({geom_col}) FROM base
    """).fetchall()
    geometry_types = sorted(
        {_GEOMETRY_TYPE_MAP.get(r[0], r[0]) for r in geom_type_rows}
    )

    # GeoParquetメタデータ設定 (1.1.0 spec: covering / bbox / geometry_types)
    column_metadata: dict = {
        "encoding": "WKB",
        "geometry_types": geometry_types,
        "covering": {
            "bbox": {
                "xmin": ["bbox", "xmin"],
                "ymin": ["bbox", "ymin"],
                "xmax": ["bbox", "xmax"],
                "ymax": ["bbox", "ymax"],
            }
        },
    }
    if bbox_row is not None and all(v is not None for v in bbox_row):
        column_metadata["bbox"] = [float(v) for v in bbox_row]

    geo_metadata = {
        "version": "1.1.0",
        "primary_column": geom_col,
        "columns": {geom_col: column_metadata},
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
    min_rows = max(1, max_rows // 4)
    total_rows = sum(c for _, c in zoomlevels)

    # 適応的prefix長: 4^P ≈ ceil(total_rows / max_rows) を満たす P
    if total_rows > max_rows:
        target_rgs = math.ceil(total_rows / max_rows)
        prefix_len = max(1, min(args.maxzoom, math.ceil(math.log(target_rgs, 4))))
    else:
        prefix_len = 1

    with pq.ParquetWriter(args.output_file, schema) as writer:
        accumulated_tables: list[pa.Table] = []
        accumulated_rows = 0
        current_zoom: int | None = None

        def flush() -> None:
            nonlocal accumulated_tables, accumulated_rows, current_zoom
            if accumulated_tables:
                writer.write_table(pa.concat_tables(accumulated_tables))
                accumulated_tables = []
                accumulated_rows = 0
                current_zoom = None

        for zoomlevel, zl_count in zoomlevels:
            if zl_count == 0:
                continue

            # zl_count > max_rows のzoomは単独で複数RGに分かれる。低zoom由来の
            # 蓄積をここでflushして、高zoomのRGに低zoomデータが混ざるのを防ぐ。
            if zl_count > max_rows and accumulated_rows > 0:
                flush()

            query = build_output_query(geom_col, args.maxzoom, zoomlevel)
            zl_table = conn.execute(query).fetch_arrow_table()

            for chunk in _split_at_quadkey_prefix(
                zl_table, max_rows, min_rows, prefix_len
            ):
                # 同一zoom内: prefix-1 alignmentを保つため、揃っていない場合flush。
                # zoom境界では check をスキップ → 小さい低zoomは隣接zoomとパック。
                if accumulated_tables and current_zoom == zoomlevel:
                    acc_first = accumulated_tables[0].column("quadkey")[0].as_py()
                    acc_last = accumulated_tables[-1].column("quadkey")[-1].as_py()
                    new_first = chunk.column("quadkey")[0].as_py()
                    if acc_first[:1] == acc_last[:1] and acc_first[:1] != new_first[:1]:
                        flush()

                if accumulated_rows > 0 and accumulated_rows + chunk.num_rows > max_rows:
                    flush()
                accumulated_tables.append(chunk)
                accumulated_rows += chunk.num_rows
                current_zoom = zoomlevel
                if accumulated_rows >= max_rows:
                    flush()

        flush()


def process(args: Args) -> None:
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    # 入力ファイル読み込み（GDAL形式を優先、失敗したらParquet）
    safe_input = _sql_escape(args.input_file)
    try:
        input_query = f"SELECT * FROM ST_Read('{safe_input}')"
        conn.execute(f"SELECT 1 FROM ({input_query}) LIMIT 1")
    except duckdb.IOException:
        input_query = f"SELECT * FROM read_parquet('{safe_input}')"

    # geometry column検出: 指定されていればそれを信用、未指定なら型名に "geometry" を含む列を採用
    if args.geometry_column is not None:
        geom_col = args.geometry_column
    else:
        cols = conn.execute(f"DESCRIBE ({input_query})").fetchall()
        geom_cols = [c[0] for c in cols if "geometry" in c[1].lower()]
        if not geom_cols:
            raise ValueError("No geometry column found")
        geom_col = geom_cols[0]

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

    total_count = conn.execute("SELECT COUNT(*) FROM base").fetchone()[0]  # type: ignore[index]

    # zoomlevelループ: 各zoomで代表点を1つ選んでassign
    for z in range(args.minzoom, args.maxzoom):
        prec = args.resolution_base / (args.resolution_multiplier**z)
        # タイル境界と整合する分割粒度を計算
        tile_size_x = 360.0 / (2**z)
        cells_per_tile_1d = tile_size_x / prec
        sub_zoom = z + max(1, round(math.log2(max(2.0, cells_per_tile_1d))))

        conn.execute(f"""
            INSERT INTO assigned
            SELECT b._uid, {z} AS zoomlevel
            FROM base b
            WHERE NOT EXISTS (SELECT 1 FROM assigned a WHERE a._uid = b._uid)
            QUALIFY row_number() OVER (
                PARTITION BY ST_Quadkey(b._rep_geom, {sub_zoom})
                ORDER BY {args.sort_by if args.sort_by else "hash(b._uid)"}
            ) = 1;
        """)

        # 全て割り当て済みなら終了
        assigned_count = conn.execute("SELECT COUNT(*) FROM assigned").fetchone()[0]  # type: ignore[index]
        print(
            f"  zoom {z}: {assigned_count}/{total_count} features assigned",
            file=sys.stderr,
        )
        if assigned_count == total_count:
            break

    # 残りはmaxzoomに割り当て
    print(f"  zoom {args.maxzoom} (remaining features)", file=sys.stderr)
    conn.execute(f"""
        INSERT INTO assigned
        SELECT b._uid, {args.maxzoom} AS zoomlevel
        FROM base b
        WHERE NOT EXISTS (SELECT 1 FROM assigned a WHERE a._uid = b._uid);
    """)

    # 出力
    print("Writing GeoParquet...", file=sys.stderr)
    write_geoparquet(conn, args, geom_col)
    print(f"Done: {args.output_file}", file=sys.stderr)

    conn.close()


def main() -> None:
    args = parse_arguments()
    process(args)


if __name__ == "__main__":
    main()
