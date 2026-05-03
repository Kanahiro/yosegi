import argparse
import json
import math
import sys
import time
from dataclasses import dataclass

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def _log(msg: str = "") -> None:
    print(msg, file=sys.stderr, flush=True)


def _fmt_secs(s: float) -> str:
    if s < 60:
        return f"{s:.2f}s"
    m = int(s // 60)
    return f"{m}m {s - m * 60:.1f}s"


def _fmt_int(n: int) -> str:
    return f"{n:,}"


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


def _split_sort_direction(sort_by: str) -> tuple[str, str]:
    """sort_by から末尾の ASC/DESC を切り離して (式, 方向) を返す"""
    parts = sort_by.strip().rsplit(maxsplit=1)
    if len(parts) == 2 and parts[1].upper() in ("ASC", "DESC"):
        return parts[0], parts[1].upper()
    return sort_by.strip(), ""


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
        help=(
            "Sort key for feature thinning "
            "(default: ST_Area DESC for polygons, ST_Length DESC for lines, "
            "hash(_uid) otherwise)"
        ),
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
            b.* EXCLUDE (_rep_geom, {geom_col}),
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
        JOIN assigned a ON b.rowid = a._uid
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


def _compute_quadkey_change_level(
    quadkey_col: pa.ChunkedArray | pa.Array, prefix_len: int
) -> np.ndarray:
    """隣接 quadkey 間で先頭 prefix_len 文字内の最初の差異位置 + 1 を返す。

    どこも違わない (prefix_len 文字すべて一致) 場合は prefix_len + 1。
    quadkey は ST_Quadkey(_, maxzoom) により maxzoom 文字に揃っており
    prefix_len <= maxzoom なので、各 prefix は固定長 prefix_len バイトとなる。
    """
    prefix = pc.utf8_slice_codeunits(quadkey_col, 0, prefix_len)  # ty: ignore[unresolved-attribute]
    if isinstance(prefix, pa.ChunkedArray):
        prefix = prefix.combine_chunks()
    n = len(prefix)
    data = np.frombuffer(prefix.buffers()[2], dtype=np.uint8, count=n * prefix_len)
    byte_arr = data.reshape(n, prefix_len)
    diff = byte_arr[1:] != byte_arr[:-1]
    any_diff = diff.any(axis=1)
    first_diff = diff.argmax(axis=1)
    return np.where(any_diff, first_diff + 1, prefix_len + 1)


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
    if n == 1:
        yield table
        return

    change_level = _compute_quadkey_change_level(table.column("quadkey"), prefix_len)

    coarse_threshold = max(1, min_rows // 4)
    start = 0
    for i in range(1, n):
        rg_size = i - start
        if rg_size >= max_rows:
            yield table.slice(start, rg_size)
            start = i
            continue

        cl = int(change_level[i - 1])
        if cl > prefix_len:
            continue

        if cl == 1:
            yield table.slice(start, rg_size)
            start = i
            continue

        threshold = min_rows if cl == prefix_len else coarse_threshold
        if rg_size >= threshold:
            yield table.slice(start, rg_size)
            start = i

    if start < n:
        yield table.slice(start, n - start)


def write_geoparquet(
    conn: duckdb.DuckDBPyConnection,
    args: Args,
    geom_col: str,
    geometry_types_raw: set[str],
) -> None:
    """GeoParquetを書き込む。

    RowGroup分割戦略:
    - 全データを (zoomlevel, quadkey) でソート済み
    - quadkey prefix-N の境界で RowGroup を切り、各 RG の quadkey min/max を
      タイト化してプッシュダウン効率を上げる
    - 低zoomは閾値到達せず自然に複数zoomがパックされる
    - 高zoomは zl_count >= max_rows のとき先にflushして混在を防ぐ
    """

    _log("      scanning schema / bbox / geometry types...")

    # スキーマ取得
    schema_query = build_output_query(geom_col, args.maxzoom) + " LIMIT 1"
    schema = conn.execute(schema_query).fetch_arrow_table().schema

    # 全体bboxを取得 (geometry_types は process() で取得済み)
    bbox_row = conn.execute(f"""
        SELECT
            MIN(ST_XMin({geom_col})),
            MIN(ST_YMin({geom_col})),
            MAX(ST_XMax({geom_col})),
            MAX(ST_YMax({geom_col}))
        FROM base
    """).fetchone()
    geometry_types = sorted(
        {_GEOMETRY_TYPE_MAP.get(t, t) for t in geometry_types_raw}
    )
    _log(f"      geometry types: {', '.join(geometry_types) or '(none)'}")
    if bbox_row is not None and all(v is not None for v in bbox_row):
        bx = [float(v) for v in bbox_row]
        _log(
            f"      bbox:           [{bx[0]:.4f}, {bx[1]:.4f}, {bx[2]:.4f}, {bx[3]:.4f}]"
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

    _log(f"      total rows:     {_fmt_int(total_rows)}")
    _log(
        f"      row groups:     target ≈ {math.ceil(total_rows / max_rows)} "
        f"(max_rows={max_rows}, prefix_len={prefix_len})"
    )
    _log("      writing row groups per zoom...")

    rg_count = 0

    with pq.ParquetWriter(args.output_file, schema) as writer:
        accumulated_tables: list[pa.Table] = []
        accumulated_rows = 0
        current_zoom: int | None = None

        def flush() -> None:
            nonlocal accumulated_tables, accumulated_rows, current_zoom, rg_count
            if accumulated_tables:
                writer.write_table(pa.concat_tables(accumulated_tables))
                accumulated_tables = []
                accumulated_rows = 0
                current_zoom = None
                rg_count += 1

        for zoomlevel, zl_count in zoomlevels:
            if zl_count == 0:
                continue

            # zl_count > max_rows のzoomは単独で複数RGに分かれる。低zoom由来の
            # 蓄積をここでflushして、高zoomのRGに低zoomデータが混ざるのを防ぐ。
            if zl_count > max_rows and accumulated_rows > 0:
                flush()

            zoom_t0 = time.perf_counter()
            zoom_rg_start = rg_count

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

                if (
                    accumulated_rows > 0
                    and accumulated_rows + chunk.num_rows > max_rows
                ):
                    flush()
                accumulated_tables.append(chunk)
                accumulated_rows += chunk.num_rows
                current_zoom = zoomlevel
                if accumulated_rows >= max_rows:
                    flush()

            zoom_rgs_written = rg_count - zoom_rg_start
            _log(
                f"        zoom {zoomlevel:>2}: {_fmt_int(zl_count):>14} rows, "
                f"{zoom_rgs_written:>4} RG flushed  ({_fmt_secs(time.perf_counter() - zoom_t0)})"
            )

        flush()
    _log(f"      row groups written: {rg_count}")


def process(args: Args) -> None:
    total_t0 = time.perf_counter()
    _log(f"Yosegi: {args.input_file} → {args.output_file}")
    _log(
        f"  zoom {args.minzoom}..{args.maxzoom}, row_group_size={args.parquet_row_group_size}, "
        f"sort={args.sort_by or 'auto'}"
    )

    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    # ===== Phase 1: input load =====
    _log("\n[1/3] Loading input")
    phase_t0 = time.perf_counter()

    # 入力ファイル読み込み（GDAL形式を優先、失敗したらParquet）
    safe_input = _sql_escape(args.input_file)
    try:
        input_query = f"SELECT * FROM ST_Read('{safe_input}')"
        conn.execute(f"SELECT 1 FROM ({input_query}) LIMIT 1")
        input_format = "GDAL/OGR (ST_Read)"
    except duckdb.IOException:
        input_query = f"SELECT * FROM read_parquet('{safe_input}')"
        input_format = "Parquet (read_parquet)"
    _log(f"      format:         {input_format}")

    # geometry column検出: 指定されていればそれを信用、未指定なら型名に "geometry" を含む列を採用
    if args.geometry_column is not None:
        geom_col = args.geometry_column
        _log(f"      geometry col:   {geom_col} (specified)")
    else:
        cols = conn.execute(f"DESCRIBE ({input_query})").fetchall()
        geom_cols = [c[0] for c in cols if "geometry" in c[1].lower()]
        if not geom_cols:
            raise ValueError("No geometry column found")
        geom_col = geom_cols[0]
        _log(f"      geometry col:   {geom_col} (auto-detected)")

    _log("      building base table (with representative points)...")

    # baseテーブル作成（入力データ + _rep_geom）
    # _uid は base.rowid を後段で参照することで代用し、row_number() OVER () による
    # シングルスレッド実行を回避する。
    conn.execute(f"""
        CREATE TABLE base AS
        SELECT
            *,
            CASE
                WHEN ST_GeometryType({geom_col}) IN ('POINT', 'MULTIPOINT')
                    THEN {geom_col}
                ELSE ST_PointOnSurface({geom_col})
            END AS _rep_geom
        FROM ({input_query});
    """)

    # assignedテーブル作成
    # PRIMARY KEY は付けない: ユニーク性はアルゴリズム上保証されており、
    # DuckDB は PK 制約付き INSERT をシリアル化するため並列度を下げる。
    conn.execute("""
        CREATE TABLE assigned (
            _uid BIGINT,
            zoomlevel INTEGER
        );
    """)

    total_count = conn.execute("SELECT COUNT(*) FROM base").fetchone()[0]  # type: ignore[index]
    _log(f"      features:       {_fmt_int(total_count)}")

    # geometry 型は Phase 2 のデフォルト sort 決定と Phase 3 のメタデータで使うため
    # ここで一度だけスキャンする
    geometry_types_raw: set[str] = {
        r[0]
        for r in conn.execute(
            f"SELECT DISTINCT ST_GeometryType({geom_col}) FROM base"
        ).fetchall()
    }
    _log(f"      elapsed:        {_fmt_secs(time.perf_counter() - phase_t0)}")

    # ===== Phase 2: zoom assignment =====
    _log(f"\n[2/3] Assigning zoom levels ({args.minzoom}..{args.maxzoom})")
    phase_t0 = time.perf_counter()

    assigned_count = 0

    # sort_by 未指定時は geometry 型から既定値を決める:
    #   line 系のみ → ST_Length DESC (長い順)
    #   polygon 系のみ → ST_Area DESC (大きい順)
    #   それ以外 / 混在 → hash(_uid) (ランダム)
    effective_sort_by = args.sort_by
    if effective_sort_by is None:
        if geometry_types_raw and geometry_types_raw <= {
            "LINESTRING",
            "MULTILINESTRING",
        }:
            effective_sort_by = f"ST_Length({geom_col}) DESC"
        elif geometry_types_raw and geometry_types_raw <= {
            "POLYGON",
            "MULTIPOLYGON",
        }:
            effective_sort_by = f"ST_Area({geom_col}) DESC"
    _log(f"      sort key:       {effective_sort_by or 'hash(_uid)'}")

    # sort_by の値を remaining に取り込み、ループ内での base への JOIN を回避する
    if effective_sort_by:
        sort_expr, sort_direction = _split_sort_direction(effective_sort_by)
        sort_select = f", ({sort_expr}) AS _sort_key"
        order_by = f"r._sort_key {sort_direction}".strip()
    else:
        sort_select = ""
        order_by = "hash(r._uid)"

    # 未割り当てfeatureだけを保持し、zoomごとの反復でbase全体を再走査しない
    conn.execute(f"""
        CREATE TEMP TABLE remaining AS
        SELECT rowid AS _uid, _rep_geom{sort_select}
        FROM base;
    """)

    # zoomlevelループ: 各zoomで代表点を1つ選んでassign
    for z in range(args.minzoom, args.maxzoom):
        zoom_t0 = time.perf_counter()
        prec = args.resolution_base / (args.resolution_multiplier**z)
        # タイル境界と整合する分割粒度を計算
        tile_size_x = 360.0 / (2**z)
        cells_per_tile_1d = tile_size_x / prec
        sub_zoom = z + max(1, round(math.log2(max(2.0, cells_per_tile_1d))))

        conn.execute(f"""
            CREATE TEMP TABLE newly_assigned AS
            SELECT r._uid, {z} AS zoomlevel
            FROM remaining r
            QUALIFY row_number() OVER (
                PARTITION BY ST_Quadkey(r._rep_geom, {sub_zoom})
                ORDER BY {order_by}
            ) = 1;
        """)

        newly_assigned_count = conn.execute(
            "SELECT COUNT(*) FROM newly_assigned"
        ).fetchone()[0]  # type: ignore[index]

        conn.execute("""
            INSERT INTO assigned
            SELECT _uid, zoomlevel
            FROM newly_assigned;
        """)

        conn.execute("""
            DELETE FROM remaining
            USING newly_assigned
            WHERE remaining._uid = newly_assigned._uid;
        """)

        conn.execute("DROP TABLE newly_assigned;")

        assigned_count += newly_assigned_count
        pct = 100.0 * assigned_count / total_count if total_count else 0.0
        _log(
            f"      zoom {z:>2}: +{_fmt_int(newly_assigned_count):>12} → "
            f"{_fmt_int(assigned_count):>14} / {_fmt_int(total_count)} "
            f"({pct:5.1f}%)  ({_fmt_secs(time.perf_counter() - zoom_t0)})"
        )
        if assigned_count == total_count:
            break

    # 残りはmaxzoomに割り当て
    remaining_count = total_count - assigned_count
    if remaining_count > 0:
        zoom_t0 = time.perf_counter()
        conn.execute(f"""
            INSERT INTO assigned
            SELECT _uid, {args.maxzoom} AS zoomlevel
            FROM remaining;
        """)
        _log(
            f"      zoom {args.maxzoom:>2}: +{_fmt_int(remaining_count):>12} → "
            f"{_fmt_int(total_count):>14} / {_fmt_int(total_count)} "
            f"(100.0%)  ({_fmt_secs(time.perf_counter() - zoom_t0)}) [remaining]"
        )
    _log(f"      elapsed:        {_fmt_secs(time.perf_counter() - phase_t0)}")

    # ===== Phase 3: write =====
    _log("\n[3/3] Writing GeoParquet")
    phase_t0 = time.perf_counter()
    write_geoparquet(conn, args, geom_col, geometry_types_raw)
    _log(f"      elapsed:        {_fmt_secs(time.perf_counter() - phase_t0)}")

    conn.close()

    _log(
        f"\nDone: {args.output_file} (total {_fmt_secs(time.perf_counter() - total_t0)})"
    )


def main() -> None:
    args = parse_arguments()
    process(args)


if __name__ == "__main__":
    main()
