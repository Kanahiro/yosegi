import argparse
import json
import sys
import time
from dataclasses import dataclass

import duckdb


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
    min_visible_size_factor: float = 1.0


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
    parser.add_argument(
        "--min-visible-size-factor",
        type=float,
        default=1.0,
        help=(
            "Skip features at low zoom whose bbox is smaller than the grid "
            "resolution times this factor. A feature is eligible at zoom z "
            "only if max(bbox_width, bbox_height) >= resolution(z) * factor. "
            "Set to 0 to disable filtering (default: 1.0)"
        ),
    )

    args = parser.parse_args()

    if not 1 <= args.maxzoom <= 23:
        parser.error(f"--maxzoom must be in [1, 23], got {args.maxzoom}")
    if not 0 <= args.minzoom < args.maxzoom:
        parser.error(
            f"--minzoom must satisfy 0 <= minzoom < maxzoom, "
            f"got minzoom={args.minzoom}, maxzoom={args.maxzoom}"
        )
    if args.min_visible_size_factor < 0:
        parser.error(
            f"--min-visible-size-factor must be >= 0, "
            f"got {args.min_visible_size_factor}"
        )

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
        min_visible_size_factor=args.min_visible_size_factor,
    )


_GEOMETRY_TYPE_MAP = {
    "POINT": "Point",
    "LINESTRING": "LineString",
    "POLYGON": "Polygon",
    "MULTIPOINT": "MultiPoint",
    "MULTILINESTRING": "MultiLineString",
    "MULTIPOLYGON": "MultiPolygon",
    "GEOMETRYCOLLECTION": "GeometryCollection",
}


def write_geoparquet(
    conn: duckdb.DuckDBPyConnection,
    args: Args,
    geom_col: str,
    geometry_types_raw: set[str],
) -> None:
    """GeoParquetを書き込む。

    全行を (zoomlevel, ST_Hilbert) でソートして DuckDB の COPY TO で出力する。
    Hilbert curve は Z-order (quadkey) と違い隣接連続性が高く、隣接行の属性
    相関が保たれるため辞書/RLE が効きやすい。RowGroup は固定サイズで切り、
    bbox covering 統計でプッシュダウンを効かせる。
    """

    _log("      computing bbox / geometry types...")

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
    if bbox_row is None or any(v is None for v in bbox_row):
        raise ValueError("Could not compute bbox: input has no valid geometries")
    bx = [float(v) for v in bbox_row]
    _log(f"      bbox:           [{bx[0]:.4f}, {bx[1]:.4f}, {bx[2]:.4f}, {bx[3]:.4f}]")

    # Hilbert の bounds は退化 (幅 or 高さ 0) を避けるため最低限のマージンを入れる
    xmin, ymin, xmax, ymax = bx
    if xmax <= xmin:
        xmax = xmin + 1.0
    if ymax <= ymin:
        ymax = ymin + 1.0
    bounds_sql = (
        f"{{'min_x': {xmin}, 'min_y': {ymin}, "
        f"'max_x': {xmax}, 'max_y': {ymax}}}::BOX_2D"
    )

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
        "bbox": bx,
    }
    geo_metadata = {
        "version": "1.1.0",
        "primary_column": geom_col,
        "columns": {geom_col: column_metadata},
    }
    geo_json = _sql_escape(json.dumps(geo_metadata))

    total_rows = conn.execute("SELECT COUNT(*) FROM assigned").fetchone()[0]  # type: ignore[index]
    _log(f"      total rows:     {_fmt_int(total_rows)}")
    _log(f"      writing parquet (row_group_size={args.parquet_row_group_size})...")

    safe_output = _sql_escape(args.output_file)
    conn.execute(f"""
        COPY (
            SELECT
                b.* EXCLUDE (_rep_geom, _min_visible_zoom, {geom_col}),
                ST_AsWKB(b.{geom_col}) AS {geom_col},
                {{
                    'xmin': ST_XMin(b.{geom_col}),
                    'ymin': ST_YMin(b.{geom_col}),
                    'xmax': ST_XMax(b.{geom_col}),
                    'ymax': ST_YMax(b.{geom_col})
                }} AS bbox,
                a.zoomlevel
            FROM base b
            JOIN assigned a ON b.rowid = a._uid
            ORDER BY a.zoomlevel, ST_Hilbert(b._rep_geom, {bounds_sql})
        ) TO '{safe_output}' (
            FORMAT PARQUET,
            ROW_GROUP_SIZE {args.parquet_row_group_size},
            KV_METADATA {{geo: '{geo_json}'}}
        );
    """)


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

    safe_input = _sql_escape(args.input_file)
    try:
        input_query = f"SELECT * FROM ST_Read('{safe_input}')"
        conn.execute(f"SELECT 1 FROM ({input_query}) LIMIT 1")
        input_format = "GDAL/OGR (ST_Read)"
    except duckdb.IOException:
        input_query = f"SELECT * FROM read_parquet('{safe_input}')"
        input_format = "Parquet (read_parquet)"
    _log(f"      format:         {input_format}")

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

    # _rep_geom は常に POINT であることを保証する (ST_X/ST_Y で参照するため)。
    # _min_visible_zoom: bbox の最大辺長が resolution(z) * factor 以上になる最小 z。
    factor = args.min_visible_size_factor
    if factor > 0:
        size_expr = (
            f"GREATEST("
            f"ST_XMax({geom_col}) - ST_XMin({geom_col}), "
            f"ST_YMax({geom_col}) - ST_YMin({geom_col})"
            f")"
        )
        threshold_at_z0 = args.resolution_base * factor
        min_visible_zoom_expr = f"""
            CASE
                WHEN {size_expr} <= 0 THEN {args.minzoom}
                WHEN {size_expr} >= {threshold_at_z0} THEN {args.minzoom}
                ELSE GREATEST(
                    {args.minzoom},
                    LEAST(
                        {args.maxzoom},
                        CAST(CEIL(
                            LN({threshold_at_z0} / {size_expr})
                            / LN({args.resolution_multiplier})
                        ) AS INTEGER)
                    )
                )
            END
        """
    else:
        min_visible_zoom_expr = f"{args.minzoom}"

    conn.execute(f"""
        CREATE TABLE base AS
        SELECT
            *,
            CASE
                WHEN ST_GeometryType({geom_col}) = 'POINT'
                    THEN {geom_col}
                ELSE ST_PointOnSurface({geom_col})
            END AS _rep_geom,
            ({min_visible_zoom_expr}) AS _min_visible_zoom
        FROM ({input_query});
    """)

    conn.execute("""
        CREATE TABLE assigned (
            _uid BIGINT,
            zoomlevel INTEGER
        );
    """)

    total_count = conn.execute("SELECT COUNT(*) FROM base").fetchone()[0]  # type: ignore[index]
    _log(f"      features:       {_fmt_int(total_count)}")

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

    if effective_sort_by:
        sort_expr, sort_direction = _split_sort_direction(effective_sort_by)
        sort_select = f", ({sort_expr}) AS _sort_key"
        order_by = f"r._sort_key {sort_direction}".strip()
    else:
        sort_select = ""
        order_by = "hash(r._uid)"

    conn.execute(f"""
        CREATE TEMP TABLE remaining AS
        SELECT rowid AS _uid, _rep_geom, _min_visible_zoom{sort_select}
        FROM base;
    """)

    for z in range(args.minzoom, args.maxzoom):
        zoom_t0 = time.perf_counter()
        prec = args.resolution_base / (args.resolution_multiplier**z)
        conn.execute(f"""
            CREATE TEMP TABLE newly_assigned AS
            SELECT r._uid, {z} AS zoomlevel
            FROM remaining r
            WHERE r._min_visible_zoom <= {z}
            QUALIFY row_number() OVER (
                PARTITION BY
                    floor(ST_X(r._rep_geom) / {prec}),
                    floor(ST_Y(r._rep_geom) / {prec})
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
