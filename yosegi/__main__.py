import argparse
from dataclasses import dataclass

import duckdb


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


def parse_arguments():
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
        help="Resolution multiplier (default: 2.0). Larger is more precise. When set to 2.0, zoom=0 resolution / 2.0 = zoom=1 resolution.",
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


def process(args: Args):
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    # ---- input ----
    try:
        conn.execute(f"""
            CREATE TABLE input_data AS
            SELECT * FROM ST_Read('{args.input_file}');
        """)
    except duckdb.IOException:
        conn.execute(f"""
            CREATE TABLE input_data AS
            SELECT * FROM read_parquet('{args.input_file}');
        """)

    # ---- geometry column detection ----
    cols = conn.execute("PRAGMA table_info('input_data');").fetchall()
    geom_cols = [c[1] for c in cols if c[2] == "GEOMETRY"]
    if not geom_cols:
        raise ValueError("No geometry column found")

    geom_col = (
        args.geometry_column if args.geometry_column in geom_cols else geom_cols[0]
    )

    # ---- base table (deterministic uid) ----
    conn.execute(f"""
        CREATE TABLE base AS
        SELECT
            *,
            row_number() OVER () AS _uid,
            CASE
                WHEN upper(ST_GeometryType({geom_col})::varchar) LIKE '%POINT'
                    THEN {geom_col}
                ELSE ST_PointOnSurface({geom_col})
            END AS _rep_geom
        FROM input_data;
    """)

    # ---- working tables ----
    conn.execute("""
        CREATE TABLE unassigned AS
        SELECT _uid, _rep_geom FROM base;
    """)

    conn.execute("""
        CREATE TABLE assigned (
            _uid BIGINT PRIMARY KEY,
            zoomlevel INTEGER
        );
    """)

    # ---- zoom loop ----
    for z in range(args.minzoom, args.maxzoom):
        prec = args.resolution_base / (args.resolution_multiplier**z)

        conn.execute(f"""
            INSERT INTO assigned
            SELECT u._uid, {z} AS zoomlevel
            FROM unassigned u
            QUALIFY row_number() OVER (
                PARTITION BY ST_ReducePrecision(u._rep_geom, {prec})
                ORDER BY u._uid
            ) = 1;
        """)

        conn.execute(f"""
            DELETE FROM unassigned
            USING assigned a
            WHERE unassigned._uid = a._uid
              AND a.zoomlevel = {z};
        """)

        remaining = conn.execute("SELECT COUNT(*) FROM unassigned").fetchone()
        if remaining is not None and remaining[0] == 0:
            break

    # ---- leftovers go to maxzoom ----
    conn.execute(f"""
        INSERT INTO assigned
        SELECT _uid, {args.maxzoom}
        FROM unassigned;
    """)

    # ---- final output ----
    partition_clause = (
        ", PARTITION_BY zoomlevel" if args.parquet_partition_by_zoomlevel else ""
    )

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
         ROW_GROUP_SIZE {args.parquet_row_group_size}
         {partition_clause});
    """)

    conn.close()


def main():
    args = parse_arguments()
    process(args)


if __name__ == "__main__":
    main()
