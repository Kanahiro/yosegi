import duckdb
import argparse
from dataclasses import dataclass

@dataclass
class Args:
    input_file: str
    output_file: str
    minzoom: int
    maxzoom: int
    base_resolution: float # heuristic
    geometry_column: str
    parquet_row_group_size: int

def parse_arguments():
    parser = argparse.ArgumentParser(description="Yosegi: Pyramid Parquet Generator")
    parser.add_argument("input_file", type=str, help="Path to the input Parquet file")
    parser.add_argument("output_file", type=str, help="Path to the output Yosegi file")
    parser.add_argument("--minzoom", type=int, default=0, help="Minimum zoom level (default: 0)")
    parser.add_argument("--maxzoom", type=int, default=16, help="Maximum zoom level (default: 16)")
    parser.add_argument("--base-resolution", type=float, default=2.5, help="Base resolution (default: 0.08)")
    parser.add_argument("--geometry-column", type=str, default="geometry", help="Geometry column name (default: geometry)")
    parser.add_argument("--parquet-row-group-size", type=int, default=10240, help="Parquet row group size (default: 10000)")
    args = parser.parse_args()

    return Args(
        input_file=args.input_file,
        output_file=args.output_file,
        minzoom=args.minzoom,
        maxzoom=args.maxzoom,
        base_resolution=args.base_resolution,
        geometry_column=args.geometry_column,
        parquet_row_group_size=args.parquet_row_group_size,
    )

def _precision_clause(args: Args) -> str:
    clauses = []
    for zoom in range(args.minzoom, args.maxzoom + 1):
        prec = args.base_resolution / (2 ** zoom)
        is_finest = "true" if zoom == args.maxzoom else "false"
        clauses.append(f"({zoom}, {prec}::double precision, {is_finest})")
    return ",\n".join(clauses)


def process(args: Args):
    conn = duckdb.connect()

    conn.execute("INSTALL spatial;")
    conn.execute("LOAD spatial;")

    try:
        conn.execute(f"CREATE TABLE input_data AS SELECT * FROM ST_Read('{args.input_file}');")
    except duckdb.IOException as e:
        # try read_parquet
        conn.execute(f"CREATE TABLE input_data AS SELECT * FROM read_parquet('{args.input_file}');")
    
    columns = conn.execute("PRAGMA table_info('input_data');").fetchall()

    geometry_columns = [col[1] for col in columns if col[2] == 'GEOMETRY']
    if not geometry_columns:
        raise ValueError("No geometry column found in the input data.")
    if len(geometry_columns) == 1:
        geometry_column = geometry_columns[0]
    else:
        geometry_column = args.geometry_column if args.geometry_column in geometry_columns else geometry_columns[0]

    conn.execute(f"""
    COPY (
    WITH
    base AS (
        SELECT
            i.*,
            row_number() OVER () AS _uid,
            CASE
                WHEN upper(CAST(ST_GeometryType(i.{geometry_column}) AS VARCHAR)) LIKE '%POINT%'
                    THEN i.{geometry_column}
                ELSE ST_PointOnSurface(i.{geometry_column})
            END AS _rep_geom
        FROM input_data AS i
    ),
    precision_levels AS (
        SELECT * FROM (
            VALUES
                {_precision_clause(args)}
        ) AS x(level, prec, is_finest)
    ),
    candidates AS (
        SELECT
            t._uid,
            t.level,
            t.geom_for_dedup,
            row_number() OVER (
                PARTITION BY t.level, t.geom_for_dedup
                ORDER BY t._uid
            ) AS rn
        FROM (
            SELECT
                b._uid,
                l.level,
                CASE
                    WHEN l.is_finest THEN b._rep_geom
                    ELSE ST_ReducePrecision(b._rep_geom, l.prec)
                END AS geom_for_dedup
            FROM base AS b
            CROSS JOIN precision_levels AS l
        ) AS t
    ),
    first_wins AS (
        SELECT
            _uid,
            MIN(level) AS zoomlevel
        FROM candidates
        WHERE rn = 1
        GROUP BY _uid
    )
    SELECT
        b.* EXCLUDE (_uid, _rep_geom),
        f.zoomlevel,
        ST_Quadkey(b.{geometry_column}, 23) AS quadkey
    FROM base AS b
    JOIN first_wins AS f USING (_uid)
    ORDER BY f.zoomlevel, quadkey
    ) TO '{args.output_file}' (FORMAT PARQUET, ROW_GROUP_SIZE {args.parquet_row_group_size});
    """)

    conn.close()



def main():
    args = parse_arguments()
    process(args)


if __name__ == "__main__":
    main()
