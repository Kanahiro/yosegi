"""Measure pyramid GeoParquet output characteristics.

What we measure
---------------
1. Build time and output file size.
2. For a set of randomly sampled tile-bbox queries at multiple zoom levels:
   - Static analysis: how many Parquet row groups have a bbox that intersects
     the query bbox (= number of row groups DuckDB *would* touch with bbox
     covering pushdown). Lower is better.
   - Wall-clock query latency through DuckDB (median of N runs).

Usage
-----
    uv run python benchmarks/bench_layout.py
    uv run python benchmarks/bench_layout.py --features 50000 --queries 200
    uv run python benchmarks/bench_layout.py --input my-dataset.parquet

If --input is omitted a synthetic polygon dataset is generated with a
heavy-tailed size distribution (few huge polygons mixed with many small
ones), the case where bbox-covering pruning matters most.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from yosegi.__main__ import Args, process


# ---------------------------------------------------------------------------
# Synthetic data
# ---------------------------------------------------------------------------


def synth_polygons(
    path: Path,
    n: int,
    seed: int = 0,
    distribution: str = "uniform",
) -> tuple[float, float, float, float]:
    """Generate `n` axis-aligned polygons with a heavy-tailed size distribution.

    distribution:
      - "uniform"   : centers uniformly random in [-170,170] x [-75,75].
      - "clustered" : centers concentrated near ~30 random hotspots
                      (Gaussian-blob mixture). Mimics real geospatial data
                      where features cluster in cities/regions.
    Size distribution is heavy-tailed in both cases: ~85% small, ~12% medium,
    ~3% large (continent-scale).
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SELECT setseed({seed / 1000.0});")

    if distribution == "uniform":
        sql = f"""
        COPY (
          WITH pts AS (
            SELECT
              i,
              (random()*340-170) AS cx,
              (random()*150-75)  AS cy,
              CASE
                WHEN random() < 0.85 THEN random()*0.05 + 0.001
                WHEN random() < 0.97 THEN random()*2.0 + 0.05
                ELSE random()*40 + 2.0
              END AS sz
            FROM range({n}) t(i)
          )
          SELECT
            i AS id,
            ST_GeomFromText(
              'POLYGON((' || cx||' '||cy || ',' ||
              (cx+sz)||' '||cy || ',' ||
              (cx+sz)||' '||(cy+sz) || ',' ||
              cx||' '||(cy+sz) || ',' ||
              cx||' '||cy || '))'
            ) AS geometry
          FROM pts
        ) TO '{path}' (FORMAT 'parquet');
        """
    elif distribution == "clustered":
        sql = f"""
        COPY (
          WITH hotspots AS (
            SELECT
              h,
              (random()*340-170) AS hx,
              (random()*150-75)  AS hy,
              random()*8 + 1     AS hsigma
            FROM range(30) t(h)
          ),
          pts AS (
            SELECT
              i,
              h.hx + h.hsigma * (random()+random()+random()+random()-2) AS cx,
              h.hy + h.hsigma * (random()+random()+random()+random()-2) AS cy,
              CASE
                WHEN random() < 0.85 THEN random()*0.05 + 0.001
                WHEN random() < 0.97 THEN random()*2.0 + 0.05
                ELSE random()*40 + 2.0
              END AS sz
            FROM range({n}) t(i)
            JOIN hotspots h ON h.h = t.i % 30
          )
          SELECT
            i AS id,
            ST_GeomFromText(
              'POLYGON((' || cx||' '||cy || ',' ||
              (cx+sz)||' '||cy || ',' ||
              (cx+sz)||' '||(cy+sz) || ',' ||
              cx||' '||(cy+sz) || ',' ||
              cx||' '||cy || '))'
            ) AS geometry
          FROM pts
        ) TO '{path}' (FORMAT 'parquet');
        """
    else:
        raise ValueError(f"unknown distribution: {distribution}")

    con.execute(sql)
    bx = con.execute(
        f"""
        SELECT MIN(ST_XMin(geometry)), MIN(ST_YMin(geometry)),
               MAX(ST_XMax(geometry)), MAX(ST_YMax(geometry))
        FROM read_parquet('{path}')
        """
    ).fetchone()
    con.close()
    assert bx is not None
    return (float(bx[0]), float(bx[1]), float(bx[2]), float(bx[3]))


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


@dataclass
class BuildResult:
    out_path: Path
    build_seconds: float
    file_bytes: int
    row_groups: int
    total_rows: int


def build(
    input_path: Path,
    out_dir: Path,
    minzoom: int,
    maxzoom: int,
    row_group_size: int,
) -> BuildResult:
    out = out_dir / "pyramid.parquet"
    args = Args(
        input_file=str(input_path),
        output_file=str(out),
        minzoom=minzoom,
        maxzoom=maxzoom,
        resolution_base=2.5,
        resolution_multiplier=2.0,
        geometry_column=None,
        parquet_row_group_size=row_group_size,
        sort_by=None,
        min_visible_size_factor=1.0,
    )
    t0 = time.perf_counter()
    process(args)
    elapsed = time.perf_counter() - t0
    pf = pq.ParquetFile(out)
    return BuildResult(
        out_path=out,
        build_seconds=elapsed,
        file_bytes=out.stat().st_size,
        row_groups=pf.num_row_groups,
        total_rows=pf.metadata.num_rows,
    )


# ---------------------------------------------------------------------------
# Row-group bbox extraction (for static selectivity analysis)
# ---------------------------------------------------------------------------


@dataclass
class RowGroupStat:
    idx: int
    zoom_min: int
    zoom_max: int
    xmin: float
    ymin: float
    xmax: float
    ymax: float
    rows: int
    byte_size: int


def row_group_stats(parquet_path: Path) -> list[RowGroupStat]:
    pf = pq.ParquetFile(parquet_path)
    md = pf.metadata
    out: list[RowGroupStat] = []
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        leaves: dict[str, object] = {}
        for i in range(rg.num_columns):
            c = rg.column(i)
            if c.statistics is not None:
                leaves[c.path_in_schema] = c.statistics
        sx_min = leaves["bbox.xmin"].min  # type: ignore[union-attr]
        sy_min = leaves["bbox.ymin"].min  # type: ignore[union-attr]
        sx_max = leaves["bbox.xmax"].max  # type: ignore[union-attr]
        sy_max = leaves["bbox.ymax"].max  # type: ignore[union-attr]
        z_min = leaves["zoomlevel"].min  # type: ignore[union-attr]
        z_max = leaves["zoomlevel"].max  # type: ignore[union-attr]
        out.append(
            RowGroupStat(
                idx=g,
                zoom_min=int(z_min),
                zoom_max=int(z_max),
                xmin=float(sx_min),
                ymin=float(sy_min),
                xmax=float(sx_max),
                ymax=float(sy_max),
                rows=rg.num_rows,
                byte_size=rg.total_byte_size,
            )
        )
    return out


def overlaps(rg: RowGroupStat, qx1: float, qy1: float, qx2: float, qy2: float) -> bool:
    return not (rg.xmax < qx1 or rg.xmin > qx2 or rg.ymax < qy1 or rg.ymin > qy2)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


@dataclass
class Query:
    z: int
    xmin: float
    ymin: float
    xmax: float
    ymax: float


def gen_queries(
    bbox: tuple[float, float, float, float],
    zooms: list[int],
    per_zoom: int,
    seed: int = 0,
) -> list[Query]:
    rng = random.Random(seed)
    xmin0, ymin0, xmax0, ymax0 = bbox
    out: list[Query] = []
    for z in zooms:
        tile_w = 360.0 / (2**z)
        tile_h = 180.0 / (2**z)
        for _ in range(per_zoom):
            cx = rng.uniform(xmin0, xmax0)
            cy = rng.uniform(ymin0, ymax0)
            out.append(
                Query(
                    z=z,
                    xmin=cx - tile_w / 2,
                    ymin=cy - tile_h / 2,
                    xmax=cx + tile_w / 2,
                    ymax=cy + tile_h / 2,
                )
            )
    return out


def static_selectivity(
    rgs: list[RowGroupStat], queries: list[Query]
) -> dict[int, dict[str, float]]:
    by_z: dict[int, list[int]] = {}
    by_z_bytes: dict[int, list[int]] = {}
    for q in queries:
        touched_rg = 0
        touched_bytes = 0
        for rg in rgs:
            if rg.zoom_min > q.z:
                continue
            if overlaps(rg, q.xmin, q.ymin, q.xmax, q.ymax):
                touched_rg += 1
                touched_bytes += rg.byte_size
        by_z.setdefault(q.z, []).append(touched_rg)
        by_z_bytes.setdefault(q.z, []).append(touched_bytes)
    out: dict[int, dict[str, float]] = {}
    for z in by_z:
        out[z] = {
            "row_groups_mean": statistics.mean(by_z[z]),
            "row_groups_p95": _pctl(by_z[z], 0.95),
            "bytes_mean": statistics.mean(by_z_bytes[z]),
            "bytes_p95": _pctl(by_z_bytes[z], 0.95),
        }
    return out


def _pctl(xs: list[int] | list[float], p: float) -> float:
    s = sorted(xs)
    if not s:
        return 0.0
    k = max(0, min(len(s) - 1, int(p * len(s))))
    return float(s[k])


# ---------------------------------------------------------------------------
# Live DuckDB query benchmark
# ---------------------------------------------------------------------------


def _wall_clock(parquet_path: Path, queries: list[Query], runs: int) -> dict[int, float]:
    """Return per-zoom mean of per-query median wall-clock seconds."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("PRAGMA disable_progress_bar;")

    con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()

    by_z: dict[int, list[float]] = {}
    for q in queries:
        timings: list[float] = []
        sql = f"""
            SELECT COUNT(*) FROM read_parquet('{parquet_path}')
            WHERE zoomlevel <= {q.z}
              AND bbox.xmax >= {q.xmin}
              AND bbox.xmin <= {q.xmax}
              AND bbox.ymax >= {q.ymin}
              AND bbox.ymin <= {q.ymax}
        """
        for _ in range(runs):
            t0 = time.perf_counter()
            con.execute(sql).fetchone()
            timings.append(time.perf_counter() - t0)
        by_z.setdefault(q.z, []).append(statistics.median(timings))
    con.close()
    return {z: statistics.mean(ts) for z, ts in by_z.items()}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=str, default=None,
                    help="input geo file. If omitted, synthesize polygons.")
    ap.add_argument("--features", type=int, default=50000,
                    help="number of synthetic polygons (default: 50000)")
    ap.add_argument("--distribution", type=str, default="uniform",
                    choices=["uniform", "clustered"],
                    help="synthetic spatial distribution (default: uniform)")
    ap.add_argument("--minzoom", type=int, default=0)
    ap.add_argument("--maxzoom", type=int, default=10)
    ap.add_argument("--row-group-size", type=int, default=10240)
    ap.add_argument("--queries", type=int, default=100,
                    help="random tile queries per zoom level (default: 100)")
    ap.add_argument("--query-zooms", type=str, default="6,8,10",
                    help="comma-separated zoom levels for queries")
    ap.add_argument("--runs", type=int, default=3,
                    help="repeats per query for median wall-clock (default: 3)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out-dir", type=str, default=None,
                    help="directory for generated parquet files (default: tmp)")
    ap.add_argument("--no-wall-clock", action="store_true",
                    help="skip DuckDB live timing (use only static analysis)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else Path("/tmp/yosegi-bench")
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.input:
        input_path = Path(args.input)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        try:
            bx = con.execute(
                f"SELECT MIN(ST_XMin(geometry)), MIN(ST_YMin(geometry)),"
                f"       MAX(ST_XMax(geometry)), MAX(ST_YMax(geometry))"
                f" FROM read_parquet('{input_path}')"
            ).fetchone()
        except duckdb.IOException:
            bx = con.execute(
                f"SELECT MIN(ST_XMin(geometry)), MIN(ST_YMin(geometry)),"
                f"       MAX(ST_XMax(geometry)), MAX(ST_YMax(geometry))"
                f" FROM ST_Read('{input_path}')"
            ).fetchone()
        con.close()
        assert bx is not None
        bbox = (float(bx[0]), float(bx[1]), float(bx[2]), float(bx[3]))
    else:
        input_path = out_dir / f"synth-{args.distribution}.parquet"
        print(
            f"[synth] generating {args.features} polygons "
            f"({args.distribution}) → {input_path}"
        )
        bbox = synth_polygons(
            input_path, args.features,
            seed=args.seed, distribution=args.distribution,
        )
    print(f"[input] bbox = {bbox}")

    zooms = [int(z) for z in args.query_zooms.split(",") if z.strip()]
    queries = gen_queries(bbox, zooms, args.queries, seed=args.seed)
    print(f"[queries] {len(queries)} queries across zooms {zooms}")

    print("\n=== building pyramid ===")
    b = build(
        input_path,
        out_dir,
        minzoom=args.minzoom,
        maxzoom=args.maxzoom,
        row_group_size=args.row_group_size,
    )
    rgs = row_group_stats(b.out_path)
    sel = static_selectivity(rgs, queries)
    timing = (
        {} if args.no_wall_clock else _wall_clock(b.out_path, queries, args.runs)
    )

    # --- report -----------------------------------------------------------
    print("\n" + "=" * 78)
    print("BUILD")
    print("=" * 78)
    print(f"{'build_s':>10} {'file_MB':>10} {'row_groups':>12} {'rows':>12}")
    print(
        f"{b.build_seconds:>10.2f} "
        f"{b.file_bytes / 1024 / 1024:>10.2f} "
        f"{b.row_groups:>12d} {b.total_rows:>12d}"
    )

    print("\n" + "=" * 78)
    print("STATIC SELECTIVITY (row groups touched per query, lower = better)")
    print("=" * 78)
    print(f"{'zoom':>6} {'rg_mean':>10} {'rg_p95':>10} "
          f"{'bytes_mean':>14} {'bytes_p95':>14}")
    for z in sorted(sel):
        s = sel[z]
        print(
            f"{z:>6} "
            f"{s['row_groups_mean']:>10.1f} {s['row_groups_p95']:>10.0f} "
            f"{s['bytes_mean']:>14,.0f} {s['bytes_p95']:>14,.0f}"
        )

    if not args.no_wall_clock:
        print("\n" + "=" * 78)
        print("WALL-CLOCK (mean of per-query medians, seconds)")
        print("=" * 78)
        print(f"{'zoom':>6} {'sec':>10}")
        for z in sorted(timing):
            print(f"{z:>6} {timing[z]:>10.4f}")

    payload = {
        "input": str(input_path),
        "bbox": list(bbox),
        "zooms": zooms,
        "queries_per_zoom": args.queries,
        "row_group_size": args.row_group_size,
        "build_seconds": b.build_seconds,
        "file_bytes": b.file_bytes,
        "row_groups": b.row_groups,
        "total_rows": b.total_rows,
        "static": sel,
        "wall_clock": timing,
    }
    json_path = out_dir / "bench-results.json"
    json_path.write_text(json.dumps(payload, indent=2))
    print(f"\n[json] wrote {json_path}")


if __name__ == "__main__":
    main()
