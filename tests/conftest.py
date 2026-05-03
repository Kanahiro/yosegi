from pathlib import Path

import duckdb
import pytest


@pytest.fixture(scope="session")
def _duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SELECT setseed(0.42);")
    return con


@pytest.fixture(scope="session")
def polygons_parquet(
    tmp_path_factory: pytest.TempPathFactory,
    _duck: duckdb.DuckDBPyConnection,
) -> Path:
    """Synthesize 2,000 axis-aligned polygons of varying size."""
    path = tmp_path_factory.mktemp("data") / "polygons.parquet"
    _duck.execute(f"""
        COPY (
          WITH pts AS (
            SELECT i,
              (random()*170-85) AS cx,
              (random()*80-40) AS cy,
              (random()*5 + 0.01) AS sz
            FROM range(2000) t(i)
          )
          SELECT
            i AS id,
            ST_GeomFromText(
              'POLYGON((' ||
              cx || ' ' || cy || ',' ||
              (cx+sz) || ' ' || cy || ',' ||
              (cx+sz) || ' ' || (cy+sz) || ',' ||
              cx || ' ' || (cy+sz) || ',' ||
              cx || ' ' || cy ||
              '))'
            ) AS geometry
          FROM pts
        ) TO '{path}' (FORMAT 'parquet');
    """)
    return path


@pytest.fixture(scope="session")
def lines_parquet(
    tmp_path_factory: pytest.TempPathFactory,
    _duck: duckdb.DuckDBPyConnection,
) -> Path:
    path = tmp_path_factory.mktemp("data") / "lines.parquet"
    _duck.execute(f"""
        COPY (
          WITH pts AS (
            SELECT i,
              (random()*170-85) AS cx,
              (random()*80-40) AS cy,
              (random()*5 + 0.01) AS dx,
              (random()*5 + 0.01) AS dy
            FROM range(1000) t(i)
          )
          SELECT
            i AS id,
            ST_GeomFromText(
              'LINESTRING(' || cx || ' ' || cy || ',' ||
              (cx+dx) || ' ' || (cy+dy) || ')'
            ) AS geometry
          FROM pts
        ) TO '{path}' (FORMAT 'parquet');
    """)
    return path


@pytest.fixture(scope="session")
def points_parquet(
    tmp_path_factory: pytest.TempPathFactory,
    _duck: duckdb.DuckDBPyConnection,
) -> Path:
    path = tmp_path_factory.mktemp("data") / "points.parquet"
    _duck.execute(f"""
        COPY (
          SELECT
            i AS id,
            ST_Point(random()*170-85, random()*80-40) AS geometry
          FROM range(1000) t(i)
        ) TO '{path}' (FORMAT 'parquet');
    """)
    return path
