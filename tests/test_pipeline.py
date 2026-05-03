from pathlib import Path

import pyarrow.parquet as pq

from yosegi.__main__ import Args, process


def _make_args(input_file: Path, output_file: Path, **overrides) -> Args:
    return Args(
        input_file=str(input_file),
        output_file=str(output_file),
        minzoom=overrides.pop("minzoom", 0),
        maxzoom=overrides.pop("maxzoom", 6),
        resolution_base=2.5,
        resolution_multiplier=2.0,
        geometry_column=None,
        parquet_row_group_size=overrides.pop("parquet_row_group_size", 10240),
        sort_by=overrides.pop("sort_by", None),
    )


def _expected_columns() -> set[str]:
    return {"id", "geometry", "bbox", "zoomlevel", "quadkey"}


def test_polygon_pipeline_preserves_rows(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out))
    t = pq.read_table(out)
    assert t.num_rows == 2000
    assert _expected_columns() <= set(t.column_names)
    assert "_uid" not in t.column_names
    assert "_rep_geom" not in t.column_names


def test_polygon_default_sort_places_large_at_low_zoom(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out))
    t = pq.read_table(out)
    bboxes = t["bbox"].to_pylist()
    zooms = t["zoomlevel"].to_pylist()
    by_zoom: dict[int, list[float]] = {}
    for z, b in zip(zooms, bboxes):
        area = (b["xmax"] - b["xmin"]) * (b["ymax"] - b["ymin"])
        by_zoom.setdefault(z, []).append(area)
    sorted_zooms = sorted(by_zoom)
    low_mean = sum(by_zoom[sorted_zooms[0]]) / len(by_zoom[sorted_zooms[0]])
    high_mean = sum(by_zoom[sorted_zooms[-1]]) / len(by_zoom[sorted_zooms[-1]])
    assert low_mean > high_mean, (
        f"expected ST_Area DESC default to keep large polygons at low zoom, "
        f"but got low_mean={low_mean} <= high_mean={high_mean}"
    )


def test_lines_pipeline(lines_parquet: Path, tmp_path: Path) -> None:
    out = tmp_path / "out.parquet"
    process(_make_args(lines_parquet, out, maxzoom=5))
    t = pq.read_table(out)
    assert t.num_rows == 1000
    assert _expected_columns() <= set(t.column_names)


def test_points_pipeline(points_parquet: Path, tmp_path: Path) -> None:
    out = tmp_path / "out.parquet"
    process(_make_args(points_parquet, out, maxzoom=5))
    t = pq.read_table(out)
    assert t.num_rows == 1000
    assert _expected_columns() <= set(t.column_names)


def test_explicit_sort_by_overrides_default(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out, sort_by="id ASC"))
    t = pq.read_table(out)
    assert t.num_rows == 2000


def test_output_is_sorted_by_zoom_then_quadkey(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out))
    t = pq.read_table(out)
    zooms = t["zoomlevel"].to_pylist()
    quadkeys = t["quadkey"].to_pylist()
    for i in range(1, len(zooms)):
        prev = (zooms[i - 1], quadkeys[i - 1])
        curr = (zooms[i], quadkeys[i])
        assert prev <= curr, f"row {i} is out of order: {prev} > {curr}"
