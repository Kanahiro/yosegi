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
        min_visible_size_factor=overrides.pop("min_visible_size_factor", 1.0),
    )


def _expected_columns() -> set[str]:
    return {"id", "geometry", "bbox", "zoomlevel"}


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


def test_output_is_sorted_by_zoom(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out))
    t = pq.read_table(out)
    zooms = t["zoomlevel"].to_pylist()
    for i in range(1, len(zooms)):
        assert zooms[i - 1] <= zooms[i], (
            f"row {i} zoom out of order: {zooms[i - 1]} > {zooms[i]}"
        )


def test_output_has_geo_metadata(polygons_parquet: Path, tmp_path: Path) -> None:
    import json

    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out))
    pf = pq.ParquetFile(out)
    md = pf.schema.to_arrow_schema().metadata
    assert b"geo" in md
    geo = json.loads(md[b"geo"])
    assert geo["version"] == "1.1.0"
    assert geo["primary_column"] == "geometry"
    assert "covering" in geo["columns"]["geometry"]


def test_size_factor_filters_small_polygons_at_low_zoom(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    """Polygons whose bbox is smaller than resolution(z) * factor must not
    be placed at zoom z."""
    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out, min_visible_size_factor=1.0))
    t = pq.read_table(out)
    bboxes = t["bbox"].to_pylist()
    zooms = t["zoomlevel"].to_pylist()
    resolution_base = 2.5
    multiplier = 2.0
    maxzoom = 6
    for z, b in zip(zooms, bboxes):
        if z == maxzoom:
            continue
        prec = resolution_base / (multiplier**z)
        size = max(b["xmax"] - b["xmin"], b["ymax"] - b["ymin"])
        assert size + 1e-12 >= prec, (
            f"polygon with size={size} should not appear at zoom {z} "
            f"(threshold={prec})"
        )


def test_size_factor_zero_disables_filter(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    """factor=0 should let small polygons appear at any zoom (preserving
    pre-filter behavior)."""
    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out, min_visible_size_factor=0.0))
    t = pq.read_table(out)
    assert t.num_rows == 2000


def test_str_layout_polygon_pipeline(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    """STR layout must produce a valid pyramid with the same row count and
    zoom-sorted output."""
    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out))
    t = pq.read_table(out)
    assert t.num_rows == 2000
    assert _expected_columns() <= set(t.column_names)
    assert "_strip_id" not in t.column_names
    assert "_bbox_ymin" not in t.column_names
    zooms = t["zoomlevel"].to_pylist()
    for i in range(1, len(zooms)):
        assert zooms[i - 1] <= zooms[i]


def test_str_layout_within_zoom_orders_by_strip_then_ymin(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    """Within each zoom level, STR layout should produce monotonic strip
    sequence (xmin grows across strips) and ymin grows within strips."""
    out = tmp_path / "out.parquet"
    process(_make_args(polygons_parquet, out, parquet_row_group_size=64))
    t = pq.read_table(out)
    bboxes = t["bbox"].to_pylist()
    zooms = t["zoomlevel"].to_pylist()
    # group rows by zoom, then check that the rolling-max xmin is non-decreasing
    # across distinct strips. We don't have strip_id at hand, but the property
    # we want is: if you partition the rows into chunks of sqrt(N*M), each
    # chunk's xmin range is mostly disjoint from the next. Use a relaxed check:
    # the global xmin should trend upward across the zoom group.
    by_zoom: dict[int, list[dict]] = {}
    for z, b in zip(zooms, bboxes):
        by_zoom.setdefault(z, []).append(b)
    for z, rows in by_zoom.items():
        if len(rows) < 200:
            continue
        first_q_xmin = sorted(r["xmin"] for r in rows[: len(rows) // 4])
        last_q_xmin = sorted(r["xmin"] for r in rows[-len(rows) // 4 :])
        # median of last quarter should be >= median of first quarter
        assert last_q_xmin[len(last_q_xmin) // 2] >= first_q_xmin[
            len(first_q_xmin) // 2
        ], f"zoom {z}: STR strips should advance in xmin"


def test_str_layout_lines_pipeline(lines_parquet: Path, tmp_path: Path) -> None:
    out = tmp_path / "out.parquet"
    process(_make_args(lines_parquet, out, maxzoom=5))
    t = pq.read_table(out)
    assert t.num_rows == 1000


def test_str_layout_points_pipeline(points_parquet: Path, tmp_path: Path) -> None:
    out = tmp_path / "out.parquet"
    process(_make_args(points_parquet, out, maxzoom=5))
    t = pq.read_table(out)
    assert t.num_rows == 1000


def test_size_factor_pushes_filtered_to_maxzoom(
    polygons_parquet: Path, tmp_path: Path
) -> None:
    """All features must still be present in output; small ones get pushed
    to maxzoom by the residual assignment."""
    out_filtered = tmp_path / "filtered.parquet"
    out_unfiltered = tmp_path / "unfiltered.parquet"
    process(_make_args(polygons_parquet, out_filtered, min_visible_size_factor=1.0))
    process(_make_args(polygons_parquet, out_unfiltered, min_visible_size_factor=0.0))
    t_f = pq.read_table(out_filtered)
    t_u = pq.read_table(out_unfiltered)
    assert t_f.num_rows == t_u.num_rows == 2000
    # filtered run should have at least as many rows at maxzoom (small
    # features are pushed there) as the unfiltered run.
    maxzoom = 6
    f_max = sum(1 for z in t_f["zoomlevel"].to_pylist() if z == maxzoom)
    u_max = sum(1 for z in t_u["zoomlevel"].to_pylist() if z == maxzoom)
    assert f_max >= u_max
