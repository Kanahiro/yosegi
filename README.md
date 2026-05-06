# Yosegi - Pyramid (Geo)Parquet Generator

Yosegi is a tool to generate Pyramid (Geo)Parquet files - optimized for streaming large geospatial datasets.

## Usage

```bash
pip install yosegi
yosegi input.parquet output.pyramid.parquet # GeoParquet
yosegi input.shp output.pyramid.parquet # you can use GDAL/OGR formats
```

```
# yosegi -h

Yosegi: Pyramid Parquet Generator

positional arguments:
  input_file            Path to the input file
  output_file           Path to the output file

options:
  -h, --help            show this help message and exit
  --minzoom MINZOOM     Minimum zoom level (default: 0)
  --maxzoom MAXZOOM     Maximum zoom level (default: 16)
  --resolution-base RESOLUTION_BASE
                        Base resolution (default: 2.5)
  --resolution-multiplier RESOLUTION_MULTIPLIER
                        Resolution multiplier (default: 2.0)
  --geometry-column GEOMETRY_COLUMN
                        Geometry column name (auto-detected if not specified)
  --parquet-row-group-size PARQUET_ROW_GROUP_SIZE
                        Parquet row group size (default: 10240)
  --sort-by SORT_BY     Sort key for feature thinning
                        (default: ST_Area DESC for polygons,
                                  ST_Length DESC for lines,
                                  hash(_uid) otherwise)
  --min-visible-size-factor MIN_VISIBLE_SIZE_FACTOR
                        Skip features at low zoom whose bbox is smaller than
                        the grid resolution times this factor (default: 1.0,
                        0 to disable)
```

## Overview of Pyramid (Geo)Parquet

### Concept

- Pre-calculate which features are visible at each zoomlevel (density-based thinning).
- Emit a per-row `bbox` covering column referenced via [GeoParquet 1.1 `covering` metadata](https://geoparquet.org/releases/v1.1.0/) so spatial readers can prune row groups via column statistics.
- Sort the output so that:
  - Each row group contains rows from exactly one zoomlevel (zoom-aligned RGs → tight `zoomlevel` stats).
  - Within each zoomlevel, rows follow [Sort-Tile-Recursive (STR) bbox packing](https://www.researchgate.net/publication/2629750_STR_A_Simple_and_Efficient_Algorithm_for_R-Tree_Packing): bbox-center-x is divided into ~√(N/M) strips, then bbox-center-y inside each strip is ordered with boustrophedon (alternating direction). This gives tight per-RG bbox stats so that bbox-covering predicates prune effectively.

**By these steps, generate Pyramid-structure in a single Parquet file, just like a GeoTIFF pyramid.**

- Like GeoTIFF, an overview of the entire dataset can be obtained quickly.
- Unlike GeoTIFF, lower-resolution data is not repeated because this is vector.

#### QGIS: read Pyramid parquet on Amazon S3. Blue to Red means zoomlevel. Data: OvertureMaps

<https://github.com/user-attachments/assets/4df86816-559d-4b34-b57a-2f3d4b8bd14c>

#### QGIS: loading raw parquet (sorted only spatially)

<https://github.com/user-attachments/assets/4e7a61f2-eb78-4658-a55f-8de31e2796c9>

*Well sorted spatially but it takes too much time to obtain an overview of the entire dataset.*

#### Browser (DeckGL + DuckDB): load that parquet and render with [GeoArrowScatterPlotLayer](https://github.com/geoarrow/deck.gl-layers)

<https://github.com/user-attachments/assets/26e2f662-474b-4d11-ab56-f73587ef8b2e>

### Table Structure

Original input columns are preserved. Yosegi appends two columns:

| Column | Type | Purpose |
|---|---|---|
| `bbox` | `STRUCT<xmin, ymin, xmax, ymax: DOUBLE>` | GeoParquet 1.1 covering bbox per feature; enables row-group pruning |
| `zoomlevel` | `INT32` | Lowest zoom at which the feature is visible (pyramid level) |

The `geometry` column is rewritten as WKB. GeoParquet `covering.bbox` metadata points to the `bbox` STRUCT child fields so that compliant readers automatically use it for spatial pushdown.

If the input already has a column named `bbox` or `zoomlevel`, the input column is dropped and overwritten by yosegi's value (a notice is logged).

```planetext
┌──────────────────────┬──────────────────────┬───┬──────────────────────────────────────┬───────────┐
│          id          │       geometry       │ … │                 bbox                 │ zoomlevel │
│       varchar        │         blob         │   │ struct(xmin double, ymin double, …)  │   int32   │
├──────────────────────┼──────────────────────┼───┼──────────────────────────────────────┼───────────┤
│ 5e1da825-ef9b-45dd…  │ <wkb POINT>          │ … │ {xmin: 122.30, ymin: 45.40, xmax: …} │         0 │
│ 8eb4aa8c-81fb-4a9b…  │ <wkb POINT>          │ … │ {xmin: 122.29, ymin: 45.41, xmax: …} │         0 │
│ … (zoom 0 features) │ …                    │ … │ …                                    │         0 │
│ … (zoom 1 features) │ …                    │ … │ …                                    │         1 │
│ …                    │ …                    │ … │ …                                    │         · │
│ … (zoom 16 features)│ …                    │ … │ …                                    │        16 │
└──────────────────────┴──────────────────────┴───┴──────────────────────────────────────┴───────────┘
```

### Querying tiles

Use the bbox covering column for spatial pushdown plus `zoomlevel` for pyramid pruning:

```sql
-- DuckDB: features visible at z<=10 inside a tile bbox
SELECT * FROM 'example.pyramid.parquet'
WHERE zoomlevel <= 10
  AND bbox.xmax >= :tile_minx AND bbox.xmin <= :tile_maxx
  AND bbox.ymax >= :tile_miny AND bbox.ymin <= :tile_maxy;
```

DuckDB's parquet reader prunes row groups using the `bbox.*` and `zoomlevel` column statistics, so only the matching RGs are downloaded over HTTP.

> Note: `geometry && ST_MakeEnvelope(...)` returns the same rows but does not push down to row-group statistics, so it reads geometry pages for every candidate RG. Prefer the explicit `bbox.*` predicates above.

## Physical layout: STR-pack

Within each zoomlevel, yosegi orders rows with [Sort-Tile-Recursive (STR) bbox packing](https://www.researchgate.net/publication/3686660_STR_A_Simple_and_Efficient_Algorithm_for_R-Tree_Packing). The goal is to keep each row group's bbox as compact as possible so that bbox-covering predicates prune effectively.

### The algorithm

Let *N* be the number of rows in this zoom and *M* the row group size (`--parquet-row-group-size`).

1. **Strip by bbox-center-x.** Sort all *N* rows by `(xmin + xmax) / 2` and chunk them into strips of `strip_size = M × round(√(N/M))` rows. There are roughly `√(N/M)` strips, each containing roughly `√(N/M)` row groups.
2. **Sort by bbox-center-y inside each strip, with boustrophedon.** Even-numbered strips are sorted ascending in y, odd-numbered strips descending. Consecutive features at strip boundaries stay close in y.

The SQL pattern (simplified):

```sql
ORDER BY
  FLOOR((ROW_NUMBER() OVER (ORDER BY xc) - 1) / strip_size),
  CASE WHEN strip_id % 2 = 0 THEN  yc ELSE -yc END
```

### Strip boundaries align with RG boundaries

`strip_size` is always a multiple of *M*, so when pyarrow writes one row group every *M* rows, **every spatial discontinuity (jumping from one strip's x-range to the next) lands on a row group boundary**. No row group ever spans two strips. Per-RG bbox extent is bounded by:

- x extent ≤ one strip width ≈ total x range / `√(N/M)`
- y extent ≤ one tile height ≈ total y range / `√(N/M)`

That is, each RG covers a near-square tile of area ≈ *M / N* of the data extent — within a constant factor of the theoretical optimum.

```
file order →
┌──── strip 0 (low x) ────┐┌──── strip 1 (mid x) ────┐┌──── strip 2 ────
│  RG0  RG1  RG2 ... RG13 ││  RG14  RG15 ... RG27    ││  ...
│   ↑                      ││   ↓                      ││   ↑
│   y low → → → → → y high ││   y high → → → → → y low ││   y low → ...
└──────────────────────────┘└──────────────────────────┘└─────────────
                            ★ strip boundary == RG boundary; x jumps,
                              y stays at high (boustrophedon)
```

### Why not Hilbert curve?

Earlier versions used `ST_Hilbert(point, bounds)` over a representative point. STR-pack consistently touches **27–37% fewer row groups** for tile-bbox queries on heavy-tailed feature-size data, regardless of whether Hilbert is fed the centroid or the bbox-center. The gap is structural, not about the input point:

- **Curve jumps.** Hilbert can place spatially distant points consecutively at recursive sub-quadrant boundaries. If such a jump lands inside a row group window, that RG's bbox spans both sub-quadrants. STR has no such jumps — strip and tile boundaries are contiguous by construction and are pinned to RG boundaries.
- **Aspect ratio.** Hilbert quantizes to a square grid. STR's strip count adapts to the actual row count, not to the bounding box shape.
- **No worst-case bound.** Hilbert's per-RG extent is small *on average* but unbounded in the worst case. STR gives an analytic upper bound (one strip × one tile).

Empirical comparison on 200k synthetic features with heavy-tailed sizes (RG size 1000, 240 random tile bbox queries at z=8/10/12):

| layout | mean RGs touched | mean bytes touched | ratio vs STR |
|---|---|---|---|
| STR-pack | ~7–8 | ~1.0 MB | 1.00× |
| Hilbert (PointOnSurface) | ~10–12 | ~1.4 MB | 1.30–1.37× |
| Hilbert (bbox-center)    | ~10–12 | ~1.4 MB | 1.33–1.38× |

The two Hilbert variants are within noise of each other — the loss is intrinsic to mapping 2D to 1D, not to the choice of input point.

## Demo

<https://d3ks1i00ysei8.cloudfront.net/>

- Client - Lambda - S3(Pyramid Parquet) architecture.

## Benefits

- Single Parquet can be used for both storage and streaming.
- Overview of the entire dataset is obtained quickly — much faster than with a plain spatial sort (quadkey / Hilbert) because lower zooms contain only the features needed at that resolution.
- Efficient row-group pruning thanks to GeoParquet 1.1 bbox covering + zoom-aligned row groups, so HTTP-range reads of huge files (100s of MB) are minimized.

## How is zoomlevel calculated?

A density-based clustering approach is used. At lower zoom we don't need to render every feature, so only essential features are kept at lower zoomlevels. Repeating this process from `--minzoom` to `--maxzoom` decides which features are visible at each zoom.

`--sort-by` controls the priority order used during thinning (default keeps larger polygons / longer lines first; smaller ones are pushed to deeper zooms).

`--min-visible-size-factor` additionally skips features whose bbox is too small to be visible at a given zoom's grid resolution.

## Why bbox covering instead of quadkey?

Earlier versions of yosegi appended a `quadkey` column and queried with `quadkey LIKE 'prefix%'`. This was replaced by the GeoParquet 1.1 bbox covering pattern because:

- Bbox struct min/max statistics are written by parquet writers automatically; row-group pruning is fully transparent to the consumer.
- A query bbox doesn't have to be aligned to a tile boundary.
- It's the canonical mechanism in the GeoParquet 1.1 specification, supported out of the box by DuckDB, GeoPandas, GDAL, and SedonaDB.
- The output stays a clean GeoParquet — no yosegi-specific column required for spatial filtering.

## Why not tiles?

- Building a tileset purely for streaming is operational overhead; streaming directly from one source file is simpler.
- Lower-zoom tile contents are wasted once you zoom in past them, and the same feature is repeated across zooms.
- Mapbox Vector Tiles are a lossy format.

## Why not FlatGeobuf?

FlatGeobuf is also oriented to single-file storage and streaming, but it has no pyramid structure, so:

- An overview of the whole dataset can't be obtained quickly.
- A dense area's tile-based query may pull too many features.

## References

- <https://medium.com/radiant-earth-insights/using-duckdbs-hilbert-function-with-geop-8ebc9137fb8a>
- <https://medium.com/radiant-earth-insights/the-admin-partitioned-geoparquet-distribution-59f0ca1c6d96>
- <https://geoparquet.org/releases/v1.1.0/>
- <https://github.com/felt/tippecanoe>
