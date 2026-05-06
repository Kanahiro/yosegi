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

## Demo

<https://dmsd2c92bdh54.cloudfront.net/index.html>

- DeckGL + GeoArrowScatterPlotLayer + DuckDB
  - Query Parquet with DuckDB and pass results to GeoArrowScatterPlotLayer

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
