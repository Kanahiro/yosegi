# Yosegi - Pyramid (Geo)Parquet Generator

Yosegi is a tool to generate Pyramid (Geo)Parquet files - optimized for streaming large geospatial datasets.

## Usage

```bash
uv sync
uv run main.py input.shp output.parquet --minzoom 0 --maxzoom 14
# other options are available
uv run main.py -h
```

## Overview of Pyramid (Geo)Parquet

### Concept

- Pre-calculate which features are visible at each zoomlevel.
- Pre-calculate quadkey for each feature.
- Sort features by zoomlevel and quadkey.

**By these steps, generate Pyramid-structure in a single Parquet file, just like GeoTIFF pyramid.** With pyramid structure:

- Overview of entire data can be obtained quickly.
- Unlike GeoTIFF, lower resolution data are not repeadted because it is vector.

<https://github.com/user-attachments/assets/4df86816-559d-4b34-b57a-2f3d4b8bd14c>

*QGIS: read Pyramid parquet on Amazon S3. Blue to Red means zoomlevel. Data: OvertureMaps*

<details><summary>Example: loading raw parquet (sorted only by spatially)</summary>

<https://github.com/user-attachments/assets/4e7a61f2-eb78-4658-a55f-8de31e2796c9>

Well sorted spatially but it takes too much time to obtain overview of entire dataset.

</details>

<https://github.com/user-attachments/assets/26e2f662-474b-4d11-ab56-f73587ef8b2e>

*Browser(DeckGL + DuckDB): load same parquet and rendered with [GeoArrowScatterPlotLayer](https://github.com/geoarrow/deck.gl-layers)*

#### Structure

Original:

```planetext
┌──────────────────────┬──────────────────────┬───┬──────────────────────┬─────────┬─────────┐
│          id          │       geometry       │ … │       filename       │  theme  │  type   │
│       varchar        │       geometry       │   │       varchar        │ varchar │ varchar │
├──────────────────────┼──────────────────────┼───┼──────────────────────┼─────────┼─────────┤
│ 5e1da825-ef9b-45dd…  │ POINT (122.309211 …  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 0c8ef190-3302-457d…  │ POINT (122.23393 4…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 6030866d-d428-4411…  │ POINT (122.164515 …  │ … │ s3://overturemaps-…  │ places  │ place   │
│ e59b1a93-383d-4d4b…  │ POINT (122.40588 4…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 993b4cb1-1dce-45c5…  │ POINT (122.8591 45…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 193eb59f-2cbf-49aa…  │ POINT (122.572 45.…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ a74d5f2e-751a-4297…  │ POINT (123.188563 …  │ … │ s3://overturemaps-…  │ places  │ place   │
│ fd94035d-26db-4b79…  │ POINT (123.164364 …  │ … │ s3://overturemaps-…  │ places  │ place   │
│ a46dcdef-e802-4928…  │ POINT (122.8753426…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 025180d0-1100-46ab…  │ POINT (122.857046 …  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 09d95c3e-99d6-4ef2…  │ POINT (122.827855 …  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 97b500b4-d540-4a08…  │ POINT (122.8279423…  │ … │ s3://overturemaps-…  │ places  │ place   │
│          ·           │          ·           │ · │          ·           │   ·     │   ·     │
│          ·           │          ·           │ · │          ·           │   ·     │   ·     │
│          ·           │          ·           │ · │          ·           │   ·     │   ·     │
│ ec9469f0-bb92-490e…  │ POINT (122.34375 2…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 16ef1bd2-aeba-473c…  │ POINT (137.3721886…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 462ff6d1-f1af-4100…  │ POINT (137.2338562…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ de4288c0-93b2-4a78…  │ POINT (138.3370972…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ a44ef5e9-ead6-45c3…  │ POINT (140.770368 …  │ … │ s3://overturemaps-…  │ places  │ place   │
│ c220f122-a7d7-4991…  │ POINT (144.6288514…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 3ee9fcf8-6684-4d01…  │ POINT (144.8833333…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ a901c450-3c83-4ffb…  │ POINT (144.1404963…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 3d8e1e58-a107-4ba0…  │ POINT (147.5786018…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ f3c402dc-3f8b-4b87…  │ POINT (146.5937114…  │ … │ s3://overturemaps-…  │ places  │ place   │
│ 083f2b74-163a-4427…  │ POINT (149.2461111…  │ … │ s3://overturemaps-…  │ places  │ place   │
├──────────────────────┴──────────────────────┴───┴──────────────────────┴─────────┴─────────┤
│ 3252680 rows (3.25 million rows, 40 shown)                            19 columns (5 shown) │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

Pyramid structure:

```planetext
┌──────────────────────┬──────────────────────┬───┬─────────┬───────────┬──────────────────────┐
│          id          │       geometry       │ … │  type   │ zoomlevel │       quadkey        │
│       varchar        │       geometry       │   │ varchar │   int32   │       varchar        │
├──────────────────────┼──────────────────────┼───┼─────────┼───────────┼──────────────────────┤
│ 5e1da825-ef9b-45dd…  │ POINT (122.309211 …  │ … │ place   │         0 │ 130321321133100110…  │
│ 8eb4aa8c-81fb-4a9b…  │ POINT (122.2922402…  │ … │ place   │         0 │ 130323323113231010…  │
│ bcbc7afb-55ab-4614…  │ POINT (123.873895 …  │ … │ place   │         0 │ 130330222223231232…  │
│ b1f4848b-9662-4690…  │ POINT (126.288473 …  │ … │ place   │         0 │ 130330233120333022…  │
│ 9e72a270-b2c4-4fa0…  │ POINT (128.76348 4…  │ … │ place   │         0 │ 130330331203002232…  │
│ 041af844-b7d1-431a…  │ POINT (131.86278 4…  │ … │ place   │         0 │ 130331233100212033…  │
│ 1610195b-60c7-4e64…  │ POINT (133.9324379…  │ … │ place   │         0 │ 130331332231132130…  │
│ a8a3a395-e059-4bcb…  │ POINT (124.6211111…  │ … │ place   │         0 │ 130332021221133210…  │
│ 486df792-8613-4d45…  │ POINT (128.58908 4…  │ … │ place   │         0 │ 130332130331020231…  │
│ 633b120a-4a12-4719…  │ POINT (125.0461 41…  │ … │ place   │         0 │ 130332223310103331…  │
│ 87a6cd83-87b9-4290…  │ POINT (128.6755512…  │ … │ place   │         0 │ 130332333200002230…  │
│ 0a44b995-d7d1-46b7…  │ POINT (128.8258357…  │ … │ place   │         0 │ 130332333203310220…  │
│ 55a1aedc-4636-45cb…  │ POINT (130.95233 4…  │ … │ place   │         0 │ 130333032023111100…  │
│ 2304c1ee-c5be-4316…  │ POINT (132.2290564…  │ … │ place   │         0 │ 130333120222031312…  │
│ 16553fd2-ced3-42ca…  │ POINT (141.182556 …  │ … │ place   │         0 │ 131221222332030323…  │
│ 084fafe9-995c-4442…  │ POINT (141.289157 …  │ … │ place   │         0 │ 131221222333300033…  │
│ b0e52fe2-81e4-43b3…  │ POINT (136.3333333…  │ … │ place   │         0 │ 131222001111223012…  │
│ 99aea4bb-0ce8-457d…  │ POINT (135.2861111…  │ … │ place   │         0 │ 131222020213032020…  │
│ 70d00889-6e9f-4ced…  │ POINT (138.6965131…  │ … │ place   │         0 │ 131222321010222033…  │
│ 2ad92be8-f3f7-4252…  │ POINT (140.6376992…  │ … │ place   │         0 │ 131223022200203023…  │
│          ·           │          ·           │ · │   ·     │         · │          ·           │
│          ·           │          ·           │ · │   ·     │         · │          ·           │
│          ·           │          ·           │ · │   ·     │         · │          ·           │
│ 34cb0782-de88-4a38…  │ POINT (142.1916148…  │ … │ place   │        16 │ 133021232223310122…  │
│ fa47e5bb-fb82-4538…  │ POINT (142.1916161…  │ … │ place   │        16 │ 133021232223310122…  │
│ 5463f9f8-e5b8-4f91…  │ POINT (142.1944501…  │ … │ place   │        16 │ 133021232223310130…  │
│ 659a8a01-bc4e-413f…  │ POINT (142.194466 …  │ … │ place   │        16 │ 133021232223310130…  │
│ b265751b-dfe2-48d0…  │ POINT (142.19446 2…  │ … │ place   │        16 │ 133021232223310130…  │
│ a4248cd0-ce3c-43cd…  │ POINT (142.194462 …  │ … │ place   │        16 │ 133021232223310130…  │
│ 6c738f13-2fe4-4fcf…  │ POINT (142.1941014…  │ … │ place   │        16 │ 133021232223310130…  │
│ fcdc9609-0736-42b0…  │ POINT (142.1955894…  │ … │ place   │        16 │ 133021232223310131…  │
│ a8d9e490-b694-4395…  │ POINT (142.1949624…  │ … │ place   │        16 │ 133021232223310131…  │
│ 9df90671-893f-4eef…  │ POINT (142.2133026…  │ … │ place   │        16 │ 133021232232220102…  │
│ d4d27ec8-66eb-462c…  │ POINT (142.2133 27…  │ … │ place   │        16 │ 133021232232220102…  │
│ 2e6fe019-9d7f-44e9…  │ POINT (142.2134482…  │ … │ place   │        16 │ 133021232232220102…  │
│ 106e9793-4d86-45a4…  │ POINT (142.2134436…  │ … │ place   │        16 │ 133021232232220102…  │
│ 6a945985-22c8-4878…  │ POINT (142.2134401…  │ … │ place   │        16 │ 133021232232220102…  │
│ 7c9bdaa3-c9dc-49b4…  │ POINT (142.1574032…  │ … │ place   │        16 │ 133023010203031233…  │
│ 820ac51f-3c10-4632…  │ POINT (142.1603296…  │ … │ place   │        16 │ 133023010203031301…  │
│ a05bdfc6-002f-4674…  │ POINT (142.1603083…  │ … │ place   │        16 │ 133023010203031301…  │
│ 228260e9-34c3-4b50…  │ POINT (142.1603606…  │ … │ place   │        16 │ 133023010203031310…  │
│ 82eb8087-8f15-4e08…  │ POINT (142.1603517…  │ … │ place   │        16 │ 133023010203031310…  │
│ 3fd3ec70-8b17-4115…  │ POINT (142.1603727…  │ … │ place   │        16 │ 133023010203031310…  │
├──────────────────────┴──────────────────────┴───┴─────────┴───────────┴──────────────────────┤
│ 3193408 rows (3.19 million rows, 40 shown)                              21 columns (5 shown) │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

As you can see:

- Sorted by zoomlevel. Then, rows of one zoomlevel are stored sequentially.
- Within each zoomlevel, sorted by quadkey. Then, spatially close features are stored sequentially in each resolution and this is efficient for reading tile-based area of interest.

Then, we can obtain features in area of interest like below:

```sql
-- DuckDB example
SELECT * FROM yosegi
  WHERE zoomlevel = 10
    AND quadkey LIKE '133002110%'
```

### Benefits

- Single Parquet can be used for storing data and streaming.
- We can get overview of entire data quickly much faster than by only ordinary spatial sort like quadkey or Hilbert curve.
- Thanks to very efficient pushdown filtering by zoomlevel and quadkey, we can read partial content of large Parquet file quickly.

### How calculate zoomlevel?

Density based clustering approach is used. Generally, at lower zoom we don't need to show all features. By this approach, only essential features for visualization are kept at lower zoomlevels. Repeating this process from minzoom to maxzoom, we can get which features are visible at each zoomlevel.

### Why quadkey?

- Hilbert curve is great for spatial locality but hierarchical query with tile index is not efficient. Querying quadkey with `LIKE` is more efficient for tile-based filtering.
- Index generated by `ST_Hilbert` function of DuckDB is not consistent, values depends on bbox ([detail](https://duckdb.org/docs/stable/core_extensions/spatial/functions#description-57)).

### Why not Tile?

- Since creating a tileset exclusively for streaming is painful, it is better to support streaming directly from one original file.
- Contents of lower zoom tiles are wasted when higher zoom levels are shown. Then same feature repeatedly appears in larger zoom levels.

## Refenrences

- <https://medium.com/radiant-earth-insights/using-duckdbs-hilbert-function-with-geop-8ebc9137fb8a>
- <https://medium.com/radiant-earth-insights/the-admin-partitioned-geoparquet-distribution-59f0ca1c6d96>
- <https://github.com/felt/tippecanoe>
