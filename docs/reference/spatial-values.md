<!-- SPDX-License-Identifier: Apache-2.0 -->

# Spatial values and planar expressions

This capability is included in the published 1.3.0 CLI and native Linux/macOS Python
wheels, as well as matching source builds (`1.1.0` and earlier wheels do not include it).

Declare two-dimensional Point, LineString and Polygon columns with
`types.point()`, `types.linestring()` and `types.polygon()`. Values use separated
Float64 `x` and `y` coordinates. Lines contain coordinate lists; polygons contain
lists of closed rings, with the exterior ring first and holes afterward.
These are the `geoarrow.point`, `geoarrow.linestring` and `geoarrow.polygon`
identities from [GeoArrow 0.2](https://geoarrow.org/format).

```python
import shardloom as sl
from shardloom import functions as f, types as t

with sl.context(transport="native", memory_bytes=128 << 20,
                max_parallelism=1) as context:
    places = context.from_rows(
        [{"id": 1, "place": {"x": 3.0, "y": 4.0}},
         {"id": 2, "place": None}],
        schema={"id": t.int64(), "place": t.point()},
    )
    selected = places.select(
        "id", "place",
        f.st_distance("place", f.st_point(0, 0)).alias("distance"),
    )
    assert selected.collect(check=True).to_rows() == (
        {"id": 1, "place": {"x": 3.0, "y": 4.0}, "distance": 5.0},
        {"id": 2, "place": None, "distance": None},
    )
    selected.write_vortex("places.vortex", check=True)
```

SQL uses the same functions, for example `ST_DISTANCE(place, ST_POINT(0, 0))`.
Python column arguments can be column names or expressions. Numeric constructor
arguments and the radius can also be literals. SQL, DataFrame, typed native
plans, embedded workflows and CLI lower into the same native kernels.

| Functions | Contract |
| --- | --- |
| `ST_POINT(x, y)`, `ST_EMPTYPOINT()` | Construct a Point or a nonnull empty Point. Integer conversion must be exact. Either null coordinate makes a null Point. |
| `ST_X(g)`, `ST_Y(g)` | Point coordinates; null and empty Points return null. |
| `ST_ISEMPTY(g)` | Geometric emptiness, distinct from SQL null. |
| `ST_NPOINTS(g)` | UInt64 count of stored vertices, including ring closing vertices. Empty geometries have zero vertices. |
| `ST_XMIN(g)`, `ST_YMIN(g)`, `ST_XMAX(g)`, `ST_YMAX(g)` | Planar coordinate bounds. Empty geometries return null. |
| `ST_DISTANCE(a, b)` | Point distance, rounded once from the exact Cartesian squared distance to nearest-even Float64. Empty operands return null. |
| `ST_DWITHIN(a, b, radius)` | Exact squared-distance comparison against a finite nonnegative radius; integer conversion must be exact. Empty operands return false. |
| `ST_LENGTH(line)` | Sum of individually rounded segment distances using an exact accumulator. An empty line has length zero. |
| `ST_CONTAINS(polygon, point)` | True strictly inside the exterior and outside all holes. Boundary points and empty geometries return false. |
| `ST_COVERS(polygon, point)` | Also includes exterior and hole boundaries. Empty geometries return false. |

Other than the constructor and empty rules above, null inputs propagate to null
outputs. Arithmetic overflow follows the context's strict/extended float policy.
Containment validates simple, nonzero-area rings, disjoint boundaries and holes
strictly inside the exterior before applying bounds. Overlapping edges,
self-intersections, touching or nested holes fail explicitly. Coordinate
inspection and payload carriage require structurally valid geometry but do not
claim topological validity.

## Nulls, empty values and metadata

An empty line or polygon is `[]`. An empty Point has two NaN coordinates in
native/Arrow storage; create it with `ST_EMPTYPOINT()`. One NaN coordinate,
infinite coordinates, null coordinates inside a present geometry, one-vertex
lines and unclosed polygon rings are rejected. Null ancestors mask their child
values. Explicit JSON input permits finite coordinates only, so use the native
constructor or Arrow for empty Points. Native `to_rows()` and `to_arrow()`
preserve them; diagnostic JSON cannot represent the NaN sentinel.

The optional `metadata` argument preserves a CRS object/string, its optional
`crs_type`, and an optional geographic `edges` declaration following
[GeoArrow metadata](https://geoarrow.org/extension-types). Omitted edges mean
planar interpolation. Unknown metadata fields and duplicate JSON keys fail.
Distance, bounds, length and containment require planar edges; binary operations
also require exactly matching CRS/edge metadata. Coordinate inspection, vertex
counts, emptiness and carriage may preserve geographic metadata. No implicit
reprojection or geographic distance calculation occurs.

## Persistence and execution boundaries

Native Vortex output retains the logical geometry and coordinate storage.
Arrow IPC and Parquet preserve GeoArrow field metadata through their Arrow
schema boundary; this does not claim GeoParquet table metadata support. Native
readers restore geometry identities. Ordinary keys can filter, join and order
rows while geometry payloads survive selection, streaming and explicit native
spill. Retained results own their buffers after their source/context closes.

Execution uses resource-admitted coordinate/offset views and native scalar
registers. Encoded coordinate providers may execute selected children; this is
not a blanket zero-decode claim. No external geometry or query engine executes
the operations. Memory refusal and cancellation retain the existing cleanup
and no-fallback contract.

Interleaved coordinates, Z/M dimensions, multi-geometries, WKB/WKT parsing,
reprojection, geographic kernels, geometry keys, persisted spatial summaries
and spatial indexes require separate admission. Unsupported paths fail
explicitly. Functional support makes no performance or broad GIS-parity claim.
