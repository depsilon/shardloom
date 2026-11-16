<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native spatial values and bounded planar operations

Status: implementation in progress under the existing item-19 rich-data
contract. The accepted dense integer join change is in PR #1695. The next
full-sort proposal has no saved proof of actual disordered full-sort cost, so
it does not justify another optimization experiment at this point. Continue
the required spatial feature roadmap; do not treat this as a performance claim.

The first delivery preserves two-dimensional Point, LineString and Polygon
values through the existing native input, selection, relational payload, spill,
result and Vortex persistence owners. Use separated Float64 `x`/`y` children
with native lists for vertex/ring coordinates. GeoArrow field identity and CRS/
edge metadata must survive compatible Arrow/Parquet boundaries. Unsupported
representations and operations must fail explicitly rather than lose metadata.

The provider check finds no spatial dtype or geometry kernel in the pinned
Vortex 0.93 array provider or the enabled dependency graph. Vortex already
provides the needed extension registry, editions, structs, lists, numeric arrays,
validity, selected providers and Arrow import/export hooks. Wrap those concepts
inside `shardloom-vortex`, following the existing tensor/vector extension owners;
do not introduce another storage system, geometry-object execution substrate or
external query engine. Any exact residual kernel remains ShardLoom-native.

Preserve geometry-level nulls separately from empty geometries. Child-coordinate
nullability, nonfinite values, polygon closure, unsupported dimensions, metadata
identity and geographic versus planar interpretation require explicit admission.
Do not silently apply Cartesian distance or straight-edge operations to an
unsupported declared edge model. CRS metadata is identity, not permission to
reproject coordinates. Exact operation scope will be recorded with implementation
and tests before public capability claims.

Functions must lower through the existing shared expression and relational
owners so SQL, DataFrame, typed plans, CLI and embedded workflows agree. Use
borrowed coordinate/offset views and bounded scalar registers; conservative
bounds may reject impossible candidates only after their semantic prerequisites
are established. Surviving candidates still receive the exact admitted operation.
Persisted spatial summaries and a spatial index are separate future work.

Acceptance requires complete read–compute–write–reopen value/metadata fidelity,
selection/parent-validity and error-order controls, resource/cancellation/refund
tests, default/native checks and installed public-interface tests. Functional
support does not require another Full43, full11k or competitor timing pass.
Keep evidence concise and retain original failures; no optional archive pipeline
is part of this delivery. Item 19, item 20 and CG-1 through CG-23 remain open.

The representation follows the [GeoArrow 0.2 format](https://geoarrow.org/format)
and [extension metadata](https://geoarrow.org/extension-types). The existing
[columnar geometry design reference](https://spiraldb.com/deep-dives/geometry)
motivates coordinate/offset execution; its performance claims are not ShardLoom
results, and no implementation code is copied.
