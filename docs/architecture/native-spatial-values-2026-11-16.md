<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native spatial values and bounded planar operations

Status: native implementation, complete source checks and installed-package
acceptance passed under the existing item-19 rich-data contract. The finite
delivery adds 15 shared expressions over two-dimensional Point, LineString and
Polygon values. Hosted integration is separate; this is functional acceptance,
with no new benchmark or performance claim. Resource-admitted deep traversal
and combined rich-data/analytical acceptance remain the next obligations.

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

Provider classification: `implement_shardloom_kernel`, using the existing
Vortex 0.93 extension, array, selection and file providers. The independently
implemented predicates reuse ShardLoom's existing exact binary64 accumulator;
no geometry dependency or implementation code is imported. The
[public contract](../reference/spatial-values.md) lists the 15 functions and
their admitted representations, metadata, arithmetic and topology semantics.

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

## Local acceptance

All 13 focused native tests pass, including exact orientation under extreme
coordinate magnitudes. Six public native tests pass all 15 functions through
five interfaces, fresh-process Vortex reopen after the original input is
removed, independent Arrow/Parquet reads, masking, native spill, cancellation
and last-owner release. Five Python schema tests pass. Coverage includes the
correction of Python's accidental acceptance of JSON `null` metadata and its
unhelpful errors for nonstring CRS/edge declarations.

| Check | Result |
| --- | --- |
| Default workspace tests | 3,344 passed; zero failures or ignores |
| Native workspace tests | 7,346 passed; zero failures; the same 42 ignored cases as the accepted predecessor |
| Formatting and strict lint | Default and native workspace checks passed |
| Feature and compiler checks | All features, no default features, and actual Rust 1.96 minimal/native checks passed |
| Function catalog and references | Five catalog tests and all three generated-reference checks passed |
| Python source suite | 1,329 tests run; zero failures/errors; 83 optional-environment skips retained |
| Fresh release and installed package | All 462 tests passed with zero failures, errors or skips; isolated load and uninstall verification passed |

The fresh CLI and native library bind to clean source commit
`db151f11ed7be9334822a3cd9a81cf7873e90490`. All 13 source/reference gates,
the release build and installed proof saw the same source-file inventory. The
[compact acceptance record](../release/evidence/native-spatial-values-2026-11-16/acceptance.json)
identifies the retained runtime, wheel, commands, original failed/stopped checks
and subsequent documentation-only changes. Raw receipts and logs remain in the
local task directory; no additional source or runtime archive was created.

Early compile/lint failures and the original Python test failures remain
recorded. Three native-test attempts stopped at storage admission or during
linking, before executing the suite. Verified retirement of completed artifacts
provided headroom; the final full run passed with the same storage floor,
deadlines and test selection. No package publication is part of this delivery.

PR [#1697](https://github.com/depsilon/shardloom/pull/1697) merged as
`6688dd9b996b443da276e01b4290e1c18c127438` after all 57 required PR checks
passed and review was clear. [Exact-main CI](https://github.com/depsilon/shardloom/actions/runs/44020827064)
and all 54 required main checks subsequently passed. Cloudflare Workers retains
the existing maintainer exception at both boundaries.

The representation follows the [GeoArrow 0.2 format](https://geoarrow.org/format)
and [extension metadata](https://geoarrow.org/extension-types). The existing
[columnar geometry design reference](https://spiraldb.com/deep-dives/geometry)
motivates coordinate/offset execution; its performance claims are not ShardLoom
results, and no implementation code is copied.
