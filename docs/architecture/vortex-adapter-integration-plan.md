# Vortex Adapter Integration Plan

## Purpose

ShardLoom will integrate Vortex through a narrow adapter boundary so core crates stay ShardLoom-domain-first while Vortex-specific integration remains isolated.

## Adapter principles

- Vortex-specific upstream API usage stays in `shardloom-vortex`.
- Core crates use ShardLoom domain types.
- Adapter maps upstream Vortex concepts into ShardLoom concepts.
- Adapter does not execute fallback engines.
- Adapter avoids unnecessary decode.
- Adapter exposes unsupported features as diagnostics.
- Adapter preserves native Vortex output as highest-fidelity.

## Boundary layers

- Vortex dependency boundary.
- Vortex metadata inspection.
- Vortex DType mapping.
- Vortex encoding/layout mapping.
- Vortex statistics mapping.
- Vortex read planning.
- Vortex output planning.
- Future Vortex actual read/write.

## First integration milestone

First future dependency PR should:

- Add upstream Vortex dependency.
- Add version/license notes.
- Implement metadata-only opening if safe.
- Add no actual data reads unless unavoidable.
- Add tests using synthetic/minimal safe fixtures only if available.
- Keep all unsupported features explicit.

## Second integration milestone

- DType mapping.
- Encoding/layout mapping.
- Statistics mapping.
- Segment descriptor population.
- No full decode default.

## Third integration milestone

- Metadata-only scan plan.
- Segment pruning input.
- Explain/estimate integration.
- No execution kernels yet.

## Fourth integration milestone

- Native Vortex output planning.
- Output fidelity mapping.
- Translation report integration.
- No actual write until writer contract is proven.

## Risk areas

- Upstream API instability.
- Over-coupling to internals.
- Accidental decode-to-Arrow default path.
- Metadata loss.
- License/provenance drift.
- Tests requiring external files.
- Object-store assumptions.
- Feature flags increasing dependency footprint.

## Success criteria

- Core code remains Vortex-aware but not upstream-Vortex-coupled.
- `shardloom-vortex` owns upstream dependency.
- Vortex remains native input/output.
- Unsupported features fail clearly.
- No fallback engines introduced.
- Tests prove adapter mappings.

## Adapter report contract

- All adapter reports must render non-empty diagnostics in human text.
- All adapter report `has_errors` methods must be severity-based.
- All adapter reports must keep fallback execution disabled visible.
- All adapter report text builders should avoid `push_str(format!(...))`.
- Public `Result`-returning adapter constructors need `# Errors` docs.
- `ShardLoom` and `DType` should be backticked in Rust doc comments when needed for clippy.

## Metadata summary layer

- Metadata probe results are normalized before scan planning.
- This layer is the future input to pruning/explain/estimate.
- It must remain no-decode and no-materialization.
- Object-store support remains out of scope.
- Vortex output remains highest-fidelity.

## Metadata summary to planning bridge

- `Vortex` metadata summaries now convert into scan/explain/estimate planning reports.
- The bridge remains plan-only and side-effect-free.
- It prepares future segment pruning and richer scan planning integration.
- Actual scan execution remains explicitly out of scope for this stage.

## Metadata pruning bridge

- Metadata planning now feeds conservative pruning decisions.
- `PredicateProof` derivation is conservative.
- Incorrect pruning is a correctness bug.
- Future execution should consume these pruning reports before reading data.
- Actual scan execution remains out of scope.

## Dependency footprint staging update

- Adapter dependency posture now uses feature gates: `upstream-vortex`, `vortex-file-io`, `vortex-object-store`, `vortex-write`.
- Default build keeps upstream Vortex disabled to reduce transitive footprint.
- File IO/object-store IO stay out of scope and disabled by default.
- No fallback engine dependency or behavior is introduced.

## Feature-gated metadata-only open contract

- Metadata-only local file open is the first IO seam for `shardloom-vortex`.
- The seam is explicitly no-scan/no-decode/no-write.
- The contract is designed to feed metadata summary/planning/pruning flows.
- Actual scan execution remains out of scope.
- Object-store and write support remain future feature-gated work.

## Universal input bridge

The `Vortex` adapter now accepts normalized `UniversalInputSource` values.

Native `Vortex` input can flow into metadata open/summary/planning/pruning reports, providing the input-side counterpart to native `Vortex` output fidelity.

Actual scan execution remains out of scope.

## Metadata-driven read planning

- Vortex metadata/pruning reports can now produce read planning reports.
- Read planning creates segment intents and split descriptors only.
- Split descriptors do not execute reads.
- Byte ranges are intentions, not IO.
- Actual scan execution remains out of scope.
- Object-store and write features remain future gates.

## Runtime task graph bridge

`Vortex` read planning reports can now produce `ShardLoom` runtime `TaskGraph` plans.
Segment read intents are mapped into `SegmentTask` skeletons only, and tasks are not executed.
Byte ranges remain read intentions only for future scheduling.
This prepares future scheduling/execution boundaries while keeping actual scan execution out of scope.
Object-store and write capabilities remain future feature gates.

## Adaptive sizing bridge

`Vortex` read planning reports and runtime task-graph bridge reports now feed directly into adaptive sizing planning. Segment read intents, split descriptors, byte-range intentions, and runtime mappings are converted into memory-aware sizing decisions.

Missing estimates are preserved as `NeedsEstimate` decisions rather than guessed byte sizes. Byte-range intents can contribute encoded-byte estimates only when safe to derive from known ranges.

This bridge remains plan-only: no tasks are executed, no data is read, no decode/materialization is performed, and no object-store IO or writes are issued. It prepares future scheduling and memory-aware execution while preserving no-fallback behavior.
