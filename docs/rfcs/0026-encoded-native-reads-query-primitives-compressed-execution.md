# RFC 0026 — Encoded-Native Reads, Query Primitives, and Compressed Execution

## Scope

This RFC defines implementation contracts for:
- CG-1 real encoded read path.
- CG-2 real query primitive execution over Vortex data.
- CG-13 encoded-native compressed execution.

## Encoded read API boundary

- A strict encoded-read API boundary must isolate upstream Vortex read contact points.
- The boundary must be feature-gated and local-first for initial implementation.
- API classification must explicitly block unsafe decode/materialization-default paths.

## Feature-gated local encoded read fixture

- Initial real encoded-read implementation must start with feature-gated local fixtures.
- Object-store IO is explicitly deferred.
- Fixtures must prove direct encoded segment/chunk path behavior.

## Execution posture requirements

- No broad row materialization.
- No Arrow-default conversion.
- Execute count/filter/project primitives over encoded segments directly when possible.
- Decode only under explicit, diagnosable policy boundaries.

## Query primitives for CG-2

- actual count.
- actual filtered count.
- actual projection.
- actual predicate/filter primitive.

Predicate behavior must include:
- dictionary-aware predicates.
- RLE/run-end-aware predicates.
- bitpack/bytebool-aware predicates.
- FSST/string-encoded predicate path.
- ALP/fastlanes numeric path.

## Encoded-native compressed execution for CG-13

- Direct count/filter/project over encoded segments.
- Execution path selection based on encoding-aware capabilities.
- Decode-avoided proof/report as a first-class output artifact.

### CG-13.1 encoded path selection report

`VortexEncodedExecutionPathSelectionReport` is the initial CG-13 report-only
contract. It composes existing physical operator profile, encoded count,
encoded predicate, selection-vector filter, and encoded projection evidence into
one path-selection artifact.

Required fields:
- schema/report identity.
- source physical operator profile matrix.
- status.
- per-operator entries for count aggregate, filter, and project.
- selected execution level.
- required kernel kinds.
- evidence sources.
- metadata-only, encoded-native, hybrid-native, and native-decoded candidate
  flags.
- decode avoided and materialization avoided flags.
- selection-vector preservation status.
- correctness, memory-safety, and benchmark evidence requirements.
- side-effect fields for data read, decode, materialization, row read, Arrow
  conversion, object-store IO, write IO, spill IO, runtime execution, external
  engine execution, fallback allowance, fallback attempt, and production claim
  allowance.

Acceptance boundaries:
- The report may select encoded-native count/filter/project candidates only as
  planning evidence.
- The report must emit `fallback_attempted=false` and
  `fallback_execution_allowed=false`.
- The report must not read encoded data, decode arrays, materialize values,
  invoke scans, convert to Arrow, write files, spill, call external engines, or
  claim production readiness.
- Generalized direct encoded count/filter/project execution remains a later
  CG-13 implementation step requiring correctness and benchmark evidence.

## Non-goals

- No object-store IO.
- No fallback engines.
- No real broad query execution beyond explicitly scoped primitives until dedicated CG
  implementation PRs.
- No Arrow-default execution.
