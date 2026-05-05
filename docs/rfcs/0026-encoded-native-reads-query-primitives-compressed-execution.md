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

## Non-goals

- No object-store IO.
- No fallback engines.
- No real broad query execution beyond explicitly scoped primitives until dedicated CG implementation PRs.
- No Arrow-default execution.
