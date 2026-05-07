# RFC 0031: Universal Native I/O Envelope

## Summary
This RFC defines a ShardLoom-native universal I/O contract for planning and execution boundaries. Universal I/O does not mean universal decoded Arrow batches. Universal I/O means ShardLoom-native work envelopes preserving physical information, encoded representation, statistics, selection vectors, materialization state, pushdown proof, and sink requirements. No implementation is added in this PR.

## Motivation
ShardLoom needs a portable, deterministic contract for source-to-sink data flow that preserves encoded semantics and avoids hidden decode/materialization pressure.

## Goals
- Define canonical envelope contracts for native work and results.
- Preserve encoded representation and materialization state across boundaries.
- Keep Vortex as the highest-fidelity persistence target.
- Allow foreign encoded representations to be preserved when possible.
- Keep diagnostics deterministic and machine-readable.

## Non-goals
- No runtime implementation in this PR.
- No new dependencies.
- No fallback/delegation execution.

## Core concept
Universal I/O is a native contract layer, not a decoded-batch normalization step.

## Contract vocabulary
- `NativeWorkEnvelope`
- `NativeWorkStream`
- `NativeResultStream`
- `RepresentationState`
- `SourceCapabilityReport`
- `SourcePushdownReport`
- `SinkRequirementReport`
- `AdapterFidelityReport`
- `MaterializationBoundaryReport`
- `NativeIoCertificate`

## RepresentationState
`RepresentationState` values:
- `metadata_only`
- `pruned`
- `vortex_encoded`
- `foreign_encoded`
- `selection_vector_encoded`
- `partially_decoded`
- `decoded_columnar`
- `materialized_rows`
- `external_effect`
- `unsupported`

## Source capability and pushdown
`SourceCapabilityReport` and `SourcePushdownReport` capture declared source support, accepted predicates/projections, and proof of applied pushdown without implicit execution delegation.

## Sink requirements
`SinkRequirementReport` captures fidelity constraints, required representation state, and commit/materialization expectations.

## Materialization boundary
`MaterializationBoundaryReport` records where state transitions occur, why transitions are required, and what fidelity is retained or lost.

## Native I/O certificate
`NativeIoCertificate` records source capability, pushdown, representation transitions, sink constraints, and adapter fidelity evidence for reproducibility and auditability.

## Adapter fidelity
`AdapterFidelityReport` records whether native or foreign encoded forms are preserved, where decode occurs, and any metadata loss. Vortex remains the highest-fidelity persistence target.

## Relationship to RFC 0013
This RFC complements RFC 0013 by formalizing I/O envelope contracts that support streaming and zero-decode priorities.

## Relationship to CG-19
This RFC defines the contract foundation for CG-19 (Universal Native I/O Envelope).

## No-fallback and no-delegation policy
Universal I/O contracts must never imply execution fallback. Unsupported paths fail explicitly with deterministic diagnostics.

## Future implementation phases
Future phases may implement these contracts incrementally in planner diagnostics, explain/estimate outputs, adapter interfaces, and execution certificates.
