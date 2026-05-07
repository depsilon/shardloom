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

## Field-level contract sketches

### NativeWorkEnvelope
Required fields:
- `envelope_id`: stable unique identifier.
- `source_ref`: source identity and scope.
- `schema_ref`: schema identity or digest.
- `representation_state`: current `RepresentationState`.
- `statistics_ref`: statistics handle used for pruning/proofs.
- `selection_vector_ref`: optional encoded selection handle.
- `pushdown_proof_ref`: proof object or diagnostic reference.
- `materialization_boundary_ref`: optional last/next boundary reference.
- `ordering`: declared ordering contract.
- `partitioning`: declared partitioning contract.
- `semantic_profile`: applicable semantic compatibility profile.
- `diagnostics`: stable machine-readable diagnostics.
- `fallback_attempted=false`: explicit no-fallback invariant.

### NativeWorkStream
Required fields:
- `stream_id`: stable stream identifier.
- `source_capability_report`: embedded or referenced `SourceCapabilityReport`.
- `envelopes`: ordered set of `NativeWorkEnvelope` units.
- `backpressure_policy`: bounded-memory/backpressure contract.
- `streaming_mode`: batch, micro-batch, or record-stream mode declaration.
- `task_granularity_policy`: partition/chunk/task sizing policy.
- `diagnostics`: stream-level diagnostics.

### NativeResultStream
Required fields:
- `stream_id`: stable result stream identifier.
- `sink_requirement_report`: sink constraints and required representation.
- `result_envelopes`: output `NativeWorkEnvelope`-compatible payload units.
- `materialization_boundary_report`: one or more `MaterializationBoundaryReport` entries.
- `native_io_certificate`: final `NativeIoCertificate` for run/report scope.
- `diagnostics`: stable output diagnostics.

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

### RepresentationState semantics and transitions

#### metadata_only
- Meaning: answerability/proof boundary from metadata only.
- Allowed transitions: `pruned`, `unsupported`.
- Forbidden assumptions: no data bytes imply no semantic loss.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.

#### pruned
- Meaning: work eliminated by statistics/metadata proof.
- Allowed transitions: terminal, or `unsupported` if proof invalidated.
- Forbidden assumptions: do not assume row ordering or emitted rows.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.

#### vortex_encoded
- Meaning: Vortex-native encoded representation preserved.
- Allowed transitions: `selection_vector_encoded`, `partially_decoded`, `unsupported`.
- Forbidden assumptions: do not assume full decode is required.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.

#### foreign_encoded
- Meaning: non-Vortex encoded representation preserved through adapter boundary.
- Allowed transitions: `partially_decoded`, `selection_vector_encoded`, `unsupported`.
- Forbidden assumptions: do not assume compatibility with all native kernels.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.

#### selection_vector_encoded
- Meaning: encoded payload with active selection/projection vector state.
- Allowed transitions: `partially_decoded`, `decoded_columnar`, `unsupported`.
- Forbidden assumptions: selection is not equivalent to materialized filtering.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.

#### partially_decoded
- Meaning: partial decode/materialization for required columns or operators.
- Allowed transitions: `decoded_columnar`, `materialized_rows`, `unsupported`.
- Forbidden assumptions: partially decoded data is not fully normalized.
- Implies decode: yes (partial).
- Implies row materialization: no.
- Can remain encoded: partially.

#### decoded_columnar
- Meaning: decoded columnar representation.
- Allowed transitions: `materialized_rows`, `unsupported`.
- Forbidden assumptions: decoded columnar is not row materialization.
- Implies decode: yes.
- Implies row materialization: no.
- Can remain encoded: no.

#### materialized_rows
- Meaning: row-oriented materialization boundary crossed.
- Allowed transitions: terminal or `unsupported`.
- Forbidden assumptions: row materialization must not be implicit/default.
- Implies decode: yes.
- Implies row materialization: yes.
- Can remain encoded: no.

#### external_effect
- Meaning: boundary involving external side effects or effectful adapters.
- Allowed transitions: any supported state based on contract.
- Forbidden assumptions: effect completion does not imply commit success.
- Implies decode: not necessarily.
- Implies row materialization: not necessarily.
- Can remain encoded: possibly.

#### unsupported
- Meaning: capability proof failed or required contract unsupported.
- Allowed transitions: terminal.
- Forbidden assumptions: no fallback/delegation allowed.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: N/A.

### Explicit transition examples
- `metadata_only -> pruned`
- `vortex_encoded -> selection_vector_encoded`
- `foreign_encoded -> partially_decoded`
- `partially_decoded -> decoded_columnar`
- `decoded_columnar -> materialized_rows`
- `any state -> unsupported` when capability proof fails

## Source capability and pushdown

### SourceCapabilityReport
Required fields:
- `source_kind`
- `adapter_id`
- `schema_discovery_status`
- `statistics_availability`
- `pushdown_capabilities`
- `encoded_representation_preserved`
- `range_read_capability`
- `streaming_capability`
- `object_store_capability`
- `fallback_attempted=false`

### SourcePushdownReport
Required fields:
- `accepted_operations`
- `rejected_operations`
- `guarantee`
- `proof_basis`
- `residual_expression`
- `conservative_false_positive_policy`
- `unsafe_rejected_reason`
- `fallback_attempted=false`

## Sink requirements

### SinkRequirementReport
Required fields:
- `target_format`
- `accepts_encoded`
- `requires_decoded_columnar`
- `requires_rows`
- `preserves_metadata`
- `requires_ordering`
- `requires_partitioning`
- `requires_commit`
- `supports_streaming`
- `max_chunk_size`
- `backpressure_policy`

## Adapter fidelity

### AdapterFidelityReport
Required fields:
- `adapter_id`
- `source_kind`
- `sink_kind`
- `metadata_preserved`
- `statistics_preserved`
- `encoded_representation_preserved`
- `materialization_required`
- `fidelity_loss`
- `metadata_loss`
- `fallback_attempted=false`

## Materialization boundary

### MaterializationBoundaryReport
Required fields:
- `boundary_id`
- `from_state`
- `to_state`
- `required_by`
- `reason`
- `bytes_decoded`
- `rows_materialized`
- `fidelity_loss`
- `fallback_attempted=false`

## Native I/O certificate

### NativeIoCertificate
Required fields:
- `certificate_id`
- `source_capability_report`
- `source_pushdown_report`
- `representation_transitions`
- `sink_requirement_report`
- `adapter_fidelity_report`
- `materialization_boundaries`
- `side_effects`
- `diagnostics`
- `fallback_attempted=false`

## Acceptance criteria
- CG-19 cannot complete until every source/sink path emits a `NativeIoCertificate`.
- Universal I/O must preserve `vortex_encoded` or `foreign_encoded` state whenever possible.
- Universal I/O must not silently normalize to decoded Arrow.
- All transitions to `decoded_columnar` or `materialized_rows` must include a `MaterializationBoundaryReport`.

## Relationship to RFC 0013
This RFC complements RFC 0013 by formalizing I/O envelope contracts that support streaming and zero-decode priorities.

## Relationship to CG-19
This RFC defines the contract foundation for CG-19 (Universal Native I/O Envelope).

## No-fallback and no-delegation policy
Universal I/O contracts must never imply execution fallback. Unsupported paths fail explicitly with deterministic diagnostics.

## Future implementation phases
Future phases may implement these contracts incrementally in planner diagnostics, explain/estimate outputs, adapter interfaces, and execution certificates.
