# Runtime Execution Envelope Validation

schema_version: `shardloom.runtime_execution_envelope_validation_status.v1`
validator_schema_version: `shardloom.runtime_execution_envelope_validation.v1`

Owning phase item: `GAR-RUNTIME-IMPL-4K/GAR-RUNTIME-IMPL-5H`

The runtime-envelope validator is a production-readiness guard for executable surfaces. It does not
grant a runtime capability by itself; it rejects runtime evidence that is missing the fields needed
to distinguish execution from report-only status.

`GAR-RUNTIME-IMPL-5R` extends this shared validator to PulseWeave prepared/local rows. If a row
emits `pulseweave_status`, the validator requires the complete PulseWeave, FlowInventory,
ScarcityLedger, EndoPulse, and ProofBound field families before the row can pass.

Validated surfaces:

- Runtime `OutputEnvelope` fixtures and flat field maps through
  `validate_runtime_execution_envelope(...)` and `validate_runtime_execution_fields(...)`.
- Traditional analytics benchmark rows before artifact emission.
- website published benchmark rows in `published_benchmark_rows`.
- The runs-today support matrix projection, where report-only and diagnostic rows cannot
  masquerade as `runtime_execution=true`.
- Release, benchmark-completeness, and website-readiness gates.

Required scalar fields:

- `fallback_attempted`
- `external_engine_invoked`
- `claim_gate_status`

Prepared/native lifecycle aliases such as `prepared_native_vortex_lifecycle_fallback_attempted`,
`prepared_native_vortex_lifecycle_external_engine_invoked`, and
`prepared_native_vortex_lifecycle_claim_gate_status` are accepted for rows whose evidence is scoped
to the prepared/native lifecycle contract.

The no-fallback booleans must parse as literal `true` or `false`; malformed values block the
runtime claim instead of being interpreted leniently.

Required runtime evidence groups:

- `route_state_ref`: one of SourceState, VortexPreparedState, OutputPlan, generated-source plan, or
  plan/artifact digest fields.
- `materialization_or_decode_evidence`: one of the explicit materialization, decode, runtime
  consumption, representation-transition, or prepared/native Vortex lifecycle materialization/decode
  fields.
- `execution_certificate`: a concrete execution certificate id/ref field or a typed
  `execution_certificate` entry. `evidence_level_certificate_refs` alone is evidence-level
  metadata and is not accepted as the runtime execution certificate.

Mode-specific rules:

- `prepared_vortex` requires `prepared_state_id` and `prepared_state_digest`.
- `compatibility_import_certified` requires `timing_scope=cold_certified_end_to_end`.
- `compatibility_import_certified` requires `preparation_included=true`.
- `minimal_runtime` evidence cannot be promoted to `claim_grade`.
- `claim_grade` rows require `claim_grade_requirements_met=true` and a certified execution
  certificate status.
- `certified` and `full_replay` evidence levels require `execution_certificate_status=certified`.
- `full_replay` requires result-sink replay proof and a concrete replay ref.
- Certified `prepared_vortex_scale_split_operator_*` rows require operator family, stateful/shuffle
  flags, retry/source replay, memory-envelope, backpressure, spill-policy, output commit proof,
  concrete split-operator certificate, and split-operator no-fallback fields.
- Applied PulseWeave rows require `pulseweave_runtime_decision_applied=true`,
  `pulseweave_status=applied`, `pulseweave_blocker=none`, FlowInventory WIP evidence,
  ScarcityLedger action/price evidence, `endopulse_persistent_state_used=false`, ProofBound
  admission/certification, `native_io_certificate_status=certified`, certified execution
  certificate status, correctness/output digest evidence, and PulseWeave no-fallback fields.
- `report_only` and `diagnostic_only` rows cannot set `runtime_execution=true`.

Validation command:

```powershell
python scripts\check_runtime_execution_envelopes.py
```

Claim boundary: this validator standardizes evidence and blocks overclaiming. It does not make a
performance, production, package, object-store, lakehouse, SQL/DataFrame, or Spark-replacement
claim. Every validated envelope must preserve `fallback_attempted=false` and
`external_engine_invoked=false`.
