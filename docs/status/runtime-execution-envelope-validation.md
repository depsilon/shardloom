# Runtime Execution Envelope Validation

schema_version: `shardloom.runtime_execution_envelope_validation_status.v1`

Owning phase item: `GAR-RUNTIME-IMPL-4K`

The runtime-envelope validator is a production-readiness guard for executable surfaces. It does not
grant a runtime capability by itself; it rejects runtime evidence that is missing the fields needed
to distinguish execution from report-only status.

Required scalar fields:

- `fallback_attempted`
- `external_engine_invoked`
- `claim_gate_status`

The no-fallback booleans must parse as literal `true` or `false`; malformed values block the
runtime claim instead of being interpreted leniently.

Required runtime evidence groups:

- `route_state_ref`: one of SourceState, VortexPreparedState, OutputPlan, generated-source plan, or
  plan/artifact digest fields.
- `materialization_or_decode_evidence`: one of the explicit materialization, decode, runtime
  consumption, or representation-transition fields.
- `execution_certificate`: `execution_certificate_ref`, `execution_certificate_refs`, or a typed
  `execution_certificate` entry.

Mode-specific rules:

- `prepared_vortex` requires `prepared_state_id` and `prepared_state_digest`.
- `compatibility_import_certified` requires `timing_scope=cold_certified_end_to_end`.
- `compatibility_import_certified` requires `preparation_included=true`.

Validation command:

```powershell
python scripts\check_runtime_execution_envelopes.py
```

Claim boundary: this validator standardizes evidence and blocks overclaiming. It does not make a
performance, production, package, object-store, lakehouse, SQL/DataFrame, or Spark-replacement
claim. Every validated envelope must preserve `fallback_attempted=false` and
`external_engine_invoked=false`.
