# Best-Default Certification Gate

GAR-0032-E closes the report-only gate for best-default language. The gate exists so ShardLoom can
name exactly what evidence is missing before any user-facing "best default", performance,
superiority, replacement, or production language is allowed.

This document is a blocker contract, not a claim. Current status:

- `schema_version=shardloom.best_default_certification_gate.v1`
- `report_id=gar-0032-e.best_default_certification_gate`
- `support_status=blocked`
- `claim_gate_status=not_claim_grade`
- `best_default_language_allowed=false`
- `best_default_claim_allowed=false`
- `performance_claim_allowed=false`
- `superiority_claim_allowed=false`
- `spark_replacement_claim_allowed=false`
- `production_claim_allowed=false`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Required Evidence

Best-default language requires all of these evidence categories for a declared workload
constitution:

- `workload_constitution`
- `correctness_evidence`
- `benchmark_evidence`
- `execution_certificate`
- `native_io_certificate`
- `materialization_decode`
- `no_fallback_policy`
- `release_security`
- `ux_install_docs`
- `capability_snapshot`
- `best_choice_scorecard`
- `best_default_dossier`

Until every category has attached refs, the gate must emit:

```text
claim_gate_status=not_claim_grade
best_default_language_allowed=false
```

## CLI Surfaces

The report-only gate is emitted through:

```powershell
shardloom world-class-sufficiency-plan --format json
shardloom capabilities certification --format json
```

The fields are prefixed with `best_default_certification_gate_` except the plain-language
publication blocker:

```text
best_default_language_allowed=false
```

## Claim Boundary

The current gate allows only this narrow statement:

```text
ShardLoom has a deterministic best-default certification gate that blocks best-default language
until workload-scoped correctness, benchmark, certificate, Native I/O, materialization/decode,
policy, release, and UX evidence are attached.
```

It does not allow:

- no best-default claim
- no performance claim
- no superiority claim
- no Spark replacement claim
- no production SQL/DataFrame claim
- no production object-store/lakehouse/Foundry claim
- no package-publication claim

## Fallback Boundary

The gate is side-effect-free. It does not parse SQL, read data, inspect files, probe adapters,
resolve credentials, open networks, execute workloads, invoke external engines, or attempt
fallback execution.

Any future claim-grade promotion must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```
