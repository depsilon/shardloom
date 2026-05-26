# UDF And External-Effect Blocker Matrix

`GAR-0032-C` records ShardLoom's current UDF and external-effect posture. UDFs, API calls, LLM
calls, embedding generation, vector search, plugin execution, media extraction, and network egress
are visible as deterministic capability rows, but every effectful path is denied by default.

`udf-local-scalar-fixture-smoke` is the only admitted UDF-like runtime fixture. It is built into
ShardLoom, pure deterministic, nullable-int64 only, and recorded by
`docs/architecture/effectful-operation-admission-matrix.md`; it is not a plugin, Python/WASM/Rust
extension, SQL-defined UDF, table function, or external-service UDF.

## User Surfaces

- `effect-budget-plan --format json`
- `capabilities udfs --format json`
- `capabilities event-api-saas-adapters --format json`
- `capabilities unstructured-media --format json`
- `capabilities extensions --format json`
- `capabilities security-governance --format json`
- `udf-local-scalar-fixture-smoke <values> --format json`

## Contract Fields

The shared blocker matrix uses schema
`shardloom.external_effect_blocker_matrix.v1` and matrix id
`gar-0032-c.udf_external_effect_blockers`.

Every row exposes:

- `support_status=blocked`
- `permission_status=policy_required`
- `effect_status=denied_by_default`
- `blocker_id`
- `diagnostic_code=SL_BLOCKED_EXTERNAL_EFFECT`
- `required_evidence`
- `credential_required`
- `network_required`
- `sandbox_required`
- `model_or_embedding_call`
- `data_egress_possible`
- `materialization_boundary_required`
- `runtime_execution=false`
- `effect_executed=false`
- `claim_boundary`

The matrix summary preserves:

- `external_effect_blocker_claim_gate_status=not_claim_grade`
- `external_effect_blocker_all_effects_blocked=true`
- `external_effect_blocker_runtime_execution=false`
- `external_effect_blocker_credential_resolution_performed=false`
- `external_effect_blocker_network_probe_performed=false`
- `external_effect_blocker_fallback_attempted=false`
- `external_effect_blocker_external_engine_invoked=false`

## Covered Families

- SQL-defined, Rust-native, WASM, Python, and external-service UDFs
- API calls
- LLM calls
- embedding generation
- vector search
- plugin execution
- media extraction
- network egress

## Claim Boundary

This is a diagnostic/report-only blocker matrix. It adds no UDF registry, SQL UDF parser, UDF
runtime, plugin loader, WASM runtime, Python UDF execution, API client, LLM client, embedding model,
vector index, media parser, credential resolution, network call, model invocation, external service
call, data egress, external engine invocation, fallback execution, or hidden fallback execution.

In short: no fallback execution is available through UDF, API, LLM, embedding, vector, plugin,
media, or network-effect rows.

Future slices may only promote a row when they attach policy, credential, sandbox, redaction, audit,
materialization/decode, correctness, execution-certificate, Native I/O where applicable,
effect-budget, and no-fallback evidence.
