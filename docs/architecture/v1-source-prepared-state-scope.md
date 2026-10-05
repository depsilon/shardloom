<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Source And Prepared-State Scope

Status: declarative contract for the v1 source and prepared-state boundary. Schema marker:
`shardloom.v1_source_prepared_state_scope.v1`.

This scope has one public native route: `native_vortex_query`. It describes where source state may
be reused and what must be revalidated; it does not certify runtime or benchmark readiness. The
golden JSON files are a declarative specification, not runtime evidence.

## Canonical Route And Owner

Every declared input or source-free expression enters the same route:

```text
declared input or source-free expression
  -> native Vortex admission
  -> native_vortex_unified_plan
  -> typed result or declared sink
```

`ResidentVortexSession` owns resident source state. Reuse is limited to that native session or an
explicit Vortex artifact. Before each execution, the runtime validates source generation and the
input declaration; a failed validation requires re-admission or a deterministic error. Reusing
source state never reuses a prior query answer: each request executes its native query and returns
that execution's result. The policy is
`validate_source_generation_and_declaration_before_each_execution` and it does not cache query
answers.

## Input And Preparation Contract

- Memory and native Vortex inputs are direct inputs to the shared native query route; memory and
  native Vortex inputs are direct.
- Compatibility inputs (`csv`, `json`, `jsonl`, `parquet`, `arrow-ipc`, `avro`, and `orc`) normalize
  through Vortex before entering that shared query route. Feature gates still apply to formats
  whose adapters are gated. Compatibility inputs normalize through Vortex before shared execution.
- Source-free expressions use the shared engine and do not require publication or durable output;
  source-free execution does not require publication.
- Durable `vortex-prepare` is optional. A caller may choose an explicit Vortex artifact when
  persistence is useful; a compatibility input does not require a workspace manifest or an
  implicit artifact cache.
- Reuse of an explicit artifact requires validating its existence, identity, source generation,
  and declaration before the query runs.

## Reuse And Invalidation Cases

The matrix covers exactly these cases:

| Case | Required posture |
| --- | --- |
| `first_request` | Admit source state, then execute the native query. |
| `same_source_same_declaration` | Reuse validated resident source state, then execute the native query again. |
| `source_changed` | Invalidate source state and re-admit before query execution. |
| `memory_declaration_changed` | Invalidate state when a memory input's declaration changes. |
| `resource_policy_changed` | Revalidate state under the changed execution resource policy. |
| `missing_artifact` | Do not reuse a missing explicit Vortex artifact. |
| `artifact_changed` | Do not reuse an explicit artifact whose identity has changed. |

No case permits query-answer caching. A reuse hit concerns validated source state only; every
matrix case retains `query_execution=execute_native_query` and `query_answer_cached=false`.

## Explicit Non-Goals

V1 does not admit a `global_hidden_cache`, external cache service, object-store prepared-state
reuse, table/catalog prepared-state reuse, or broad non-local preparation. Unsupported behavior
must fail deterministically without external execution and preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```

The scope makes no performance, production-readiness, Spark-replacement, or broad adapter-support
claim. Such claims require separate runtime and benchmark evidence under their applicable gates.

## Validation And Runtime Evidence

`scripts/check_v1_source_prepared_state_scope.py` checks the current context report, these
documented boundaries, and the exact declarative fixtures. It does not read benchmark artifacts,
infer readiness from benchmark rows, or claim that fixture contents prove runtime behavior.

Runtime regression ownership stays with the existing native execution tests:

- `shardloom-cli/tests/resident_worker.rs` checks resident worker execution and source-state reuse
  while verifying that requests execute and preserve no-fallback evidence.
- `python/tests/test_native_session_execution.py` checks per-request native execution, result
  freshness, reuse reporting, and session lifecycle behavior.

Those tests provide runtime behavior checks; the fixtures and validator describe and guard the
static contract only. All admitted route evidence remains `claim_gate_status=not_claim_grade`,
`fallback_attempted=false`, and `external_engine_invoked=false`.

## Vortex-First Provider Check

- Subject: local source normalization, native input, and resident prepared-state reuse.
- Vortex concepts checked: Vortex files and arrays, source/split, scan, and sink boundaries.
- Decision: use Vortex-native input/provider surfaces within ShardLoom's single
  `native_vortex_query` admission route; keep the scope as a ShardLoom report and policy wrapper.
- Residual handling: execute through the shared ShardLoom-native Vortex plan or reject explicitly;
  no query-engine integration is a fallback.
- Evidence boundary: this document, the declarative fixture set, the context report, and the
  separately owned native execution regression tests. This validator alone is not runtime proof.
- Unsupported boundaries remain explicit above; no performance claim is admitted here.
