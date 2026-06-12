<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL and DataFrame capability posture

## Quick Answer

- **Audience:** user asking whether SQL text or DataFrame-style APIs have production support
- **Status:** `report_only`
- **Execution mode:** `report_only`
- **Engine mode:** `none`
- **Claim boundary:** Scoped SQL runtime families are inspectable and fixture-smoke-supported through the SQL frontend runtime ladder, and the Python method matrix marks ctx.sql plus scoped local DataFrame/query-builder rows, including predicate-object join conditions that lower to admitted SQL JOIN ON expression predicates, as fixture-smoke-supported. Broad SQL/DataFrame readiness remains report-only or blocked and is not production-claimable.

## Can ShardLoom Do This?

SQL and DataFrame capability posture is inspectable as posture or diagnostics, but it is not broad runtime support.

## Claim Boundary

Scoped SQL runtime families are inspectable and fixture-smoke-supported through the SQL frontend runtime ladder, and the Python method matrix marks ctx.sql plus scoped local DataFrame/query-builder rows, including predicate-object join conditions that lower to admitted SQL JOIN ON expression predicates, as fixture-smoke-supported. Broad SQL/DataFrame readiness remains report-only or blocked and is not production-claimable.

## How To Try It

```text
target\debug\shardloom capabilities sql --format json
```

## Blocker

Broad SQL parse/bind/plan/execute, catalogs, CTEs, set operations, correlated/broad subqueries, object-store/table SQL, and broad DataFrame runtime support beyond current scoped predicate-object join conditions require future admitted runtime slices with correctness, evidence, and no-fallback proof.

## Internal Flow

`sql_text, dataframe_api_request -> report_only -> none -> capability_report, deterministic_unsupported_diagnostics -> evidence -> claim gate`

## Evidence You Should See

- `support_status=report_only`
- `sql_frontend_runtime_ladder_schema_version=shardloom.sql_frontend_runtime_ladder.v1`
- `planner_readiness_non_executing`
- `scoped_sql_runtime_execution=true`
- `dataframe_method_matrix_sql_support_status=fixture_smoke_supported`
- `workflow_unsupported_broad_generated_with_column_blocker=gar-gen-1.dataframe_generated_with_column_broad_expression_runtime_blocked`
- `broad_sql_claim_allowed=false`
- `claim_gate_status=not_claim_grade`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A capability posture report showing the scoped SQL runtime ladder, the Python/DataFrame method matrix split between fixture-smoke-supported rows and broad blockers, plus report_only or unsupported broad SQL/DataFrame rows.

## Common Mistakes

- `submitting_broad_sql_and_expecting_execution`
- `assuming_dataframe_lazy_api_means_broad_runtime`
- `mistaking_report_only_for_supported`

## Reference Files

- `python/README.md` - What this proves: Python wrapper scope, local smoke usage, and Python API claim boundaries.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/canonical-terminology.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.

## Related Use Cases

- `python-wrapper-client-smoke`
- `source-free-generated-output-boundary`

## Related Field Guide Terms

- [No fallback](https://shardloom.io/field-guide/no-fallback) (`Start Here` / `runtime_supported`)
- [Deterministic blockers](https://shardloom.io/field-guide/deterministic-blockers) (`Unsupported Diagnostics` / `runtime_supported`)
- [report_only](https://shardloom.io/field-guide/report-only) (`Unsupported Diagnostics` / `report_only`)
