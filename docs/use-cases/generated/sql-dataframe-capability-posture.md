<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL and DataFrame capability posture

## Quick Answer

- **Audience:** user asking whether SQL text or DataFrame-style APIs are production-ready
- **Status:** `report_only`
- **Execution mode:** `report_only`
- **Engine mode:** `none`
- **Claim boundary:** SQL/DataFrame readiness is inspectable but runtime execution is not broadly supported or production-claimable.

## Can ShardLoom Do This?

SQL and DataFrame capability posture is inspectable as posture or diagnostics, but it is not broad runtime support.

## How To Try It

```powershell
target\debug\shardloom capabilities sql --format json
```

## Blocker

SQL parse/bind/plan/execute and broad DataFrame runtime support require future admitted runtime slices with correctness, evidence, and no-fallback proof.

## Internal Flow

`sql_text, dataframe_api_request -> report_only -> none -> capability_report, deterministic_unsupported_diagnostics -> evidence -> claim gate`

## Evidence You Should See

- `support_status=report_only`
- `runtime_execution=false`
- `planner_readiness_non_executing`
- `claim_gate_status=not_claim_grade`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A capability posture report showing report_only or unsupported rows rather than runtime execution.

## Common Mistakes

- `submitting_sql_and_expecting_execution`
- `assuming_dataframe_lazy_api_exists`
- `mistaking_report_only_for_supported`

## Reference Files

- `python/README.md`
- `docs/architecture/compute-engine-flow-reference.md`
- `docs/architecture/canonical-terminology.md`
- `README.md`

## Related Use Cases

- `python-wrapper-client-smoke`
- `source-free-generated-output-boundary`
