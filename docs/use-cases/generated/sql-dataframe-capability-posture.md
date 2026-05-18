<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL and DataFrame capability posture

## Quick Answer

- **Audience:** user asking whether SQL text or DataFrame-style APIs have production support
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

- `python/README.md` - What this proves: Python wrapper posture, local smoke usage, and Python API claim boundaries.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/canonical-terminology.md` - What this proves: Canonical terminology for support states, execution modes, and claim language.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.

## Related Use Cases

- `python-wrapper-client-smoke`
- `source-free-generated-output-boundary`

## Related Field Guide Terms

- `website/field-guide/direct-compatibility-transient.html` - Direct Compatibility Transient (`Execution Modes` / `report-only`)
- `website/field-guide/report-only.html` - Report-Only (`Evidence And Claims` / `status-vocabulary`)
- `website/field-guide/unsupported-diagnostics.html` - Unsupported Diagnostics (`Evidence And Claims` / `current-diagnostic-pattern`)
- `website/field-guide/arrow-ipc-boundary.html` - Arrow IPC Boundary (`I/O And Output` / `compatibility-boundary`)
- `website/field-guide/rest-control-plane.html` - REST Control Plane (`Platform Boundaries` / `report-only`)
- `website/field-guide/mcp-agent-api.html` - MCP Agent API (`Platform Boundaries` / `planned-report-only`)
