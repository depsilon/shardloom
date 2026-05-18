<!-- SPDX-License-Identifier: Apache-2.0 -->

# Table and lakehouse boundary

## Quick Answer

- **Audience:** user asking whether Iceberg, Delta, Hudi, or catalog commits are supported
- **Status:** `blocked`
- **Execution mode:** `report_only_blocked`
- **Engine mode:** `none`
- **Claim boundary:** No production lakehouse/table-format runtime, commit, merge/update/delete, catalog, object-store, or Foundry claim.

## Can ShardLoom Do This?

Table and lakehouse boundary is blocked or unsupported until the listed evidence exists.

## How To Try It

```powershell
target\debug\shardloom table-compat-plan aggregate --format json
```

## Blocker

Table-format runtime needs metadata, snapshot, delete/tombstone, write, commit, rollback, object-store, and certificate evidence before support can be claimed.

## Internal Flow

`iceberg_table, delta_table, hudi_table, catalog_metadata -> report_only_blocked -> none -> table_compatibility_matrix, deterministic_blocker -> evidence -> claim gate`

## Evidence You Should See

- `table_scan_status`
- `metadata_read_status`
- `delete_tombstone_status`
- `commit_protocol_status`
- `rollback_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=not_claim_grade`

## Expected Output Or Evidence

A report-only table compatibility plan or deterministic unsupported output.

## Common Mistakes

- `mistaking_metadata_smoke_for_table_runtime`
- `assuming_commit_semantics`
- `treating_local_files_as_lakehouse_support`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/universal-compatibility-coverage-scoreboard.md` - What this proves: Compatibility scoreboard status and source/sink support boundaries.
- `docs/architecture/universal-input-contract.md` - What this proves: Universal input contract posture and unsupported input-family diagnostics.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.

## Related Use Cases

- `object-store-boundary-report`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/blocked-status.html` - Blocked Status (`Evidence And Claims` / `status-vocabulary`)
- `website/field-guide/table-lakehouse-boundary.html` - Table/Lakehouse Boundary (`I/O And Output` / `blocked-report-only`)
- `website/field-guide/universal-adapter-catalog.html` - Universal Adapter Catalog (`Platform Boundaries` / `report-only`)
- `website/field-guide/scale-profile.html` - Scale Profile (`Performance Architecture` / `report-only-scale-contract`)
