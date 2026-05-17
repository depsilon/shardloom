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

- `docs/architecture/compute-engine-flow-reference.md`
- `docs/architecture/universal-compatibility-coverage-scoreboard.md`
- `docs/architecture/universal-input-contract.md`
- `README.md`

## Related Use Cases

- `object-store-boundary-report`
- `output-result-sink-and-fanout-boundary`
