<!-- SPDX-License-Identifier: Apache-2.0 -->

# Table and lakehouse boundary

## Quick Answer

- **Audience:** user asking whether Iceberg, Delta, Hudi, or catalog commits are supported
- **Status:** `blocked`
- **Execution mode:** `report_only_blocked`
- **Engine mode:** `none`
- **Claim boundary:** Local table metadata/read and append commit rehearsal smokes are fixture-scoped only; no production lakehouse/table-format runtime, merge/update/delete, catalog, object-store, or Foundry claim.

## Can ShardLoom Do This?

Table and lakehouse boundary is not admitted runtime support yet. Use the blocker and evidence requirements to understand what remains.

## Claim Boundary

Local table metadata/read and append commit rehearsal smokes are fixture-scoped only; no production lakehouse/table-format runtime, merge/update/delete, catalog, object-store, or Foundry claim.

## How To Try It

```text
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
- `local_table_append_commit_rehearsal_smoke_present`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=not_claim_grade`

## Expected Output Or Evidence

A report-only table compatibility plan or deterministic unsupported output.

## Common Mistakes

- `mistaking_metadata_smoke_for_table_runtime`
- `treating_local_commit_rehearsal_as_production_table_commit`
- `assuming_catalog_commit_semantics`
- `treating_local_files_as_lakehouse_support`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/table-intelligence-layer.md` - What this proves: Table maintenance execution posture and lakehouse/table claim boundaries.
- `docs/architecture/universal-compatibility-coverage-scoreboard.md` - What this proves: Compatibility scoreboard status and source/sink support boundaries.
- `docs/architecture/universal-input-contract.md` - What this proves: Universal input contract posture and unsupported input-family diagnostics.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.

## Related Use Cases

- `local-table-append-commit-rehearsal-smoke`
- `object-store-boundary-report`
- `object-store-local-emulator-write-smoke`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/object-store-boundary.html` - Object-store boundary (`Platform Boundaries` / `smoke_supported`)
- `website/field-guide/table-lakehouse-boundary.html` - Table/lakehouse boundary (`Platform Boundaries` / `blocked`)
