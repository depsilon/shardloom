<!-- SPDX-License-Identifier: Apache-2.0 -->

# Python wrapper and client smoke

## Quick Answer

- **Audience:** Python user who wants import-friendly status and capability checks
- **Status:** `ready_local`
- **Execution mode:** `no_dataset_smoke`
- **Engine mode:** `batch_status`
- **Claim boundary:** Thin Python CLI client only; not a native binding, DataFrame API, SQL runtime, UDF runtime, package-publication proof, or fallback path.

## Can ShardLoom Do This?

Python wrapper and client smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Thin Python CLI client only; not a native binding, DataFrame API, SQL runtime, UDF runtime, package-publication proof, or fallback path.

## How To Try It

```powershell
$env:PYTHONPATH = "python\src"; python -c "from shardloom import ShardLoomClient; print(ShardLoomClient.from_repo().status().status)"
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`none -> no_dataset_smoke -> batch_status -> python_capability_view, typed_output_envelope -> evidence -> claim gate`

## Evidence You Should See

- `protocol_version`
- `resolved_cli_path`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `support_status`
- `claim_gate_status`

## Expected Output Or Evidence

Python can import the package and invoke explicit status/capability commands through the local CLI.

## Common Mistakes

- `expecting_import_side_effects`
- `expecting_native_python_execution`
- `treating_capability_view_as_runtime_support`

## Reference Files

- `python/README.md` - What this proves: Python wrapper scope, local smoke usage, and Python API claim boundaries.
- `docs/getting-started/first-10-minutes.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `examples/local-python-smoke/README.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.

## Related Use Cases

- `first-10-minutes-local-smoke`
- `python-local-csv-query-builder-smoke`
- `sql-dataframe-capability-posture`

## Related Field Guide Terms

- `website/field-guide/direct-compatibility-transient.html` - direct_compatibility_transient (`Execution Routes` / `smoke_supported`)
