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

## How To Try It

```powershell
$env:PYTHONPATH = "python\src"; python -c "from shardloom import ShardLoomClient; print(ShardLoomClient.from_repo().status().status)"
```

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

- `python/README.md` - What this proves: Python wrapper posture, local smoke usage, and Python API claim boundaries.
- `docs/getting-started/first-10-minutes.md` - What this proves: Shortest local orientation path for smoke checks and evidence inspection.
- `examples/local-python-smoke/README.md` - What this proves: Runnable or blocked example posture, expected local command path, and claim boundary.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.

## Related Use Cases

- `first-10-minutes-local-smoke`
- `sql-dataframe-capability-posture`

## Related Field Guide Terms

- `website/field-guide/first-ten-minutes.html` - First 10 Minutes (`User Workflows` / `ready-local-docs`)
- `website/field-guide/python-wrapper-client.html` - Python Wrapper Client (`User Workflows` / `current-local-client`)
