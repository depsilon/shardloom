<!-- SPDX-License-Identifier: Apache-2.0 -->

# Certified Local Workload

ShardLoom's current workload-certified slice is:

```text
local_vortex_analytics_v1
```

This is a scoped local workflow:

- local compatibility input imported into Vortex artifacts
- supported local analytics execution
- Vortex result artifact write
- source and result replay verification
- execution certificate and Native I/O certificate fields
- scheduler and memory evidence
- `fallback_attempted=false`
- `external_engine_invoked=false`

## What It Does Not Claim

This certification is not a broad SQL engine claim, DataFrame runtime claim,
live/hybrid production claim, object-store claim, Foundry claim, or Spark
replacement claim. Those surfaces remain future or unsupported until they have
their own correctness, benchmark, certificate, Native I/O, and no-fallback
evidence.

## Local Workflow Command

Use the public benchmark harness with Vortex output to inspect native execution,
committed output and complete result readback:

```powershell
python benchmarks\traditional_analytics\run.py `
  --shardloom-binary target\release\shardloom.exe `
  --workspace "$HOME\LocalData\shardloom\certified-local-workload" `
  --engines shardloom pandas --reference-engine pandas `
  --formats csv parquet `
  --scenarios "selective filter" `
  --dataset-profile tiny_smoke `
  --rows 256 --dim-rows 20 --repeats 3 `
  --input-state raw --output-format vortex
```

Supply an already-built executable and keep generated artifacts in local-only storage.
The run retains every workload declaration, complete expected and observed result, resource
request, native report and process receipt. Pandas supplies an independent reference; it never
executes work on ShardLoom's behalf. A successful smoke verifies this configured workflow and
does not establish a performance claim or broader workload certification.
