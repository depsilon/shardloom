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

Use the benchmark smoke with result-sink replay enabled when you want to inspect
the current evidence path:

```powershell
python benchmarks\traditional_analytics\run.py `
  --engines shardloom `
  --formats csv,parquet `
  --scenario "selective filter" `
  --dataset-profile tiny_smoke `
  --rows 256 `
  --iterations 3 `
  --shardloom-build-profile debug `
  --shardloom-result-sink `
  --skip-shardloom-native `
  --no-markdown `
  --data-dir target\shardloom-local-workload-smoke-data `
  --output target\shardloom-local-workload-smoke.json `
  --regenerate
```

The generated artifact includes timing rows, coverage rows, certificate fields,
Native I/O fields, materialization boundary fields, and no-fallback evidence.
