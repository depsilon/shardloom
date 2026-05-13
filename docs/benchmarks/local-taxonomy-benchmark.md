<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local Taxonomy Benchmark

The local taxonomy benchmark harness lives at
`benchmarks/traditional_analytics/run.py`. It separates timing rows from
coverage rows and keeps external engines labeled as baselines, not fallback
execution.

## ShardLoom Smoke

```powershell
python examples\local-vortex-benchmark\run.py --repo-root .
```

Equivalent direct command:

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
  --output target\shardloom-local-taxonomy-smoke.json `
  --regenerate
```

## Comparative Local Baselines

Optional local baselines may be added for comparison:

```powershell
python benchmarks\traditional_analytics\run.py `
  --engines shardloom,pandas,polars,duckdb,datafusion `
  --formats csv,parquet `
  --include-taxonomy-extra `
  --dataset-profile narrow_fact_dim `
  --rows 100000 `
  --iterations 3 `
  --shardloom-result-sink `
  --output target\shardloom-local-taxonomy-comparative.json `
  --regenerate
```

Only install baseline engines in benchmark environments. They are not ShardLoom
runtime dependencies and must not execute unsupported ShardLoom work as
fallback.

## Claim Scope

Rows are claim-grade only when the artifact includes the required correctness,
benchmark, execution certificate, Native I/O, materialization/decode boundary,
provider, residual executor, and no-fallback evidence. Unsupported scenarios
should emit deterministic coverage rows rather than crash or delegate to an
external engine.
