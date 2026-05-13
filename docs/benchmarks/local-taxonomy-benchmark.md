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

## Claim-Readiness Rerun

The selected P7.4.4 closeout rerun is:

```powershell
python benchmarks\traditional_analytics\run.py `
  --claim-readiness-rerun `
  --dataset-profile narrow_fact_dim `
  --rows 100000 `
  --iterations 3 `
  --output target\shardloom-claim-readiness-rerun.json `
  --regenerate
```

The preset uses ShardLoom, the ShardLoom Vortex fixture lane, and selected
local optional baselines. It keeps managed platforms out, enables ShardLoom
result-sink proof, includes taxonomy extras when no explicit scenario is
provided, and rejects fewer than three iterations.

## Claim Scope

Coverage rows are separate from timing rows. Each row carries a
`row_classification`/`status` and a `support_status` so support evidence,
claim evidence, fixture-smoke rows, unsupported rows, blocked rows, and
external baselines are not conflated.

ShardLoom rows are claim-grade only when the artifact includes stable
correctness digests across at least three iterations, benchmark and coverage
refs, execution certificate evidence, source Native I/O certificate evidence,
result Native I/O certificate evidence when result-sink proof is enabled,
materialization/decode boundary evidence, `fallback_attempted=false`, and
`external_engine_invoked=false`. Unsupported or incompatible scenario/profile
pairs should emit deterministic coverage rows rather than crash or delegate to
an external engine.
