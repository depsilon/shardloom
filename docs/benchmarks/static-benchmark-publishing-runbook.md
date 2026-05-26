# Static Benchmark Publishing Runbook

ShardLoom publishes benchmark evidence as committed static artifacts. The website must not discover
competitor availability from the Cloudflare/static publishing environment, and it must not require
pandas, Polars, DuckDB, Spark, DataFusion, Dask, Java, or optional extended benchmark packages at
page-render time.

This workflow separates two jobs:

- **Benchmark execution artifact:** produced locally in a benchmark environment with the selected
  profile dependencies installed.
- **Website rendering artifact:** committed under `website/assets/benchmarks/latest/` and consumed
  by the static website generator.

Benchmark artifacts are local pre-release evidence only. They do not authorize performance,
superiority, Spark-displacement, production SQL/DataFrame, object-store/lakehouse/Foundry, or
package-publication claims.

## Profiles

- `smoke`: ShardLoom-only quick checks.
- `full_local`: ShardLoom plus pandas, Polars eager/lazy, DuckDB, DataFusion, and Dask.
- `full_local_plus_spark`: `full_local` plus `spark-default` and `spark-local-tuned`.
- `extended_local`: optional local ecosystem lanes such as pyarrow, clickhouse-local, Daft, Ray
  Data, and Ibis adapters.
- `gpu_optional`: GPU/CUDA-specific lanes such as cuDF/RAPIDS.
- `object_store_optional`: object-store scenarios only after object-store runtime support is
  separately admitted.
- `io_reuse_and_fanout`: source/prepared/output reuse and cross-format fanout benchmark family.

Missing required lanes fail the selected full profile. `full_local_plus_spark` treats PySpark
`spark-default` and `spark-local-tuned` as required baselines, so `pyspark` and a local JDK must be
available before publishing that profile. Missing optional extended/GPU/object-store lanes remain
visible with a reason. External engines are baseline context only and never ShardLoom fallback
execution.

## Publish A Local Artifact

```powershell
python -m venv .venv-bench
.\.venv-bench\Scripts\Activate.ps1
python -m pip install -r benchmarks\traditional_analytics\requirements-full-local.txt
python scripts\check_benchmark_environment.py --profile full_local_plus_spark
```

Run the benchmark and write the local execution artifact:

```powershell
python benchmarks\traditional_analytics\run.py `
  --rows 100000 `
  --iterations 3 `
  --include-taxonomy-extra `
  --engines shardloom,shardloom-vortex,shardloom-prepared-vortex,shardloom-prepare-batch,pandas,polars-eager,polars-lazy,duckdb,datafusion,dask,spark-default,spark-local-tuned `
  --formats csv,parquet `
  --shardloom-result-sink `
  --require-all-engines `
  --output target\benchmark-artifacts\traditional-full-local-plus-spark.json `
  --markdown-output target\benchmark-artifacts\traditional-full-local-plus-spark.md
```

The legacy `polars` CLI alias expands to `polars-eager` and `polars-lazy`, but full-local publishing
commands should name the split lanes explicitly so the manifest and raw rows remain easy to audit.
The legacy `spark` CLI alias expands to `spark-default` and `spark-local-tuned`, but publishing
commands should name the PySpark split lanes explicitly for the same reason.
Optional extended lanes such as `pyarrow-dataset`, `pyarrow-acero`, `clickhouse-local`, `daft`,
`ray-data`, `ibis-*`, and `cudf-gpu` are selected only by extended/GPU profiles or explicit
`--engines` requests. Missing optional dependencies and unimplemented adapters must remain visible
as deterministic unavailable or unsupported rows.

Promote the local execution artifact into committed website artifacts:

```powershell
python scripts\promote_benchmark_artifact.py `
  --profile full_local_plus_spark `
  --input target\benchmark-artifacts\traditional-full-local-plus-spark.json
```

The promotion step is the only supported way to refresh the public comparative benchmark snapshot.
It derives the website timing/context tables from the promoted benchmark artifact, records
`expected_lanes`, `available_lanes`, `missing_lanes`, lane versions, and lane availability reasons,
and keeps external engines marked as `external_baseline_only`.

Keep raw benchmark Markdown under `target/benchmark-artifacts/` as local evidence unless a separate
claim-safe public Markdown renderer is added. The website latest bundle publishes the JSON manifest
and website summary only, because raw benchmark Markdown can contain claim-safety language that is
appropriate for local evidence but not for the public static site.

The promoter mirrors the same generated bundle into the Astro import data, the Astro public asset
source, and the committed static output. Do not hand-edit those copies independently:

- `website-public/assets/benchmarks/latest/manifest.json`
- `website-public/assets/benchmarks/latest/benchmark-results.json`
- `website-public/assets/data/benchmark-evidence.json`
- `website-src/src/data/benchmark-manifest.json`
- `website-src/src/data/benchmark-evidence.json`
- `website/assets/benchmarks/latest/manifest.json`
- `website/assets/benchmarks/latest/benchmark-results.json`
- `website/assets/data/benchmark-evidence.json`

Prepare website pages from the committed Astro source/public assets:

```powershell
cd website-src
npm run build
cd ..
python scripts\check_benchmark_artifact_completeness.py `
  --manifest website\assets\benchmarks\latest\manifest.json
python scripts\check_benchmark_constitution.py
python scripts\check_website_readiness.py
node website\validate_static_assets.js
git diff --check
```

The committed latest bundle is expected to include matching Astro source/public/static copies:

- `website/assets/benchmarks/latest/manifest.json`
- `website/assets/benchmarks/latest/benchmark-results.json`
- `website/assets/data/benchmark-evidence.json`
- `website-public/assets/benchmarks/latest/manifest.json`
- `website-public/assets/benchmarks/latest/benchmark-results.json`
- `website-public/assets/data/benchmark-evidence.json`
- `website-src/src/data/benchmark-manifest.json`
- `website-src/src/data/benchmark-evidence.json`
- `website/benchmarks.html`

## Manifest Rules

The manifest must include:

- `schema_version`
- `generated_at_utc`
- `benchmark_profile`
- `benchmark_git_sha`
- `shardloom_git_sha`
- `expected_lanes`
- `available_lanes`
- `missing_lanes`
- `lane_versions`
- `lane_availability_reasons`
- `environment`
- `benchmark_constitution_schema_version`
- `benchmark_constitution_validator`
- `benchmark_constitution_required_field_order`
- `benchmark_constitution_claim_gate_status`
- `benchmark_constitution_performance_claim_allowed=false`
- `claim_boundary`
- `performance_claim_allowed=false`
- `artifact_paths`

ShardLoom rows must preserve `fallback_attempted=false` and `external_engine_invoked=false`.
External rows must remain `external_baseline_only=true` or `row_classification=external_baseline_only`.

## Incomplete Artifacts

Incomplete artifacts may be committed only when they are explicitly marked incomplete and not
presented as latest full-local evidence. The website must show missing lanes with reasons instead of
omitting them.

## Stale Artifact Guardrails

Do not rely on a generated HTML dashboard from another repository or workstation as the canonical
comparative source. `website/benchmarks.html` must be generated from
`website/assets/benchmarks/latest/manifest.json`, and that manifest must point at committed website
benchmark data. The completeness checker fails artifacts that still reference `spark-retire`, collapse
Polars into a single full-local lane, or mark an expected lane available without published row
evidence.
