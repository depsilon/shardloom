<!-- SPDX-License-Identifier: Apache-2.0 -->

# E2E Performance Baseline Protocol and Retain/Drop Contract (`v1.1.0`)

Status: frozen verification contract for `E2E-VERIFY-01`, `E2E-VERIFY-02`, and `E2E-PERF-00` through `E2E-PGO-01` under canonical `PERF-01` and `PERF-12`.

```text
schema_version=shardloom.e2e_perf_baseline_protocol.v1
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
performance_claim_allowed=false
production_claim_allowed=false
```

## 1. Environment & Dataset Boundary

- Full 100M-row ClickBench `Full43` (`hits.parquet` / `hits.vortex`) and offline DuckDB (`clickbench_independent_reference.py`) verification requires an explicitly mounted local dataset and optional DuckDB test environment outside cloud-synced folders (`docs/architecture/local-development-storage.md`).
- When those full external artifacts are not mounted, CI and local verification execute bounded deterministic fixtures (`scripts/run_ci_perf_smoke.py` and `shardloom-vortex/src/local_primitives/differential_oracle_tests.rs`) and unit tests without synthesizing or claiming 100M timings.

## 2. Same-Commit Exclusive-Span Baseline Capture Contract

Every same-commit `PERF-01` baseline and downstream `E2E-PERF-*` comparison MUST capture:

1. **Timing Surface Separation (`E2E-VERIFY-02`)**:
   - `cold_process_cli`: fresh subprocess spawn per query (`run_clickbench_query_uat.py --lane cold_process_cli`), reporting both `engine_execution_seconds` and `seconds` (`process_wall`).
   - `warm_resident_session` / `warm_resident_concurrent`: persistent resident worker (`run_clickbench_query_uat.py --lane warm_resident_session` or `--lane paired`), measuring `worker_startup_seconds` strictly outside per-query execution timings and asserting `zero_answer_cache_verified=true`.
   - `ingest_100m`: exclusive stage breakdown via `scripts/run_clickbench_ingest_uat.sh`.
2. **OS Resource Telemetry**:
   - Per-operation `user_cpu_seconds`, `system_cpu_seconds`, `minor_page_faults`, `major_page_faults`, and `native_peak_rss_bytes` via `scripts/timed_native_command.py`.
3. **Correctness & No-Fallback Evidence**:
   - Full `result_sha256` equality across every `(query, run)` against the frozen `independent_reference` packet (updated to `HUGEINT` exact integer accumulation matching `E2E-FIX-01`), with `fallback_attempted=false` and `external_engine_invoked=false`.

## 3. Numeric Retain / Drop Decision Thresholds (`scripts/evaluate_perf_ship_drop.py`)

A candidate commit is eligible for `retain` **only** if all of the following pass on the same `timing_surface` and `input_format`:

| Dimension | Threshold | Failure Action |
| --- | --- | --- |
| Run completeness & correctness | `100%` queries passed, matching `result_sha256` & `zero_answer_cache_verified` | Immediate `drop` |
| Timing surface / format / query set | Exact match (`cold_process_cli` vs `cold_process_cli`, `warm_*` vs `warm_*`) | `blocked` |
| Aggregate engine time improvement | `<= -2.0%` delta vs same-commit baseline (configurable per screen) | `drop` |
| Per-query tail regression | No individual query worse than `+5.0%` | `drop` |
| Peak RSS delta | `<= +10.0%` | `drop` |
| System CPU seconds delta | `<= +10.0%` | `drop` |
| Minor / major page-fault delta | `<= +15.0%` | `drop` |

Any dropped candidate keeps the baseline source intact and records both baseline and dropped summary digests under non-synced LocalData logs.
