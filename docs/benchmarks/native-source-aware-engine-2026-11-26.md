<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source-Aware Engine Foundation: Orders 1–5 Benchmark Summary (`2026-11-26`)

Orders 1–5 of the 7-order source-aware engine performance program are **locally retained**
against `04ad4b6d1ea1f12200b6bbd9bdb82e5ad5827bf3` (`origin/main` after
[PR #1722](https://github.com/depsilon/shardloom/pull/1722)). All 24 frozen cohort
queries (`Q01..Q24` across `G1..G5` Parquet and native `.vortex` workloads) pass
independent oracle verification in both cold-process CLI and warm-resident session
lanes with `fallback_attempted = false`, `external_engine_invoked = false`, and
strictly lower complete-cohort `engine_execution_seconds`:

- **Cold CLI complete cohort (`24/24` passed):** `1.977699s -> 1.555910s`
  (**`-0.421789s` / `-21.33%`**; exact: `1.977698827s -> 1.555910083s`)
- **Warm resident session complete cohort (`24/24` passed):** `1.893275s -> 1.553449s`
  (**`-0.339826s` / `-17.95%`**; exact: `1.893274827s -> 1.553448878s`)
- **Python embedded smoke:** `4/4` passed (`G1_agg_50k_int_parquet`: `0.049221s`,
  `G2_distinct_str_50k_parquet`: `0.054480s`, `G3_filtered_agg_where_parquet`: `0.004381s`,
  `G5_multi_join_agg_parquet`: `0.006109s` `engine_execution_seconds`).

Per `AGENTS.md` and the [benchmarking policy](../skills/benchmarking.md#timing-communication-decision),
all totals, group subtotals, and per-query figures below report **engine execution time
only (`engine_execution_seconds`)**. This local deterministic cohort gives
`claim_gate_status = not_claim_grade`, does not close any CG-* gate or claim
competitive superiority, leaves November 16 retired campaigns and `Full43` / `full10k`
untouched, and leaves **Orders 6–7** queued in
[the phased execution plan](../architecture/phased-execution-plan.md).

## Artifact provenance & execution envelope

- **Baseline (`04ad4b6d1ea1f12200b6bbd9bdb82e5ad5827bf3`):**
  `shardloom` CLI (`--features release-user-surfaces`, SHA256 `14a64fb177613caa462b59d8dc2b65c2cbae59dba83cbd1686f02e608c90614e`, prefix `14a64fb17761`),
  `_native.abi3.so` (`--features python-native`, SHA256 `a295ce55f3241c7e5b646f0f4b261d92f754275b14be25992adb6e4d7465689d`, prefix `a295ce55f324`).
- **Candidate (Orders 1–5):**
  `shardloom` CLI (`--features release-user-surfaces`, SHA256 `7802176bf6ee247081ba6e2b6e09b91c43f578aa87616d03fe7e50b1e9588440`, prefix `7802176bf6ee`),
  `_native.abi3.so` (`--features python-native`, SHA256 `6c330e8f7de6e7c2a70d4869d0ab440c1f954ec9c2e91a6594bb03b4af973a1c`, prefix `6c330e8f7de6e7c`).
- **Runtime environment:** `macOS arm64` (`macOS-27.2.1-arm64-arm-64bit-Mach-O`), isolated single-pass
  runner (`run_source_aware_cohort.py`), `--memory-gb 2 --max-parallelism 4` on
  frozen 1M-row Parquet/`.vortex` fact and 50K-row dimension fixtures plus Hive /
  `union_by_name` wrapper manifests (`24/24` oracle parity in both lanes;
  `fallback_attempted = false`, `external_engine_invoked = false`).

## Cohort group subtotals (`engine_execution_seconds`)

| Lane | Group | Queries | Baseline (s) | Candidate (s) | Delta (s) | Change |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Cold CLI | Complete cohort (`Q01..Q24`) | 24 | `1.977699` | `1.555910` | `-0.421789` | `-21.33%` |
| Cold CLI | `G1_grouped_agg` | 4 | `0.781953` | `0.685119` | `-0.096834` | `-12.38%` |
| Cold CLI | `G1_grouped_agg_float_ctrl` | 2 | `0.120623` | `0.111415` | `-0.009208` | `-7.63%` |
| Cold CLI | `G2_distinct_measures` | 2 | `0.413937` | `0.238898` | `-0.175039` | `-42.29%` |
| Cold CLI | `G2_string_distinct` | 4 | `0.202277` | `0.197261` | `-0.005016` | `-2.48%` |
| Cold CLI | `G3_predicate_first` | 4 | `0.069251` | `0.020862` | `-0.048389` | `-69.88%` |
| Cold CLI | `G4_logical_wrappers` | 2 | `0.070798` | `0.012121` | `-0.058677` | `-82.88%` |
| Cold CLI | `G5_join_and_fused` | 4 | `0.187397` | `0.170399` | `-0.016998` | `-9.07%` |
| Cold CLI | `G5_q12_native_control` | 2 | `0.131463` | `0.119834` | `-0.011629` | `-8.85%` |
| Warm resident | Complete cohort (`Q01..Q24`) | 24 | `1.893275` | `1.553449` | `-0.339826` | `-17.95%` |
| Warm resident | `G1_grouped_agg` | 4 | `0.731066` | `0.707986` | `-0.023080` | `-3.16%` |
| Warm resident | `G1_grouped_agg_float_ctrl` | 2 | `0.132587` | `0.105560` | `-0.027026` | `-20.38%` |
| Warm resident | `G2_distinct_measures` | 2 | `0.408264` | `0.246216` | `-0.162048` | `-39.69%` |
| Warm resident | `G2_string_distinct` | 4 | `0.199608` | `0.177592` | `-0.022017` | `-11.03%` |
| Warm resident | `G3_predicate_first` | 4 | `0.063396` | `0.016185` | `-0.047211` | `-74.47%` |
| Warm resident | `G4_logical_wrappers` | 2 | `0.059920` | `0.008954` | `-0.050966` | `-85.06%` |
| Warm resident | `G5_join_and_fused` | 4 | `0.165011` | `0.163423` | `-0.001588` | `-0.96%` |
| Warm resident | `G5_q12_native_control` | 2 | `0.133423` | `0.127533` | `-0.005889` | `-4.41%` |

*(Every one of the 8 cohort groups improved in both Cold CLI and Warm resident lanes; `G1_grouped_agg` and `G1_grouped_agg_float_ctrl` report separately above while `total_seconds` sums unique `Q01..Q24`.)*

## Per-query `engine_execution_seconds` (`Q01..Q24`)

| Query | Case | Format | Group | Cold base (s) | Cold cand (s) | Cold change | Warm base (s) | Warm cand (s) | Warm change | Observed mechanics / policy notes |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `Q01` | `G1_agg_50k_int_parquet` | `parquet` | `G1_grouped_agg` | `0.061006` | `0.049599` | `-18.70%` | `0.057695` | `0.043330` | `-24.90%` | `src_w=4, op_w=4, rg_tasks=4` |
| `Q02` | `G1_agg_50k_f64_parquet` | `parquet` | `G1_grouped_agg_float_ctrl` | `0.060987` | `0.054241` | `-11.06%` | `0.063126` | `0.056876` | `-9.90%` | Float determinism `src_w=4, op_w=1, rg_tasks=8` |
| `Q03` | `G1_agg_50k_str_parquet` | `parquet` | `G1_grouped_agg` | `0.065568` | `0.059295` | `-9.57%` | `0.064797` | `0.060163` | `-7.15%` | `src_w=4, op_w=4, rg_tasks=4` |
| `Q04` | `G2_distinct_str_10k_parquet` | `parquet` | `G2_string_distinct` | `0.027938` | `0.038029` | `+36.12%` | `0.026982` | `0.034286` | `+27.07%` | `src_w=4, op_w=4, rg_tasks=4` |
| `Q05` | `G2_distinct_str_50k_parquet` | `parquet` | `G2_string_distinct` | `0.055084` | `0.053598` | `-2.70%` | `0.051408` | `0.052065` | `+1.28%` | `src_w=4, op_w=4, rg_tasks=4` |
| `Q06` | `G2_count_distinct_str_parquet` | `parquet` | `G2_distinct_measures` | `0.200166` | `0.114612` | `-42.74%` | `0.198305` | `0.118590` | `-40.20%` | `src_w=4, op_w=1`; Order 4 borrowed `DistinctMeasureRows` |
| `Q07` | `G3_filtered_agg_where_parquet` | `parquet` | `G3_predicate_first` | `0.017775` | `0.004564` | `-74.32%` | `0.016387` | `0.004399` | `-73.16%` | `src_w=4, op_w=4, kf=1,000,000->50,000, pay=50,000` |
| `Q08` | `G3_filtered_proj_where_parquet` | `parquet` | `G3_predicate_first` | `0.042493` | `0.005900` | `-86.12%` | `0.040335` | `0.004971` | `-87.68%` | `src_w=4, op_w=1, kf=1,000,000->10,000, pay=10,000` |
| `Q09` | `G5_inner_join_parquet` | `parquet` | `G5_join_and_fused` | `0.015669` | `0.017879` | `+14.11%` | `0.016734` | `0.015618` | `-6.67%` | `src_w=4, op_w=4, kf=1,050,000->53,011, pay=53,011` (`SealedJoin`) |
| `Q10` | `G5_multi_join_agg_parquet` | `parquet` | `G5_join_and_fused` | `0.010627` | `0.006580` | `-38.08%` | `0.010433` | `0.006001` | `-42.48%` | `src_w=4, op_w=1, kf=1,050,000->53,011, pay=53,011` (`join_aggregate`) |
| `Q11` | `G5_q12_control_parquet` | `parquet` | `G5_q12_native_control` | `0.035563` | `0.034760` | `-2.26%` | `0.034421` | `0.034859` | `+1.27%` | `src_w=4, op_w=1, kf=1,000,000->250,000, pay=250,000` |
| `Q12` | `G1_agg_50k_int_vortex` | `vortex` | `G1_grouped_agg` | `0.343669` | `0.315345` | `-8.24%` | `0.327263` | `0.340713` | `+4.11%` | Native `.vortex` integer grouped aggregate (`src_w=4, op_w=4`) |
| `Q13` | `G1_agg_50k_f64_vortex` | `vortex` | `G1_grouped_agg_float_ctrl` | `0.059636` | `0.057175` | `-4.13%` | `0.069461` | `0.048685` | `-29.91%` | Native `.vortex` float determinism (`src_w=4, op_w=1`) |
| `Q14` | `G1_agg_50k_str_vortex` | `vortex` | `G1_grouped_agg` | `0.311711` | `0.260882` | `-16.31%` | `0.281310` | `0.263781` | `-6.23%` | Order 3 `.vortex` dictionary string reuse (`src_w=4, op_w=4`) |
| `Q15` | `G2_distinct_str_10k_vortex` | `vortex` | `G2_string_distinct` | `0.053325` | `0.043027` | `-19.31%` | `0.053925` | `0.038878` | `-27.90%` | Order 3 `RowSet` `EpochDomainMap` (`src_w=4, op_w=4`) |
| `Q16` | `G2_distinct_str_50k_vortex` | `vortex` | `G2_string_distinct` | `0.065930` | `0.062607` | `-5.04%` | `0.067294` | `0.052363` | `-22.19%` | Order 3 `RowSet` `EpochDomainMap` (`src_w=4, op_w=4`) |
| `Q17` | `G2_count_distinct_str_vortex` | `vortex` | `G2_distinct_measures` | `0.213771` | `0.124286` | `-41.86%` | `0.209959` | `0.127626` | `-39.21%` | Order 4 borrowed `DistinctMeasureRows` (`src_w=4, op_w=1`) |
| `Q18` | `G3_filtered_agg_where_vortex` | `vortex` | `G3_predicate_first` | `0.004661` | `0.005761` | `+23.61%` | `0.003565` | `0.003597` | `+0.88%` | Native `.vortex` selective aggregate (`src_w=4, op_w=4`) |
| `Q19` | `G3_filtered_proj_where_vortex` | `vortex` | `G3_predicate_first` | `0.004321` | `0.004636` | `+7.30%` | `0.003110` | `0.003219` | `+3.50%` | Native `.vortex` filtered projection (`src_w=1, op_w=1`) |
| `Q20` | `G5_inner_join_vortex` | `vortex` | `G5_join_and_fused` | `0.109413` | `0.103928` | `-5.01%` | `0.118707` | `0.123240` | `+3.82%` | Order 5 `.vortex` `SealedJoin` (`src_w=4, op_w=4`) |
| `Q21` | `G5_multi_join_agg_vortex` | `vortex` | `G5_join_and_fused` | `0.051689` | `0.042012` | `-18.72%` | `0.019137` | `0.018563` | `-3.00%` | `src_w=1, op_w=1` (compressed `.vortex` key-borrow decline explicit) |
| `Q22` | `G5_q12_control_vortex` | `vortex` | `G5_q12_native_control` | `0.095899` | `0.085074` | `-11.29%` | `0.099002` | `0.092675` | `-6.39%` | Order 1 `FileKernel` unwrap (`src_w=1, op_w=1`) |
| `Q23` | `G4_part_prune_where_parquet` | `parquet` | `G4_logical_wrappers` | `0.013314` | `0.007004` | `-47.39%` | `0.013143` | `0.005509` | `-58.09%` | Order 2 Hive partition prune + `src_w=2, op_w=2, rg_tasks=2` |
| `Q24` | `G4_union_by_name_agg_parquet` | `parquet` | `G4_logical_wrappers` | `0.057484` | `0.005117` | `-91.10%` | `0.046776` | `0.003445` | `-92.64%` | Order 2 `union_by_name` Stage-1 `RowSelection`: `src_w=4, op_w=4, kf=1,000,000->50,000, pay=50,000` |

## Validation & wheel proof

- **`cargo fmt --all -- --check`:** passed.
- **`cargo clippy --workspace --all-targets -- -D warnings`:** passed.
- **`cargo clippy -p shardloom-cli -p shardloom-vortex -p shardloom-python --all-targets --features release-user-surfaces -- -D warnings`:** passed.
- **`cargo test --locked --profile ci-native -p shardloom-vortex --lib --features release-user-surfaces`:** `4,571` passed, `0` failed, `42` ignored.
- **`cargo test --workspace --all-targets`:** `3,389` passed, `0` failed, `0` ignored across `108` test binaries.
- **`scripts/write_public_api_reference.py --check`:** passed.
- **`scripts/run_all_checks.py --no-json`:** `37/37` passed.
- **Isolated native Python wheel proof (`scripts/prove_native_python_wheel.py`):** `"status": "passed"`, `"uninstall_status": "passed"`, wheel `shardloom-1.3.0-cp310-abi3-macosx_27_0_arm64.whl` (`61,370,954` bytes, SHA256 `3af026cca1a8808b6e5795a6eaabfa3bcaba72b4bcf2056365adc2bbf9a872dd`), isolated `integration` suite `494/494` passed (`0` failures, `0` errors, `0` skipped).
