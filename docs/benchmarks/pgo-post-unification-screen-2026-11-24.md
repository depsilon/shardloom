<!-- SPDX-License-Identifier: Apache-2.0 -->

# Post-Unification `E2E-PGO-01` ThinLTO Training, Coverage, and Screen (2026-11-24)

This note records the post-unification `E2E-PGO-01` (`PERF-13`) non-ClickBench `multi-family` PGO
screen executed via [pgo_local_experiment.py](../../scripts/pgo_local_experiment.py). All
measurements keep `claim_gate_status=not_claim_grade`, `default_release_profile_changed=false`,
`target_cpu_native_enabled=false`, `fallback_attempted=false`, and `external_engine_invoked=false`.
Only **engine execution time** (`engine_execution_seconds`) is reported in prose for TPC-H/SSB;
process wall, CPU, peak RSS, page faults, build durations, and held-out operator supervised process
timings are separated into the diagnostic tables below.

## Build, training corpus, and LLVM 22 hot-symbol coverage

- **Source commit**: `78e4a1cd77241b82e605a8d00f0086f865096432` (after aligning harness scripts; see
  *Harness disclosures* below).
- **Toolchain**: `rustc 1.96.0 (ac68faa20 2026-05-25)` (`LLVM 22.1.2`, target `aarch64-apple-darwin`)
  paired with `/opt/homebrew/opt/llvm@22/bin/llvm-profdata` (`Homebrew LLVM version 22.1.8`),
  `--features release-user-surfaces`, `--profile release-pgo` (`CARGO_BUILD_JOBS=4`,
  `caffeinate -i`, non-synced workspace `/Users/dylan/LocalData/shardloom/pgo/screen-20261124-final`).
- **Artifacts**:
  - `control` (`[]`): `83,802,864` B (`sha256: 743d5a132d427a60744001b7a8371df315fa3dcfcb7980fede197c5fa82a89a4`)
  - `instrumented` (`-Cprofile-generate=...`): `167,904,896` B (`sha256: c62dd8f914fdac86119167f8bc7c072d88fd33926168aae7f7bb92f84af4a2f9`)
  - `profile-use` (`-Cprofile-use=merged.profdata -Cllvm-args=-pgo-warn-missing-function`): `73,665,376` B (`sha256: e2551c5f830f7782ee953164a7027948359059c3b452875abdb5b5ac33ecfe3b`)
  - Merged profile: `1` `.profraw` input (`27,145,784` B) merged into `merged.profdata` (`40,227,728` B).
- **Training corpus (`multi-family`)**:
  - `12`-case operator subset (`rows=4096`, `workers=1,4`, `samples=1`) + deterministic TPC-H/SSB
    (`scale_rows=2400`, `seed=19991231`, Parquet + Vortex), `all_exact_values_passed=true`.
- **Hot-symbol coverage gate (`llvm-profdata show --topn=250`)**:
  - `total_functions = 100,738`, `max_function_count = 18,022,400`, `48` hot `shardloom_vortex`
    symbols and `34` hot `shardloom_runtime` symbols within the top 250 functions.

## Held-out TPC-H/SSB Vortex evaluation (`scale_rows=12000`, `seed=20261124`, `14/14` queries)

Both matched `control` and `profile-use` binaries passed `complete=true`,
`full_result_validation=true`, `zero_answer_cache_verified=true`, `fallback_attempted=false`, and
`external_engine_invoked=false` across `8` TPC-H + `6` SSB queries:

- **`cold_process_cli` total engine time**: `0.093602836s` (`control`) -> `0.089224004s`
  (`profile-use`), **`-4.68%`** overall, returning **`retain`** under the active
  `binary_complete_cohort` policy in [evaluate_perf_ship_drop.py](../../scripts/evaluate_perf_ship_drop.py)
  (peak RSS `-23.98%`, minor faults `-21.32%`, system CPU `+8.28% <= 10.00%`, major faults `0`).
  Under the preserved historical `perf01_thresholds` check it records **`drop`** solely because Q1
  regressed `+27.74% > max 5.00%` (`0.003865958s -> 0.004938375s`); Q2–Q13 improved (`-2.07%` to
  `-23.47%`) and Q14 changed `+1.56%`.
- **`warm_resident_session` total engine time**: `0.077699875s` (`control`) -> `0.073529207s`
  (`profile-use`), **`-5.37%`** overall (`max_query_regression_pct = +1.52%` on Q14; Q1–Q13 improved
  `-0.52%` to `-28.61%`). Under the hardened OS-resource gate in
  [evaluate_perf_ship_drop.py](../../scripts/evaluate_perf_ship_drop.py), this `78e4a1cd7` warm summary
  reconciles to **`blocked`** because it was recorded prior to
  [run_tpch_ssb_suite.py](../../scripts/run_tpch_ssb_suite.py) capturing `os.wait4`
  `worker_resource_usage` (before OS-resource enforcement it recorded `retain`).

### Per-query engine time (`engine_execution_seconds`)

| Query | Suite | Cold Control (s) | Cold Profile-Use (s) | Cold Delta | Warm Control (s) | Warm Profile-Use (s) | Warm Delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | TPC-H | `0.003865958` | `0.004938375` | `+27.74%` | `0.002773583` | `0.001987749` | `-28.33%` |
| Q2 | TPC-H | `0.004579000` | `0.003924335` | `-14.30%` | `0.003586417` | `0.003368583` | `-6.07%` |
| Q3 | TPC-H | `0.002631417` | `0.002020626` | `-23.21%` | `0.001879791` | `0.001442499` | `-23.26%` |
| Q4 | TPC-H | `0.008829042` | `0.008042375` | `-8.91%` | `0.007685043` | `0.006997709` | `-8.94%` |
| Q5 | TPC-H | `0.001115417` | `0.000984000` | `-11.78%` | `0.000468334` | `0.000460083` | `-1.76%` |
| Q6 | TPC-H | `0.005816959` | `0.005381708` | `-7.48%` | `0.004485918` | `0.004371917` | `-2.54%` |
| Q7 | TPC-H | `0.002825751` | `0.002425167` | `-14.18%` | `0.001994499` | `0.001718042` | `-13.86%` |
| Q8 | TPC-H | `0.007001582` | `0.006648376` | `-5.04%` | `0.005320417` | `0.005276167` | `-0.83%` |
| Q9 | SSB | `0.001961875` | `0.001722583` | `-12.20%` | `0.001493584` | `0.001335124` | `-10.61%` |
| Q10 | SSB | `0.001805292` | `0.001381540` | `-23.47%` | `0.000722583` | `0.000515833` | `-28.61%` |
| Q11 | SSB | `0.003809458` | `0.003033167` | `-20.38%` | `0.003371667` | `0.002599167` | `-22.91%` |
| Q12 | SSB | `0.004069082` | `0.003541000` | `-12.98%` | `0.003400748` | `0.002741709` | `-19.38%` |
| Q13 | SSB | `0.022531835` | `0.022066127` | `-2.07%` | `0.020588833` | `0.020482541` | `-0.52%` |
| Q14 | SSB | `0.022760168` | `0.023114625` | `+1.56%` | `0.019928458` | `0.020232084` | `+1.52%` |
| **Total (14/14)** | **All** | **`0.093602836`** | **`0.089224004`** | **`-4.68%`** | **`0.077699875`** | **`0.073529207`** | **`-5.37%`** |

## Held-out 19-case native operator suite (`rows=8192`, `workers=1,4`, `samples=2`)

All `38/38` matrix cells (`19` cases × `2` worker settings, including explicit checked overflow
diagnostic `checked_signed_group_overflow`) passed exact typed parity (`status="passed"`,
`all_requested_acceptance_cases_passed=true`). Note that `run_heldout_operator_uat.py` records
supervised fresh-process CLI duration (`seconds`, not `engine_execution_seconds`), so median paired
baseline/profile-use ratios (`35/36` timed cells `> 1.0x`, `0.9496x`–`1.1396x`) are reported solely
as process-level diagnostics below.

## Diagnostic process, build, and held-out operator tables (`not engine time`)

### TPC-H/SSB evaluation OS resources & process walls

| Variant / Lane | Process wall (s) | Supervised wall (s) | User CPU (s) | System CPU (s) | Max Peak RSS (B) | Minor / Major faults |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `control` — cold (`14` spawns) | `0.252574` | `0.878763` | `0.186918` | `0.044767` | `33,619,968` | `30,959` / `0` |
| `profile-use` — cold (`14` spawns) | `0.707504` | `1.339734` | `0.176973` | `0.048472` | `25,559,040` | `24,357` / `0` |
| `control` — warm (`1` resident) | `0.150279` | — | — | — | — | — |
| `profile-use` — warm (`1` resident) | `0.134872` | — | — | — | — | — |

Cold `profile-use` delta vs `control`: Peak RSS `-23.98%`, minor faults `-21.32%`, system CPU
`+8.28%`, major faults `0`.

### Held-out operator process p50 diagnostic summary (`rows=8192`, `seconds`, not engine time)

| Metric slice | Range across `36` timed `(case, workers)` cells |
| --- | --- |
| Complete cells | `38/38` (`36` timed value cells + `2` overflow diagnostic cells) |
| Cells with `median_paired_ratio > 1.0` | `35/36` (below `1.0`: `metadata_count.workers_1` `0.9912x`, `numeric_group_sum.workers_4` `0.9496x`) |
| Highest `median_paired_ratio` | `1.1396x` (`filtered_sort_offset.workers_4`: `0.009670s -> 0.008484s`) |
| Lowest timed `profile-use` p50 | `0.006732s` (`metadata_count.workers_4`, ratio `1.0820x`) |

### PGO orchestration step durations

`build-control`: `349.85s`; `build-instrumented`: `424.05s`; `training`: `11.83s`; `merge`: `0.63s`;
`hot-symbol-coverage`: `0.35s`; `build-profile-use`: `314.06s`; `eval-tpch-ssb-control`: `2.56s`;
`eval-tpch-ssb-profile-use`: `1.86s`; `eval-heldout-operators`: `14.14s`.

## Harness disclosures and open external-dataset gates

- **Retained-CTE metric accounting disclosure**: A subsequent fix to `run_owned_source_pushdown`
  (`shardloom-vortex/src/local_primitive_relational_scan.rs`) stops counting materialized CTE
  replay rows in base `scan_rows_delivered` telemetry; zero queries in the TPC-H/SSB or held-out
  operator evaluation suites execute `Plan::Materialized` CTE replay, leaving all recorded engine
  timings and verdicts unaffected.
- **Prior screen diagnostics**: An initial run (`screen-20261124` at `3931f6473`) failed during
  training because `run_heldout_operator_uat.py` still queried `length(category_text)` against a
  UTF-8 byte oracle after SQL length unification (`octet_length` for UTF-8 bytes) and expected
  pre-unification overflow text, and `pgo_local_experiment.py` passed `--all-functions --covered` to
  `llvm-profdata show --topn=250`, which suppresses header summary lines in LLVM 22 (`d986b45bc`
  aligned those flags). A second run (`screen-20261124-clean` at `d986b45bc`) aborted during
  `build-profile-use` with `[Errno 1] Operation not permitted` caused by a `FileNotFoundError` TOCTOU
  on transient `rustc` `rmeta*` dirs during storage polling followed by macOS `os.killpg` `EPERM` on a
  zombie process group in `stop_group` (`78e4a1cd7` fixed `stop_group`, and the bounded
  `FileNotFoundError` retry in `pgo_local_experiment.py`'s `Runner.guard` preserves
  `scripts/local_uat_storage.py`'s frozen historical replay hash).
- **Disposition & open external gates**: Under `binary_complete_cohort`,
  `cold_process_cli` (`-4.68%` engine time) passes all correctness, no-fallback, OS-resource, and
  complete-cohort engine-time gates (**`retain`**). The `78e4a1cd7` `warm_resident_session`
  summary (`-5.37%` engine time) reconciles to **`blocked`** under the hardened OS-resource gate in
  [evaluate_perf_ship_drop.py](../../scripts/evaluate_perf_ship_drop.py) because it preceded
  `os.wait4` `worker_resource_usage` collection in
  [run_tpch_ssb_suite.py](../../scripts/run_tpch_ssb_suite.py) (prior to OS-resource enforcement it
  recorded `retain`). Default `[profile.release]` remains unchanged
  (`default_release_profile_changed=false`) and `E2E-PGO-01` remains open pending external `100M`
  ClickBench Full43 cold/warm and `100M` ingest PGO ship/drop screens once those datasets are
  mounted.
