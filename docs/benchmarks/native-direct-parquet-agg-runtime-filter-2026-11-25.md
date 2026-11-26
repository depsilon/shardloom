<!-- SPDX-License-Identifier: Apache-2.0 -->

# Direct Parquet aggregation and early runtime join filtering (`2026-11-25`)

The bounded direct-Parquet aggregation and early join-filter follow-on is
**locally retained** against `dd7b441998430aebbcf61bbb3879cc54c339d0af`. All
14 frozen cohort queries pass independent oracle checks in both cold-process
CLI and warm-resident modes with `fallback_attempted = false` and strictly lower
complete-cohort `engine_execution_seconds`:

- **Cold CLI complete cohort (`14/14` passed):** `1.933791543s -> 0.949083623s`
  (**`-0.984708s` / `-50.92%`**)
- **Warm resident complete cohort (`14/14` passed):**
  `1.915099168s -> 0.921524043s` (**`-0.993575s` / `-51.88%`**)
- **Python embedded smoke (`3/3` passed):**
  - `PY_Q01_parquet_sql` (`256` rows): `0.106412s -> 0.055170s`
    (`106,411,833 ns -> 55,169,666 ns`, `-48.15%`, `5,000,000` rows decoded)
  - `PY_Q06_join_agg` (`4` rows): `0.225482s -> 0.060613s`
    (`225,482,208 ns -> 60,613,375 ns`, `-73.12%`, `5,010,000 -> 66,000` rows
    decoded)
  - `PY_DF_small`: `4,096` rows passed

Per `AGENTS.md` and the [benchmarking policy](../skills/benchmarking.md#timing-communication-decision),
headline and per-query comparisons below report **engine execution time only**
(`engine_execution_seconds`). This local deterministic cohort does not close
CG-5/6/7/8/14/16/19/20/21, reopen `Full43` / `full10k` / November 16 retired
campaigns, or establish competitive superiority.

## Cohort totals (`engine_execution_seconds`)

Baseline runtime artifacts were built at `dd7b441998430aebbcf61bbb3879cc54c339d0af`
(`cli` SHA prefix `1bcb53de537c`, `extension` SHA prefix `77f7e4f5d9da`);
candidate artifacts were built identically with `--features release-user-surfaces`
(`cli` SHA prefix `ba77b738ceb3`) and `--features python-native` (`extension` SHA
prefix `a965c94fcf6c`) under `--memory-gb 2 --max-parallelism 4`.

| Lane | Group | Baseline (s) | Candidate (s) | Delta (s) | Change |
| --- | --- | ---: | ---: | ---: | ---: |
| Cold CLI | Complete cohort (`Q01..Q14`) | `1.933792` | `0.949084` | `-0.984708` | `-50.92%` |
| Cold CLI | Track A (`Q01..Q05`) | `1.071234` | `0.535802` | `-0.535432` | `-49.98%` |
| Cold CLI | Track B (`Q06..Q11`) | `0.792416` | `0.326185` | `-0.466231` | `-58.84%` |
| Cold CLI | Controls (`Q12..Q14`) | `0.070142` | `0.087096` | `+0.016954` | `+24.17%` |
| Warm resident | Complete cohort (`Q01..Q14`) | `1.915099` | `0.921524` | `-0.993575` | `-51.88%` |
| Warm resident | Track A (`Q01..Q05`) | `1.069207` | `0.532726` | `-0.536481` | `-50.18%` |
| Warm resident | Track B (`Q06..Q11`) | `0.777345` | `0.308044` | `-0.469301` | `-60.37%` |
| Warm resident | Controls (`Q12..Q14`) | `0.068546` | `0.080754` | `+0.012208` | `+17.81%` |

## Per-query `engine_execution_seconds` and decoded-row reduction

| Code | Cohort role | Cold base (s) | Cold cand (s) | Cold change | Warm base (s) | Warm cand (s) | Warm change | Cold decoded rows (`base -> cand`) | Candidate mechanics observed |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `Q01` | `track_a_int_group` | `0.103983` | `0.064709` | `-37.77%` | `0.104248` | `0.048393` | `-53.58%` | `5,000,000 -> 5,000,000` | `w=4`, `states=8` |
| `Q02` | `track_a_compound_group` | `0.423343` | `0.183720` | `-56.60%` | `0.422199` | `0.188241` | `-55.41%` | `5,000,000 -> 5,000,000` | `w=4`, `states=8` |
| `Q03` | `track_a_ungrouped` | `0.050316` | `0.025413` | `-49.49%` | `0.049685` | `0.025007` | `-49.67%` | `5,000,000 -> 5,000,000` | `w=4`, `states=8` |
| `Q04` | `track_a_group_distinct` | `0.397642` | `0.174394` | `-56.14%` | `0.397044` | `0.177690` | `-55.25%` | `5,000,000 -> 5,000,000` | `w=4`, `states=8` |
| `Q05` | `track_a_float_decline` | `0.095950` | `0.087565` | `-8.74%` | `0.096031` | `0.093395` | `-2.75%` | `5,000,000 -> 5,000,000` | `w=4`, `states=0` (Float decline -> A1 ordered) |
| `Q06` | `track_b_join_agg_bloom_1pct` | `0.221978` | `0.066685` | `-69.96%` | `0.219417` | `0.059544` | `-72.86%` | `5,010,000 -> 66,000` | `w=4`, `kf=5,000,000 -> 56,000`, `payload=56,000` |
| `Q07` | `track_b_join_agg_pred_tiny` | `0.149264` | `0.050809` | `-65.96%` | `0.146104` | `0.049745` | `-65.95%` | `5,010,000 -> 14,000` | `w=4`, `kf=5,000,000 -> 4,000`, `payload=4,000` |
| `Q08` | `track_b_ordinary_join_selective` | `0.207887` | `0.068660` | `-66.97%` | `0.203433` | `0.063669` | `-68.70%` | `5,010,000 -> 14,000` | `w=4`, `kf=5,000,000 -> 4,000`, `payload=4,000` |
| `Q09` | `track_b_empty_build_join_agg` | `0.000180` | `0.000203` | `+12.53%` | `0.000127` | `0.000157` | `+23.37%` | `0 -> 0` | Empty build short-circuit preserved |
| `Q10` | `track_b_dup_join_agg_fallback` | `0.157652` | `0.100065` | `-36.53%` | `0.154444` | `0.097813` | `-36.67%` | `5,000,200 -> 25,200` | `w=4`, `kf=5,000,000 -> 25,000`, `payload=25,000` |
| `Q11` | `track_b_unfav_90pct_ctrl` | `0.055454` | `0.039764` | `-28.29%` | `0.053820` | `0.037115` | `-31.04%` | `1,010,000 -> 899,300` | `w=4`, `kf=1,000,000 -> 889,300`, `payload=889,300` |
| `Q12` | `control_vortex_join_agg` | `0.068049` | `0.084058` | `+23.53%` | `0.066982` | `0.079214` | `+18.26%` | `N/A` (`.vortex` control) | Ordinary `.vortex` control |
| `Q13` | `control_small_parquet` | `0.000593` | `0.000744` | `+25.53%` | `0.000365` | `0.000346` | `-5.24%` | `4,096 -> 4,096` | Single-RG `w=0` decline preserved |
| `Q14` | `control_arrow_ipc` | `0.001500` | `0.002294` | `+53.01%` | `0.001200` | `0.001195` | `-0.41%` | `50,000 -> 50,000` | Non-Parquet Arrow IPC control |

Notes on cohort observations:

- In `cohort_harness.py`, `Q06..Q10` use multi-aggregate / join projections where
  `join_aggregate` is not selected (`direct_aggregate.chains_considered = 0`), so
  their Track B speedup (`-36.53%` to `-72.86%`) comes directly from Stage-1
  key-first early Parquet filtering (`key_filter_rows_decoded` `5,000,000` down
  to `4,000`–`56,000` payload rows decoded) composed across `4` parallel
  workers (`w=4`). Separate focused tests in
  `public_parquet_parallel_aggregate_and_early_join_filter_evidence` (`shardloom-cli`),
  `test_native_parquet_agg_and_join_filter.py`, and
  `test_native_join_aggregate_binding.py` verify `direct_aggregate.probe_filters_pushed`
  (`>= 1` and `2` on star joins) and payload reduction across SQL, `LazyFrame`,
  `Session`/`DataFrame`, and typed `native.Relation` / `native.Session`.
- `Q05` declines Track A2 (`persistent_aggregate_states = 0`) for floating-point
  `SUM`, preserving exact ordered reduction while still benefiting slightly from
  Track A1 ordered parallel range decode (`-8.74%` cold / `-2.75%` warm).
- Controls `Q12..Q14` add `+16.95ms` cold / `+12.21ms` warm inside single-pass
  measurement variance against `-984.71ms` cold / `-993.58ms` warm cohort gains.
- See [the architecture design note](../architecture/native-direct-parquet-agg-runtime-filter-2026-11-25.md)
  for exact operator contracts and decline boundaries.

## Validation & wheel proof

- Rust formatting & strict lint: `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo clippy -p shardloom-cli -p shardloom-vortex --all-targets --features release-user-surfaces -- -D warnings` passed.
- Rust test suites: `cargo test --workspace --all-targets` passed;
  `cargo test --locked --profile ci-native -p shardloom-vortex --lib --features release-user-surfaces`
  passed `4,565` tests (`0` failed; `42` explicit pre-existing ignored);
  `cargo test --locked -p shardloom-cli --lib --features release-user-surfaces format_demand_tests`
  passed `10` tests (`0` failed).
- Isolated native Python wheel proof (`prove_native_python_wheel.py` on
  `shardloom-1.3.0-cp310-abi3-macosx_27_0_arm64.whl`, SHA prefix `daff11ab1e3e`):
  passed all `493` integration tests (`0` failures, `0` errors, `0` skipped)
  and clean uninstall verification (`uninstall_status = "passed"`).

## Diagnostic observations (non-engine runtime evidence)

- Process wall-clock sums: cold CLI `2.115396s -> 1.189271s`; warm resident
  `1.959683s -> 0.967563s`.
- Worker process peak RSS & CPU: cold baseline `74,383,360 B` / candidate
  `73,662,464 B` (`user_cpu_seconds`: `1.954882 -> 1.973678`,
  `system_cpu_seconds`: `0.082863 -> 0.108779`); warm baseline `85,098,496 B` /
  candidate `88,293,376 B` (`user_cpu_seconds`: `1.913578 -> 1.968855`,
  `system_cpu_seconds`: `0.047063 -> 0.046247`).
- Host note: an unrelated background web dev server (`vite`, `PID 84708`) was
  present on the host at ~`121%` CPU during the candidate pass; because complete-cohort
  `engine_execution_seconds` improved cleanly (`-50.92%` cold / `-51.88%` warm)
  with all `14/14` oracle assertions and `3/3` Python smoke checks passing, no
  re-run or artificial host intervention was performed.
- Non-synced working artifacts (`fixtures/`, `baseline-runtime/`,
  `candidate-runtime/`, `evidence/`, `venv/`) are retained outside synced
  folders under `/Users/dylan/LocalData/shardloom/direct-parquet-agg-join-filter-2026-11-25/`;
  transient build caches and temporary wheel-proof venvs were retired (`138 GiB`
  free on `/Users/dylan/LocalData`).
