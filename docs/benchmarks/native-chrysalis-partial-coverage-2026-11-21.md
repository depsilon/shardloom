<!-- SPDX-License-Identifier: Apache-2.0 -->

# Chrysalis exact partial native source coverage

The bounded resident partial native source coverage implementation ([RFC 0045](../rfcs/0045-partial-native-source-coverage.md))
is **locally retained** after passing all correctness, resource, ownership, and
isolated installed-package checks and completing the frozen 18-request prospective
three-role comparison once sequentially per role (`150,509,625 → 149,794,248` engine
nanoseconds, **-0.48%**).

This is a prospective matched single-execution comparison against the accepted November 18
baseline build; hosted CI acceptance remains pending. Gain on overlapping queries
(`-5,616,584 ns` combined across `numeric-overlap` and `text-overlap`) is nearly offset
by overhead across all eight non-reusing control groups (`+4,901,207 ns`), leaving a small
net engine difference (`-715,377 ns`) within normal run-to-run noise. No large speedup,
repeatability, production readiness, or competitive-gate closure is claimed.

## Behavior and boundary

Chrysalis retains completed native column fragments for an immutable local Parquet source
generation inside the owning `ResidentVortexSession`, delegated through the session's
single optional-memory reclaimer (`source_segments`). Covered integer, float (preserving
raw subnormal, signed-zero, and NaN bit payloads under `FloatPolicy::Extended`), and UTF-8
columns (plus all-null constant arrays) are keyed by `(source_path, FileGeneration,
row_group_index, first_row, rows, field_index, dtype, session_float_policy)`.

First-use realization decodes only demanded missing columns, validates complete natural
row-group extent and held generation identity, and publishes atomically only upon
enclosing-operation completion (`complete_segment_reuse()`). Subsequent operations within
the same serialized session combine retained columns with freshly decoded missing columns
by slicing and recomposing exact original coordinates (`StructArray` / `ChunkedArray`).
Unsupported types, directory/table inputs, concurrent serving sessions, prefix/filtered
incomplete extents, >128 records, >128 fragments, or grants smaller than the ceiling fall
back cleanly to the direct native Parquet reader without error or silent external fallback.
On Python `Session::close()`, lifecycle cancellation is released first and non-blocking
optional cache reclamation releases unreachable coverage credits while leaving independently
retained result leases intact. Bounded retention remains capped at `min(32 MiB, grant / 16)`.

## Frozen three-role comparison

The population was frozen in `exposure-manifest-r2.json` before candidate measurement:
18 literal SQL requests across two immutable local Parquet files (`numeric.parquet` and
`text.parquet`, 131,072 rows across 8 row groups each) partitioned into five fresh-session
workflow groups per file (`overlap`, `single`, `disjoint`, `full`, `empty`), under a
512-MiB (`536,870,912`-byte) memory limit and 1 CPU lane on macOS `arm64` (Rust 1.99.0,
Python 3.13.13). Each role ran once sequentially under guarded process/storage isolation
with `subprocess.Popen` patched to fail, comparing identical result values and schemas:

| Role | Engine ns (`native_query_execution_nanos`) | Public query ns | Explicit prep ns | Complete workflow ns | Artifacts written |
| --- | ---: | ---: | ---: | ---: | ---: |
| Direct (Nov 18 baseline) | `150,509,625` | `169,037,209` | `0` | `282,764,791` | `0` |
| Full preparation + query | `95,469,499` | `108,418,208` | `985,052,876` | `1,171,740,248` | `10` `.vortex` |
| Partial (Chrysalis candidate) | `149,794,248` | `170,560,665` | `0` | `245,960,709` | `0` |

Note on clocks: `full-preparation` writes and atomically commits ten complete single-file
`.vortex` artifacts (`985,052,876 ns`); its query sum alone (`95,469,499 ns`) is never a
complete workflow cost. `partial` vs `direct` compares the exact `engine_nanos` sum
defined in `comparison-protocol-r1.md`, with source publication verified by source
inspection (`PreparedNativeOperation::execute_native_with_inputs` stops after
`ResidentVortexSession::with_sources_execution` calls `complete_segment_reuse()`).

### Per-group engine comparison and work avoided

| Workflow group | Direct engine ns | Partial engine ns | Engine delta ns | Full-prep query ns | Full-prep explicit ns |
| --- | ---: | ---: | ---: | ---: | ---: |
| `numeric` / `overlap` (4 queries) | `16,194,250` | `12,276,124` | `-3,918,126` (`-24.19%`) | `10,909,041` | `49,342,459` |
| `numeric` / `single` (1 query) | `2,976,500` | `3,254,833` | `+278,333` | `2,276,958` | `45,765,542` |
| `numeric` / `disjoint` (2 queries) | `6,417,292` | `6,981,666` | `+564,374` | `4,645,708` | `44,586,708` |
| `numeric` / `full` (1 query) | `11,819,500` | `12,939,583` | `+1,120,083` | `9,294,125` | `44,583,916` |
| `numeric` / `empty` (1 query) | `38,708` | `46,958` | `+8,250` | `154,542` | `45,377,333` |
| `text` / `overlap` (4 queries) | `54,002,292` | `52,303,834` | `-1,698,458` (`-3.15%`) | `32,814,959` | `152,223,209` |
| `text` / `single` (1 query) | `11,992,125` | `12,471,375` | `+479,250` | `7,092,958` | `150,050,209` |
| `text` / `disjoint` (2 queries) | `23,927,625` | `25,170,584` | `+1,242,959` | `14,100,625` | `151,039,708` |
| `text` / `full` (1 query) | `23,111,625` | `24,305,250` | `+1,193,625` | `14,119,250` | `150,822,459` |
| `text` / `empty` (1 query) | `29,708` | `44,041` | `+14,333` | `61,333` | `151,261,333` |

Work-avoidance and honest boundary observations:

- **`numeric-overlap`**: Selected Parquet reads dropped from `3,172,290` bytes (direct) to
  `1,420,709` bytes (partial). On `numeric-overlap-3` (all columns covered across prior
  queries), `native_source_demand_rows_decoded` dropped to `0`, `native_source_demand_data_read_bytes`
  dropped to `0`, `native_source_coverage_field_rows` reached `262,144`, and engine time
  dropped from `3,502,208` to `1,505,208` ns (`-57.02%`).
- **`text-overlap`**: Expanded UTF-8 columns approach the fixed `32 MiB` ceiling
  (`native_source_coverage_retained_bytes` reached `32,947,336` bytes on query 1 and
  `32,947,293` bytes on query 2), forcing FIFO eviction of earlier text chunks and partial
  re-decoding (`637,925 → 424,287` total read bytes across the four queries; `-3.15%` engine time).
- **Control overhead**: Every non-reusing control (`single`, `disjoint`, `full`, `empty`)
  incurred modest bookkeeping/retention overhead (`+4,901,207 ns` across the eight control
  groups), and `public_query_terminal_nanos` increased slightly overall (`169,037,209 →
  170,560,665 ns`, `+0.90%`). Preserved unchanged controls remain part of the retention gate.

## Validation and portable evidence

All 15 local validation checks pass against frozen candidate sources without errors or skips:

- Formatting and strict lint: `cargo fmt --all -- --check`, workspace default `clippy`, and
  native `shardloom-cli` / `shardloom-vortex` `--features release-user-surfaces` `clippy` pass.
- Rust suites: default workspace (`3,351` passed across `108` targets), checked `ci-native`
  `shardloom-vortex` (`4,449` passed, `42` declared ignored across `17` targets; `src/lib.rs`
  inventory discovers `4,474` unit tests including all `17` `columnar_coverage_` tests across
  `4` partitions), and public CLI tests (`1,257` passed across `8` targets).
- Feature matrix and MSRV: `--all-features`, `--no-default-features`, and actual `rustc 1.96.0`
  minimal and native surface checks pass.
- Release artifacts and isolated Python wheel: candidate `cli-release` and `native-extension`
  release builds, focused Python tests (`3/3` passed), and `scripts/prove_native_python_wheel.py`
  (`468/468` isolated wheel tests passed with `0` failures, `0` errors, and `0` skips, plus
  clean uninstall verification) pass.

All five preserved failures (`exposure-r1`, `coverage-native-full-r1`, `coverage-native-cli-lint-r3`,
`coverage-public-cli-r3`, and `coverage-installed-wheel-r1`) plus pre-fix snapshots are
retained verbatim in the checked-in [portable evidence packet](../release/evidence/chrysalis-partial-coverage-2026-11-21/README.md)
alongside relocated standalone replay (`python3 -I -B`) and four rejected corruption controls.
`fallback_attempted=false`, `external_engine_invoked=false`, `publication_attempted=false`, and
`claim_gate_status=not_claim_grade` remain preserved.
