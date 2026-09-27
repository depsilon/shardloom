# Compiler profile screens — C5.a/C5.b/C5.c

Status: retain portable ThinLTO for ordinary release builds; final ordinary-profile
artifact acceptance is in progress. These are PERF-INTAKE / RFC 0044 compiler
screens over the retained Vortex 0.85.0 provider and unchanged native runtime.
The C1 provider upgrade remains dropped at resource admission.

1. C5.a compares the existing `release-lto` profile (ThinLTO, one codegen unit)
   with ordinary `release` on the final C2 runtime.
2. C5.b compares trained `release-pgo` with the selected portable control. Use the
   guarded instrument/train/merge/use helper, matching Rust/LLVM tools, and a
   declared training corpus covering native ingest, queries and pressure. Retain
   separate untrained values/distributions for evaluation; training timing is
   not an evaluation result.
3. C5.c compares the existing `release-native-benchmark` with an explicit native
   CPU target against the selected portable control. Record the resolved CPU,
   architecture and flags. A local CPU-specific artifact is not a portable
   distribution binary.

Freeze each binary with source/content identity, dependency lock, feature set,
build command, toolchain, flags, build duration and size. Keep generated outputs
under the local-only Cargo/UAT roots with disk, log and child-cleanup guards.
Run local builds and measurements sequentially. Do not change source during a
build. Existing runtime tests remain applicable to unchanged source; every
candidate must independently validate complete results through its actual binary.

Use symmetric best-valid comparisons with every observation preserved. A broad
retained build needs complete paired Full43, guarded native ingest and disjoint
held-out acceptance, including resource/error behavior. Stage a bounded screen
first when it can reject a consistently slower variant without more bulk work.
No percentage cutoff discards a useful gain. Any default-profile or packaging
change requires its own final artifact verification; the selected ordinary release
profile change is undergoing that verification before merge.

These are compiler experiments over ShardLoom-native execution, with explicit
no-fallback evidence. They do not introduce JIT execution, a new provider or a
new operator family. Failed candidates leave the retained portable build intact.
PERF-13 and the broader CG-5/CG-6 obligations remain open beyond these screens.

## Completed screen decisions

Each row has its own matched control. Query totals sum each query's best of three
complete CLI calls; ingest compares the best of two complete calls for each role.
All observations, including slower samples, remain in the evidence. Host concurrency
is accepted context and does not invalidate a faster valid observation.

| Screen | Query control → candidate | Ingest control → candidate | Decision |
| --- | --- | --- | --- |
| C5.a portable ThinLTO | 68.150982 → 65.966738 s; 3.21% lower | 87.708583 → 81.104577 s; 7.53% lower | Retain; make ThinLTO and one codegen unit the ordinary release configuration. |
| C5.b tested PGO corpus/profile | 70.028088 → 69.832891 s; 0.28% lower | 95.380974 → 103.386866 s; 8.39% higher | Drop promotion of this configuration; preserve existing explicit PGO tooling and its measured query/size gains. |
| C5.c native CPU targeting | 71.837888 → 71.231105 s; 0.84% lower | 93.130639 → 94.855549 s; 1.85% higher | Preserve the explicit benchmark profile; drop automatic selection of this tested configuration. It remains nonportable. |

These are workload tradeoffs, not a percentage floor. PGO and native CPU targeting
save less query time than they add to ingest in these comparisons. Their small
query gains remain recorded and available for workload-specific review. This does
not establish that every PGO corpus or CPU target is unprofitable.

The default change moves `lto = "thin"` and `codegen-units = 1` into
`[profile.release]`. `release-lto` remains a compatible alias; PGO and native CPU
profiles retain their explicit identities. Benchmark metadata follows Cargo
inheritance. CI builds an ordinary portable release binary and selects it for
example replay. No host-specific CPU setting or trained profile becomes default.

## Build identity and cost

All screen binaries share 493 tracked Rust/Cargo source hashes from the final C2
runtime, Rust 1.98.0 / LLVM 22.1.8, the same lockfile and `release-user-surfaces`.
The ThinLTO pair was frozen at `8a492c945484`; PGO/native CPU builds use later
documentation-only commit `2b52ba7350b2`. Receipts retain exact identities and flags.

| Artifact | Binary bytes | Cached build seconds |
| --- | ---: | ---: |
| Ordinary release control before admission | 85,339,648 | 111.628 |
| Portable ThinLTO | 73,353,184 | 239.047 |
| PGO matched ThinLTO control, explicit target triple | 73,345,872 | 310.258 |
| PGO instrumented | 142,444,624 | 371.472 |
| PGO profile-use | 65,362,960 | 260.468 |
| Native CPU target | 73,491,696 | 326.667 |

ThinLTO reduces binary size 14.05%; PGO profile-use reduces its matched control
10.88%. Build durations include existing caches and are not cold-build comparisons.
The Apple M5 host resolves `target-cpu=native` to `apple-m4` in this compiler, adding
recorded `bf16`, `bti` and `i8mm` features. This does not certify another machine.

## Exact-result, ingest and pressure acceptance

Each screen passes all 258 paired Full43 calls against complete retained results.
ThinLTO has no query crossing both the 10% and 150 ms slowdown review flags. PGO
flags Q3 (+0.195 s), Q9 (+0.211 s), Q31 (+0.278 s) and Q32 (+0.325 s); native CPU
flags Q15 (+0.203 s) and Q27 (+0.318 s). Flags are review aids, not gain cutoffs.
Full43 references are regression references, not independent oracles.

Four full-size ingests per screen run control/candidate/candidate/control through
the official storage/concurrency/cleanup guards. Native process seconds are:

| Screen | Control 1 | Candidate 1 | Candidate 2 | Control 2 |
| --- | ---: | ---: | ---: | ---: |
| ThinLTO | 90.347985 | 87.077741 | 81.104577 | 87.708583 |
| PGO | 95.380974 | 104.405196 | 103.386866 | 106.648955 |
| Native CPU | 95.525486 | 101.631798 | 94.855549 | 93.130639 |

Every output is exactly 15,682,956,116 bytes with SHA-256
`31cc61cfc347cf19a0328c196d59cd1eb431679311294cdc92263fef31062b35`, matching the
retained native artifact. Duplicates were removed only after complete hash and
stable generation checks. Hash/cleanup time is outside the native ingest clock.
Receipts prove byte identity; they do not add a duplicate-row counter.

Each screen passes 456 held-out calls: 19 independently specified exact-result or
checked-overflow cases, workers 1/4/12, a warmup and three samples per role. This
completes the declared matrix; default worker settings 2/8 were not included.
Ten pressure calls per screen cover native Vortex fixture creation, side-effect-free
route inspection, exact SQL/DataFrame values, actual spill/merge, quota rejection
and empty owned workspaces. Successful envelopes report no external engine. Typed
quota errors report no fallback but lack a separate external-engine field.

## PGO training and reopening

Training uses a 16,384-row renamed-schema fixture, 152 native operator calls,
native Parquet export/re-ingest plus 19 query oracles, and a 500,000-row pressure
fixture. Training and profile merge take 11.900 and 0.367 seconds. Evaluation uses
full ClickBench, separate 8,191-row operator data and 450,001-row pressure data.
No complete held-out row overlaps training after normalizing names; scalar values
and signed-boundary sentinels do overlap. ClickBench is never training input.
Training timing is not performance evaluation.

Training exercises direct scalar/count/sum/distinct and dictionary-count paths.
Full43 additionally selects complete-pair distinct, pair preunion, heavy-hitter
first passes, late measures, transformed dictionary partials and numeric-minute-string
updates. The strategy audit records those coverage differences; it does not diagnose
timing changes. Profile-use emits 2,110 missing-function-profile warnings across
five crates and exits successfully. Reopening requires declared training coverage
of selected native paths/types and separately held-out evaluation, not silent
training on these evaluation results.

The first PGO attempt stops with an OS permission error in its control build;
no candidate is produced. A traced retry completes with unchanged guards; the
initial cause is not established. The interrupted first ThinLTO build and preliminary
pressure/export admission failures remain preserved alongside successful attempts.

## Final artifact gate

Local receipts and reproduction scripts are under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926/c5-*`.
Complete envelopes, typed oracle checks, identities and ingest receipts are being
bundled for the PR. Large payloads, executable caches and profile blobs remain local,
with hashes and generators retained. The changed ordinary release profile requires
its own frozen binary, complete Full43, guarded ingest, held-out/resource acceptance,
workspace checks and independent review before merge. Screen evidence does not
substitute for that final artifact verification. No package publication or version
bump occurs. C7 JSON/JSONL ingestion is next after this compiler work is merged.
