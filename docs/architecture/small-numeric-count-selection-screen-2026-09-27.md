# Small-input numeric COUNT selection — C2.b

Decision: retain bounded selection of the existing direct native aggregate path
before worker admission. This closes the C2.b screen under PERF-INTAKE / RFC 0044;
it does not introduce a general cost model, new aggregation implementation or
persistent cache. No minimum percentage or seconds cutoff applies.

## Selection contract

The selector uses complete source metadata and already-lowered request facts.
It admits 1–32,768 source rows, one non-null I32/I64/U64 identity grouping key,
COUNT(*) ordered descending, no predicate or HAVING, and a checked OFFSET + LIMIT
between 1 and 128. It requires the existing numeric-state admission proof,
at least two requested CPU slots, a memory budget of at least 32 MiB, and enough
group-state item capacity for every source row. Filtered, derived, nullable,
compound, larger and below-floor budget requests retain their existing admission paths.

Selection happens before input. Declining aggregate workers restores the native
provider drivers on the same runtime/source; it neither reopens nor replays the
source. An admitted execution failure does not trigger the alternate choice.
Results report `aggregate_worker_selection=small_numeric_count_direct`, the
source row count and the row bound. Existing execution and resource evidence
remains available. SQL/CLI/Python requests that reach this shared native family
receive the same rule; no frontend-specific selector is added.

Vortex-first classification: `use_vortex_native_provider`. The change reuses the
existing native source, typed aggregate states and provider-driver restoration.
Vortex-native input/output and deterministic no-fallback behavior remain intact.

## Complete-call measurements

Three accepted cohorts contain 960 complete public native calls in frozen release
test binaries. Each fixture uses the same source and exact request for every
role, five repetitions, and alternating role order. The independent ordered-map
oracle checks every result value and key tie. Every observation is retained.
The renamed `alias_key` request groups COUNT(*) descending with LIMIT 10, a
1 GiB policy and twelve requested CPU slots. Native flat chunks hold at most
65,536 rows; the encoded cases are persisted separately in the same Vortex format.

| Cohort | Calls | Scope and outcome |
| --- | ---: | --- |
| Initial crossover | 180 | Six sizes, 128–1,048,576 rows, skewed/uniform/nearly unique keys; direct execution wins at the small sizes and loses on larger high-cardinality inputs. |
| Held-out sizes and encodings | 390 | Seven sizes, 1–32,768 rows, plus packed, dictionary, constant and signed-extreme sources. The initial 8,192-row selector improves best complete calls by 34.41–66.93% within its admitted cells. |
| Extended boundary | 390 | Seven sizes, 16,381–65,536 rows, plus the five encoded/signed cases. The final 32,768-row selector improves best complete calls by 0.62–62.92% within its admitted cells; median improvements are 13.43–64.58%. |

The first held-out cohort found a 7.61% best-time gain from direct execution at
32,768 nearly unique rows, motivating the extension beyond the original 8,192
bound. In the final extended cohort, that boundary's worker/automatic best times
are 0.869209/0.863833 ms, and medians are 1.029916/0.891625 ms. The separately forced
direct role records a 0.873458 ms best, slightly slower than the worker best;
that observation remains in the packet. The small automatic gain is retained
without implying that every individual call improves.

At 49,152 and 65,536 nearly unique rows, forced direct best times are slower than
workers. The selector therefore stops at 32,768 instead of extrapolating the
low-cardinality wins. These measurements do not establish a universal crossover
for every machine, encoding or cardinality distribution.

The clock includes fresh public source/session preparation, native execution,
complete result-summary formation, native I/O certification and report cleanup.
Fixture construction, oracle validation and benchmark-record output are outside
the clock. This is neither CLI startup nor persistent prepared-call latency.
Caches are warm/uncontrolled and concurrent host work is accepted context.
Process peak RSS covers the entire fixture/test process, not either role alone.

One earlier extended run completed its calls but failed the post-run cumulative
256 MiB log guard. It is excluded from accepted measurements and preserved as
diagnostic evidence. Lossless compaction of completed historical call logs made
room for the successful rerun; no guard was disabled or raised.

## Acceptance and evidence

Regular tests cover automatic selection on both sides of the row boundary,
retained caps of 1/7/128, non-null signed extremes, packed/dictionary/constant
sources, nullable rejection, low-budget and single-slot preservation, cancellation
before execution and before publication, source mutation/replacement detection,
complete values and reservation cleanup. Existing owned-result worker-pressure
tests explicitly retain forced-worker coverage and add automatic-route coverage.

Formatting and strict workspace/native Clippy pass. The final source passes
3,425 workspace tests and 1,985 native tests; 22 native tests are ignored,
including the three explicitly run release measurement screens.
Paired Full43 passes all 258 complete outputs against the frozen C4 control.
The retained source has 99,997,497 rows and 15,682,956,116 bytes. Best-of-three
sums are 74.175540 s control and 74.091229 s candidate; no query crosses the
10% plus 150 ms timing screen. All calls remain outside the new selector, with
zero selection activations. These are regression results, not an attributed
Full43 speedup. The frozen final CLI is `3d6594259710`, SHA-256
`b6bf781688d84061083ccc2921a9045b7f7ea169ae22385695e2e76a82bd2cdc`.

The [portable evidence packet](../benchmarks/small-numeric-count-selection-2026-09-27.json)
includes complete screen and Full43 outputs,
all samples, source snapshots/patches, binary hashes, validation logs, excluded-run
diagnostics and guarded runners. Historical local commit IDs identify build
provenance; archived source bytes make the proof independent of those Git objects.
Independent reconciliation checks all 960 raw screen records, literal oracle
values, route evidence, source identities and best/median/gain calculations.
The host is an Apple M5 with ten logical CPUs and 16 GiB RAM, macOS 27.0;
the packet captures the Rust 1.98.0/LLVM 22.1.8 toolchain and complete-call resource
observations. The Full43 policy requests twelve CPU slots and 24 GB; these are
policy values, not additional physical resources or an enforced RSS ceiling.

The change adds no persistent state and respects the existing state-item bound.
It does not close the broader audit of provider allocations, reservations and
process RSS. No ingest, persisted-format or package-release change is included.
