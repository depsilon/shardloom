# Grouped-cost attribution — October 31

The isolated diagnostic completed **48 queries once each**, with all **1,920
result rows and schemas** matching their original independent references. Its
instrumented native engine total is **1.404428827 seconds**. This is attribution
inside the existing relational owner, not a production speedup observation or a
replacement for the accepted 347-query baseline of 12.055132613 seconds.

## Observed engine work

| Exclusive phase | Engine seconds | Share of diagnostic engine time |
| --- | ---: | ---: |
| Demanded-source intake | 0.634856831 | 45.20% |
| Group-key preparation | 0.120067248 | 8.55% |
| Group interning | 0.210069189 | 14.96% |
| State reservation | 0.001125656 | 0.08% |
| Measure preparation | 0.010573059 | 0.75% |
| Typed aggregate updates | 0.132167179 | 9.41% |
| Other aggregate updates | 0.000000000 | 0.00% |
| Other aggregate consumption | 0.004508159 | 0.32% |
| Aggregate finalization | 0.002880618 | 0.21% |
| Unclassified engine remainder | 0.288180888 | 20.52% |

The remainder is retained explicitly. The probe does not establish a finer
breakdown of input validation, selection, downstream ordering or other engine
work in that interval. Synchronous downstream delivery is excluded from the
finalization scope; result serialization stays outside the existing engine
clock. Nested intervals are disjoint and their sum never exceeds that clock.

The two selection strata differ substantially:

| Original selection | Cases | Source intake | Key preparation + interning | Typed updates |
| --- | ---: | ---: | ---: | ---: |
| All rows | 24 | 28.94% | 40.46% | 17.08% |
| One-percent predicate | 24 | 64.50% | 3.40% | 0.32% |

There are 44,040 typed-update scope entries, averaging approximately 3.0
microseconds of recorded update time per entry. This is a batch/measure-scope
average, not a per-row time or a measurement of worker coordination overhead.
Instrumentation can perturb these short intervals. Even removing the entire
typed-update interval would remove only 9.41% of this recorded sample under an
otherwise unchanged execution; that is an upper bound, not an attainable gain.

## Scope and controls

The [predeclared contract](../architecture/native-grouped-cost-attribution-2026-10-31.md)
selects existing direct-argument cases over signed, nullable, floating and
all-null values; integer, string and combined grouping keys; SUM-only and mixed
SUM/AVG/MIN/MAX/COUNT; and all rows versus the original `selection_key < 10000`
predicate. Original corpus order, the resident million-row Parquet fact source,
complete references, ordering and tolerance policies remain unchanged.
All 48 cases also match their accepted execution's demanded column count,
decoded row count, data-read bytes and data-read requests.

The release test executable constructs the equivalent native relational plan.
It does not time SQL parsing. Timers exist only in test builds and use coarse
caller-thread scopes; debug work counters are disabled. Each query uses the
existing demanded source and certified native executor, a 2 GiB grant and
maximum parallelism two. No worker pool, external engine, input regeneration,
cache manipulation, artificial pacing or Full43 execution was introduced.

Every case reports complete input consumption, detached result ownership, no
fallback, no denied reservations and zero remaining reserved bytes after owner
release. Full identities were checked before and after the cohort. During
queries, the bounded supervisor uses only the active-output storage checks and
lightweight workload/resource guard. Other task work and agents were idle.

The sample does not cover every grouped expression, high-cardinality state,
spilling or all 180 grouped cases. Its fractions cannot be multiplied directly
into a guaranteed saving for the 347-query suite.

## Compiled-code check and next decision

Inspection of two update closures in the accepted native library confirms that
page selection is already strength-reduced to a shift and mask. Page-descriptor
loads and separate page/element bounds checks still remain. The paired-extrema
closure also retains per-state count-overflow checks after its existing
whole-batch headroom admission. Exact library identity and disassembly are
retained with the diagnostic; no queries were executed for that inspection.

The next candidate is once-bound contiguous state access for suitable small
tables and explicit consumption of the existing paired-extrema count proof.
Preserve ordered arithmetic, first errors, cancellation and leased state;
ordinary paged/checked execution must remain available for declined inputs.
This candidate still requires semantic/resource controls and a single complete
unchanged 347-query gain/loss observation.

General complete-group worker routing is deferred for the current workload.
The measured short update scopes do not justify its coordination and ownership
complexity yet, especially for selective cases. Larger workloads may need a
separate attribution and admission contract. Source intake and grouping remain
larger measured targets in this sample. The broader optimization queue and
CG-1 through CG-23 remain open.

## Reproduction

The maintained runner is `scripts/native_grouped_cost_diagnostic.py`. Its
`freeze` stage binds the original baseline, separately built release test
executable and build receipt before the single `observe` stage. The `replay`
stage verifies saved observations without executing queries. The portable
packet and `scripts/verify_grouped_cost_packet.py` retain the complete records,
references, source versions, rejected initial lint attempt and disassembly;
archived code is never executed during replay.
