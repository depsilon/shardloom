# Selection, retention and public-construction outcome

The combined candidate is **dropped**. One isolated execution of each of the
original 10,000 requests returned correct complete results, but the candidate
missed both performance acceptance rules. The accepted source and runtime remain
the shared-mechanisms baseline. No component of this combined candidate is
individually accepted from its bounded controls or construction profile.

| Complete cohort | Accepted baseline | Combined candidate | Change |
| --- | ---: | ---: | ---: |
| Engine execution, seconds | 1,513.095098453 | 1,613.587599075 | +100.492500622 |
| Setup and declaration, seconds | 377.101436302 | 302.808987449 | −74.292448853 |
| Collection, seconds, including engine | 1,557.448630041 | 1,673.537732993 | +116.089102952 |
| Delivery, seconds | 1.823282086 | 3.597416296 | +1.774134210 |
| Complete public request, seconds | 1,936.373348428 | 1,979.944136738 | +43.570788310 |

The public clocks are included because the maintainer explicitly requested this
construction and collection investigation. They are separate from engine time;
collection includes engine execution, so those two rows must not be added.
The complete-request clock starts at the existing public API setup boundary,
after outer interpreter/native-extension loading, and ends after full delivery.
It does not measure the enclosing supervised workflow. The original worker,
adapters and timing boundaries are byte-identical; the candidate Python package
is an explicit product change.

## Candidate and disposition

The tested source is `ea5fc5988b0453a5b64289bed82e9440c0a2e04c`, including the
selection/sort precursor `7ee9e6673`. It combines shared join coordinates,
discarded sorted-payload validation, credited window partition spans and peer
omission, checked projection admission, dictionary unary reuse, retained SQL/
frame descriptions and deferred Python implementation modules. Its full
[design contract](../architecture/native-selection-retention-2026-11-04.md)
remains as investigation history.

The binary engine gain rule rejects the higher complete total. The public-request
rule independently rejects the higher complete-request total despite lower setup.
The 98 changed product, product-test and generated-reference paths were restored
exactly to `ac20b82118a4626bc5f47b530842318ed006f0d7`. The rejected diff is retained
as inert evidence. The accepted baseline remains **1,513.095098453 engine seconds**.

## Correctness and protocol

The capture uses the original 10,000 request identities, manifest order, input
generations, independent expected values, five public surfaces, two execution
lanes and 2 GiB declared memory. Every request executes once. There are no warmups,
retries, alternate routes, additional Full43 passes or new external-engine runs.
All other task work stays idle during query execution. Complete hashes and
recursive inventories remain outside the query interval; active-output limits,
deadlines, the lightweight concurrency guard and cleanup remain enabled.

The complete observation has 10,000 passing attempts, unchanged post-run hashes
and a drained/reaped process group. The portable replay compares complete rows
under each original semantic contract, checks native attribution and resources,
reconciles integer engine nanoseconds and each public clock, and verifies the
exact captured requests against their pre-execution hashes.

The candidate passed strict formatting, default and native-feature linting,
6,926 native-feature tests and 3,319 default tests. There were 36 native-feature
ignored tests and no default ignored tests. The Python core log reported 817
tests run, 57 skip entries and two failures caused by missing historical
sparse-checkout evidence. Its JSON receipt counted 1,145 planned tests; the
original acceptance receipt incorrectly inferred 1,086 passes from that planned
count. Both receipts remain unchanged, and the replay records this correction
without inferring a successful count from class-level skip entries. After
restoring those evidence files, both
affected tests passed without changing product code. Eleven compatibility tests
and 314 installed-package tests passed; the installed proof has no failures,
errors or skips. Original failed checks and their later resolutions remain
visible in the packet.

## Bounded diagnostics and limits

The selection-only 13 controls passed 224 expected rows in 46.675851664 engine
seconds, versus 50.622956124 for the same archived requests. Both named ranking
controls were slower. The combined 24 controls passed 671 rows in 48.104409003
engine seconds, versus 53.692952583. Neither smaller set predicted the complete
cohort's accepted disposition.

Two additional instrumented smoke requests split collection into native
preparation/execution, CLI launch/waiting and Python report adaptation. They
are excluded from every scored cohort. Complete report adaptation was below
one millisecond in each diagnostic request; those two observations do not
justify redesigning result ownership or making a broad large-result claim.

The construction-only profile found source compilation substantially larger
than repeated envelope-wrapper generation. The candidate therefore preserved
76 canonical classes and 73 dataclass decorators while deferring substantial
implementation bodies. Compatibility evidence covers declarations, source
inspection, type hints, frozen/slots behavior, subclasses, pickle, forwarding
arguments and original lexical name resolution. These correctness results do
not override the failed performance decision.

This is one observation of combined changes. Per-case and family regressions,
work-counter coverage and complete-request clocks are retained. The results
do not isolate the causal contribution of any one change or estimate repeat
variance. Further performance changes require a separately justified experiment;
this outcome does not authorize another query pass or package publication.

The largest family changes in engine seconds were:

| Family | Candidate minus baseline |
| --- | ---: |
| Subqueries | +51.267290120 |
| Windows | +44.884958150 |
| CTE composition | +19.750236242 |
| Nested values | +8.447425880 |
| Ordering | +5.549906391 |
| Ordinary joins | −29.779256237 |
| Multiway joins | −5.568237965 |

DataFrame complete-request time decreased by 49.245 seconds. The other four
surface totals increased; together they exceeded that gain. CLI and DataFrame
setup decreased by 42.739 and 32.000 seconds, respectively. These are matched
subtotals of the same complete cohort, not additional observations. The broad
engine regressions and uncontrolled cache/host conditions prevent causal
attribution to the facade or any one native optimization from this run alone.

## Portable evidence

The [packet index](evidence/native-expanded-query-2026-11-03/selection-retention/index.json)
binds 40 lossless case parts, bounded proof streams, the rejected source diff,
build and installed-package receipts, construction/collection diagnostics and
all original failed observations. The [replayed result](evidence/native-expanded-query-2026-11-03/selection-retention/results.json)
contains exact engine totals, separate public clocks, family/profile/surface
breakdowns, named controls, regressions and explicit counter absence.

```sh
packet=docs/benchmarks/evidence/native-expanded-query-2026-11-03
python -B -S benchmarks/traditional_analytics/query_surface/expanded_selection_replay.py \
  --baseline "$packet/packet" --previous "$packet/shared-mechanisms" \
  --archive "$packet/selection-retention"
```

Replay reads saved data only. Archived paths are identifiers; no original host
files, native library, query engine or packaged source code are executed.
