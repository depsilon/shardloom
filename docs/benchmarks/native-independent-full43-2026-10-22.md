# Independent Full43 correctness and Q29 correction

The corrected native runtime at `a973ad72af847010594311d852da854203d01768`
matches all 381 result rows for the 43 pinned ClickBench queries against an
independently calculated reference over the original 99,997,497-row Parquet
input. The accepted Vortex generation is linked to that input through the
retained ingest validation. DuckDB 2.0.0 runs only as a separate correctness
oracle; native execution has no external-engine fallback.

The final isolated pass records **49.953164785 engine seconds**, one execution
of each query. The [portable review](evidence/native-independent-full43-2026-10-22/review.json)
binds the source, release build, executable, original inputs, two reference
versions, complete results and every native observation. The
[design](../architecture/native-independent-full43-correctness-2026-10-22.md)
describes the integer aggregation and regex contracts.

## Correctness findings

The first independently frozen reference exposed large-integer AVG accumulation
drift and an incorrect equivalence between a regex replacement and `url_domain`.
Native integer-input SUM/AVG now compensate additions across scalar, grouped,
fused, encoded and partial-merge consumers. Their F64 output and initial F64
conversion remain explicit; floating-input evaluation retains its existing
order. The exact regex specialization preserves unchanged nonmatching input,
the required slash, newline behavior and the complete capture bytes. Existing
URL-domain metadata cannot stand in for a regex result.

The first corrected pass stopped at Q4 because the original oracle's ordinary
floating AVG also accumulated error. Source-only adjudication compared the
original integer total, the exact total after F64 input conversion and an
independent compensated AVG. They agree with the corrected native result. The
replacement oracle accumulates the pinned integral SUM/AVG operands exactly,
then applies the documented F64 output conversion and count division. Its type
and overflow checks are retained. No native output was used to generate either
reference, and the original `1e-12` comparison tolerance was unchanged.

## Retained engine observations

| Observation | Complete queries | Engine sum | Q29 engine time | Disposition |
| --- | ---: | ---: | ---: | --- |
| First corrected attempt, 947 | 3 accepted before Q4 failed | Unavailable | Not executed | Stopped; original oracle error adjudicated separately |
| Corrected parity, 948 | 43 | 71.991550760 s | 24.832479084 s | All 381 rows match; Q29 overhead retained |
| Maintainer-requested repeat, 949 | 43 | 73.414695881 s | 25.267967791 s | All 381 rows match; regression confirmed |
| Dictionary admission correction, 950 | 43 | 49.953164785 s | 3.079574458 s | All 381 rows match; native dictionary path restored |

The earlier **48.754238077-second** observation used retained native regression
values and the old semantics. It remains historical evidence, not proof of the
corrected result or an interchangeable correctness baseline.

Q29's regression came from the existing derived-column rewrite admission rule.
Requiring embedded URL-domain metadata for the distinct regex expression broke
same-source dictionary fusion and projected an extra length column. The fix
preserves the original dictionary for the admitted regex/length combination.
Recorded counters change from `direct_accessor_general_group_update` to
`transformed_dictionary_dense_general_chunk_partial_group_update`, with only
the `Referer` dictionary accessor, no materialized group values and no embedded
derived-column rewrite. The other 42 queries sum to 48.146728090 seconds in 949
and 46.873590327 seconds in 950. These are individual observations, not a paired
performance study or a general speedup claim.

Each complete pass used the standard runner, 24-GiB admission, parallelism 12,
one execution per query, complete result validation and unchanged deadlines,
storage and process-cleanup guards. Other task work was idle during execution.
Input hashing and recursive inventories occurred before or after queries. Page
cache state was uncontrolled; required hashing may warm it. Other clocks and
host/resource observations remain in the evidence.

## Validation and replay

Source-bound checks pass: formatting; strict workspace and native-feature
linting; 3,239 workspace tests; 3,174 Vortex library tests with 31 existing
manual ignores; 1,100 public CLI tests; and 11 independent-oracle tests. Three
small public SQL checks distinguish the rebuilt runtime's integer and regex
behavior. The pre-fix dictionary regression test fails as intended; the final
native suite passes. A focused run invalidated by concurrent formatting is
retained and receives no acceptance credit.

The packet stores 941 versioned members in 3,075,208 compressed bytes. It
includes both references, the stopped pass, both slow passes, the corrected
pass, build/source identities, raw results, source-only numerical adjudication,
and failed tooling observations. Large inputs, binaries and environments are
omitted. Original absolute paths are lookup identities, not replay dependencies.
Replay uses the current trusted result extractor and never executes archived
code or an engine.

From the repository root, with Python 3.10 or later:

```sh
evidence=docs/benchmarks/evidence/native-independent-full43-2026-10-22
python3 "$evidence/verify_correctness_packet_950.py" --review "$evidence/review.json" --packet "$evidence/records.json.xz" --repository . --check-current-source
python3 "$evidence/test_correctness_packet_950.py"
```

Omit `--check-current-source` when replaying this historical observation after
a later runtime change. All 18 verifier tests pass, including corrupted values,
coverage, source/input identities, timings, fallback and unsupported claims.
The packet supersedes the earlier missing-independent-Full43 disposition for
this finite workload. It does not close CG-5/CG-6, the fifteen required owner
contracts, broader SQL parity or release readiness. Refreshing the selected
22-task matrix is completed below. Current PR integration remains separate
acceptance work. Required prerequisites and gated investigations continue;
optional breadth remains deferred.

## Corrected-runtime workload matrix

The same corrected executable now passes **704 complete native/reference
comparisons**, covering all 22 existing workload declarations in 18 cohorts.
Each task has 32 cells: raw/prepared collection across eight input formats and
raw/prepared CSV input across eight writer formats. The fixture remains 1,000
fact rows, 20 dimension rows and eight partitions. Pandas 3.0.3 is a separate
comparison oracle. Its complete environment inventory of 6,832 files is
identical before and after execution; nine installed distributions and the
source-tree ShardLoom metadata remain distinct.

All 368 original collected statements pass logical-schema checks, including
48 explicit terminal-order checks. Writer results pass their original complete
readbacks. The matrix uses the original workload declarations, one execution
per cell, 1-GiB native admission, parallelism 2, sampled 1.5-GiB process-tree RSS
limit, 60-second request deadline and unchanged storage limits. None of these
settings is inferred from the Full43 run, which has its own resource contract.

The original driver stopped during log retention after **13 complete cohorts**.
Its closed-handle check found a PID log open in macOS Spotlight; it refused to
retire that file. The failed supervisor and all 594 completed comparisons are
retained. A frozen continuation first reverified those results and the unchanged
runtime, harness and environment, then executed only the five remaining cohorts
for 110 further comparisons. No completed cohort was repeated, no native result
failed, and no guard limit was raised. The first twelve cohorts' compacted logs
are verified member by member; cohorts 13–18 retain their original logs. This
is a completed matrix with an explicit interruption, not an uninterrupted run.

The [matrix manifest](evidence/native-independent-full43-2026-10-22/matrix-manifest.json)
binds 11,557 stored members in 4,071,672 compressed bytes and 1,307 omitted
physical-payload identities. The [portable replay](evidence/native-independent-full43-2026-10-22/matrix-replay.json)
rechecks every comparison and binds its executable to the independent Full43
packet above. It executes trusted repository validators over recorded bytes,
without running archived code, a native engine or a comparison engine. Large
physical inputs, binaries and output files are represented by their original
verified identities; replay does not claim to reopen those omitted bytes.

```sh
evidence=docs/benchmarks/evidence/native-independent-full43-2026-10-22
python3 "$evidence/verify_matrix_packet_951.py" --repository . --manifest "$evidence/matrix-manifest.json" --output /tmp/corrected-matrix-replay.json --check-current-runtime
python3 "$evidence/test_matrix_packet_951.py"
```

Use a new output path and Python 3.10 or later. Omit `--check-current-runtime`
after subsequent runtime changes. The existing Python CI shard discovers both
packet replay tests. The Full43 test also copies its Python replay dependencies
to a temporary checkout location before replaying the packet and all 18 controls.
This reproduces and fixes the CI failure caused by looking up a historical oracle
member with the replay checkout's absolute path; member lookup now uses the exact
source identity in the retained oracle protocol. Original packet paths and bytes
remain unchanged. All 14 matrix test methods pass, with semantic corruption
subcases for provenance, environment, missing work, resource/SQL drift, complete
values, fallback, writer readbacks and original payload type/order/completeness.
The [validation record](evidence/native-independent-full43-2026-10-22/matrix-validation.json)
retains the unsupported-interpreter attempt and a corrected negative-test
diagnostic expectation, neither of which changed the runtime or verifier.

This refresh supersedes the stale-runtime limitation of the
[prior matrix](native-workload-comparisons-2026-10-22.md), whose results remain
unchanged. Its four-decimal comparison normalization, empty-sum convention and
small fixture scale remain explicit limits. No collected output contains NULL,
and the reference records do not preserve logical schema metadata, so these
checks do not certify cross-engine nullability or type equality. Precision,
pressure, lifecycle and installed-platform guarantees retain their separate
tests and reviews. The matrix establishes no speed, broad parity, CG-5/CG-6,
production-readiness or package-publication claim.
