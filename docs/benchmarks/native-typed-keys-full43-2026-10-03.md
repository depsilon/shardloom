<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native typed key acceptance — October 3, 2026

Flat Binary, exact Decimal128, Date32 and timezone-free microsecond timestamp
keys now use the shared native relational comparison, hashing and ordering
components. The [implementation contract](../architecture/native-typed-keys-2026-10-03.md)
defines the admitted operators, expressions, source handoff and sort spill.
Complete local correctness and resource acceptance passes. Hosted acceptance
remains pending; typed literals, casts, arithmetic, retained unary state, nested
keys, broader adapters and remaining state spill still require implementation.

## Frozen implementation and complete workflows

| Evidence | Accepted observation |
| --- | --- |
| Candidate | `2f40222671ae7312053b2140966c88075f190e63` |
| Candidate executable SHA-256 | `5aa5e5d85fb17adfcd516efee5e9cbbe99a793df6de1254bdd82460ee57741a6` |
| Same-window control | `8237a90033887107eef06a8736f2f8a559a0b3ba`, the accepted typed-payload runtime |
| Control executable SHA-256 | `231226350762132f55c0a0a366b4fb0559fa208ad4f02b858cc67a765e287980` |
| Build | Rust 1.99.0, release profile, `release-user-surfaces`, identical Cargo lockfile |
| Source proof | Clean committed candidate, 811 source hashes; CLI and both fixture executables frozen and reverified |
| Public workflow matrix | 5,733 checks; 12,282,897 complete row comparisons |
| Typed subset within that matrix | 2,428 checks; 4,595,372 complete row comparisons |
| Separate direct-unary matrix | 202 checks; 131,734 complete row comparisons |
| Accepted sort regression screen | 24 complete Q24–Q27 results; no timing, RSS or aggregate threshold crossed |

The matrix retains all 4,093 still-applicable cases from the preceding 4,109-case
payload matrix. Sixteen old typed-key denial cases are replaced by 1,640 checks:
1,400 composed key workflows, 80 larger spill workflows and 160 ordinary SQL
workflows. Independent typed oracles cover exact complete values, schemas, nulls
and order through SQL and DataFrame declarations, native Vortex and admitted
Arrow inputs, collection and representable local writers. Existing collection
limits remain; the 65,541-row fixture also proves complete output above that
boundary. Unsupported dtype/format combinations fail before publication.

The shared key owner covers joins, sets, grouping, ordering, window partition/order
and membership/correlation, plus COUNT, COUNT DISTINCT, MIN/MAX and the contract's
same-type comparisons and null-selection expressions. Binary comparison uses
complete unsigned bytes. Decimal precision and scale must match exactly; temporal
logical types remain distinct from integer storage and each other. Tests include
different dictionary domains, exact decimal boundaries, complete temporal storage
ranges, null/empty plans, branch selection and incompatible-type denials. Forced
native sort runs preserve complete values, logical schemas, stable ties and owned
cleanup. They do not admit group, join or window state spill.

Ordinary SQL selects its native strategy after metadata admission, over one
generation-bound source and operation grant. It keeps the existing optimized
aggregate, sort and simple projection/filter providers for their supported types;
extended typed fields reach the shared relational binder before scanning. Failed
execution is terminal. Writer tests verify LIMIT, metadata-pruned empty results,
matched-row count exactness, source replacement and resource release. Compact SQL
comparisons also preserve operators inside quoted strings.

Sort scratch estimation and Top-K consume the same decoded native columns, and
UTF8 sizing reads view lengths without scalar-by-scalar provider execution. The
operation file provider retains Vortex's `SharedSegmentSource`, sharing live
requests without caching later answers. A real-file regression test proves one
physical read and identical buffers for live requests, a new read after request
owners drop, and allocation credit release; it fails against the preceding plain
file provider. These are bounded ownership proofs, not total process-RSS coverage.

All 24 selected local gate categories pass. Default workspace tests report 3,448
passed; native Vortex tests report 2,280 passed with 23 existing ignored tests;
native CLI tests report 1,588 passed. Python runs 865 tests with 144 skips covering
retired direct-source fixtures and optional dependencies. The UAT harness runs
19 tests without skips; its consumer suite runs 30 with one existing skip. Counts
overlap across build configurations. Formatting, strict default/native Clippy,
lean and native-without-write builds, Rust 1.96 compatibility, governance,
documentation and website checks pass.

The packet retains each check's source manifest. Sixteen affected categories were
rerun after the final source changes; eight unaffected categories retain their
earlier receipts with an exact ten-path source-difference allowlist. The final
native engine and CLI checks, release build and all accepted public/paired
workloads match the same 811 source hashes. Earlier evidence is not relabeled as
a new run. Documentation written after acceptance has separate validation.

## Paired Full43 observation

Both binaries run against the same resident 99,997,497-row Vortex source and all
43 retained result references, with three calls per query and role. The source
is 15,682,956,489 bytes with SHA-256
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
Complete bytes, generation and residency are checked before timing and reverified
during acceptance. Full43 is retained-result regression evidence, separate from
the independent typed oracles. No ingest timing or external-engine comparison is
part of this observation.

The macOS aarch64 host has 16 GiB physical RAM and 10 logical CPUs. Both roles use
parallelism 12 and a 24 GiB admission grant, which is not an enforced process-RSS
limit. Native work is serial under the existing storage, concurrency, deadline
and process cleanup guards. Prehashing warms source bytes; OS cache and ordinary
host activity remain uncontrolled. Full43 load averages range from 2.824 to 9.613.

Predeclared investigation thresholds apply symmetrically to gains and regressions:
per-query median time changes of at least 10% and 0.1 seconds, median RSS changes
of at least 10% and 32 MiB, or aggregate best/median-sum changes of at least 5%
and one second. A flagged query requires reversed-role-order repeats; an aggregate
flag requires all 43 queries to repeat.

| Observation | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Sum of fastest valid query runs | 65.260744 s | 64.505372 s | -0.755372 s / -1.157% |
| Sum of per-query medians | 66.895004 s | 66.399734 s | -0.495270 s / -0.740% |
| Sum of all raw runs | 203.316041 s | 199.862612 s | -3.453429 s / -1.699% |
| Maximum observed process RSS | 4,517,871,616 B | 4,442,112,000 B | -75,759,616 B |

All 258 complete results match. No aggregate or RSS threshold is crossed. Q21's
median time initially changes from 1.228237 to 1.097456 seconds, a 10.648% gain
of 0.130781 seconds. Its six required reversed-order calls all pass, with medians
of 0.798280 and 0.778426 seconds: a 2.487% difference of 0.019854 seconds, below
the investigation threshold. The repeat has no remaining timing or RSS flags.
Both cohorts remain in the packet; the initial gain is not reproduced at the
declared threshold and no performance improvement is claimed.

These totals sum query process observations. The Full43 supervisor takes
493.246 seconds, including validation and archiving; the Q21 repeat supervisor
takes 7.696 seconds. Neither the sums nor their difference is elapsed end-to-end
ingest/query/delivery time. The earlier payload report's faster absolute cohort
is not substituted for this same-window control.

## Rejected development observations

Three failed typed public runs remain recorded after 840, 920 and 920 successful
checks. They exposed the DISTINCT-count SQL spelling and shared parser admission
for column-to-column null selection and compact comparisons. The repairs use the
existing native binder/parser. The completed accepted matrix uses the final
binary; passing prefixes of failed runs are not completion evidence.

An earlier candidate completes its public matrix but fails the paired run at
Q26's 120-second deadline after 150 complete comparisons. The packet retains the
incomplete summary, all completed raw responses and the timeout artifacts. Native
sort inspection identifies repeated scalar scratch decoding and changed layout
splits; the subsequent repair reuses decoded columns and bounded provider splits.
The subsequent candidate's Q24–Q27 screen passes all 24 results but shows a Q24 RSS
increase of about 16.7%, reproduced at about 16.1% in its six reversed-order calls.
This candidate is also rejected, and both cohorts are retained.

After restoring live native segment request sharing, the final 24-result screen
crosses no threshold. Q24 median RSS changes from 997,588,992 to 1,014,661,120 bytes
(+1.711%, about 16.3 MiB). In the final full cohort it changes from 1,062,912,000 to
1,085,030,400 bytes (+2.081%, about 21.1 MiB). This establishes that the measured
regression no longer crosses the frozen thresholds; it does not attribute all
process-memory differences to one allocation or claim a general speedup.

## Preserved evidence and remaining boundaries

The [portable packet](evidence/native-typed-keys-2026-10-03.json.xz) is 9,626,192
compressed bytes with SHA-256
`1a68c2cf0e9cb849289551b9870a85e82fab26a744a35c96e6a024fe72983312`.
Its 860,988,438 uncompressed bytes have SHA-256
`1de18b3262eaed3a78cf9251389a8e2a13b550a0ebae084c1e1c5c7b21a81b89`.
It retains 12,448 full public envelopes and 1,178 supplemental envelopes,
independent expected values, complete references, persisted-output hashes,
build/check receipts, failed observations and verification tools. Verification
checks 119,805 field sets for unique names. All three candidate Q1 calls prove
99,997,497 rows from footer metadata, with no row read, decode, materialization,
answer cache or fallback. Execution and Native I/O certificates retain explicit
`fallback_attempted=false` and `external_engine_invoked=false`.

Before the paired runs, 1,032 completed historical per-call files were compacted
losslessly with original identities, per-member size/hash checks and unchanged
summaries. This recovered 3,264,512 accounted log bytes. Failed or incomplete
cohorts remain untouched and storage ceilings remain unchanged. The packet
includes the compaction receipt and independently verifies every archive member.

Reproduce public cases with `scripts/run_native_relational_uat.py --family all`
and both fixture-generator arguments built from the frozen revision. Embedded
`run_acceptance.py`, `prepare_full43.py`, `compare_full_uat.py`,
`run_sort_regression.py`, `run_sort_repeat.py`, `run_full43_repeat.py` and
`finalize_acceptance.py` preserve guarded commands, hashes and thresholds.
Reconstruct local roots and retained source/reference artifacts before replaying.

Hosted completion still depends on the nested/pivot/typed stack and the website
audit's explicit advisory decision or remediation. This work does not activate
the disabled exception, publish packages, certify total allocator/RSS coverage,
or complete the broader universal-workflow and competitive gates. Native Vortex
output proof is real payload readback; it does not turn unrelated placeholder
artifacts into completed output support.
