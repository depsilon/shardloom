<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native typed expression acceptance — October 3, 2026

Status: complete local acceptance; hosted review and the inherited website advisory
decision remain pending. The refreshed acceptance for review corrections is
recorded [below](#review-corrections-and-refreshed-acceptance); the original frozen
observation remains intact.

Binary, exact Decimal128, Date32 and timezone-free microsecond timestamp
expressions use the existing native binder, column owners and result builder.
The [implementation contract](../architecture/native-typed-expressions-2026-10-03.md)
defines typed literals, explicit casts, exact decimal operations, binary functions
and checked calendar semantics. Hosted acceptance and broader native breadth
remain separate from this finite local unit.

## Frozen implementation and complete workflows

| Evidence | Frozen scope |
| --- | --- |
| Candidate source | `3e507b979358c6fbcdd2cdd81c68a7512548ec63` |
| Candidate executable SHA-256 | `34d536b02be7367db39019f1782d809c1e44770115804a11edc928951388cc4e` |
| Same-window control | `2f40222671ae7312053b2140966c88075f190e63`, the accepted typed-key runtime |
| Control executable SHA-256 | `5aa5e5d85fb17adfcd516efee5e9cbbe99a793df6de1254bdd82460ee57741a6` |
| Build | Rust 1.99.0, release profile, `release-user-surfaces`, identical Cargo lockfile |
| Source proof | Clean committed candidate; 817 source hashes; CLI and both fixture executables frozen and reverified |
| Complete public scope | 6,510 checks; 14,120,153 complete row comparisons |
| Typed subset | 3,205 checks; 6,432,628 complete row comparisons |
| New expression subset | 778 checks; 1,837,256 complete row comparisons |
| Separate direct-unary scope | 202 checks; 131,734 complete row comparisons |

The public scope and complete expression oracles are frozen before execution.
All 5,732 still-applicable cases from the 5,733-case typed-key matrix retain their
names and row counts. The old empty decimal-to-float denial becomes ten positive
collection/writer checks. Together with 36 SQL/DataFrame expression families and
four ordinary source-SQL families, the new scope adds 778 checks. Native Vortex
and explicitly declared Arrow inputs use the same public workflow consumer.
Normal, null and empty results retain their field order and logical types.

Computed output from 65,541-row inputs tests Binary, Decimal128, Date32 and
microsecond timestamps above the collection limit. Limited collection reads 97
complete rows; full collection fails explicitly. Representable Vortex, Parquet,
Arrow IPC, Avro, JSON, JSONL and CSV writes are read back completely. ORC continues
to reject decimal/temporal output before publication; binary and other admitted
outputs have positive ORC checks. This is not broader format or spill permission.

Core and native kernels share checked decimal helpers. Output precision and scale
bind before execution, including zero rows. Integer operand precision comes from
declared width and signedness. Tests cover exact rescaling, rounding ties,
overflow, invalid metadata, dictionary/constant/all-null inputs and incompatible
types. Explicit finite floating casts permit rounding; arithmetic does not
implicitly mix float and decimal values.

Calendar tests cover complete Date32 formatting, checked Date32-to-timestamp
conversion, negative-epoch flooring and differences/offsets whose intermediates
need more range than their final values. Both signed and unsigned integer offsets
use checked wider intermediates. Native timestamp literals wrap primitive storage
constants in native extension arrays: the pinned provider's timestamp scalar
validation otherwise panics for valid extreme microsecond values.

TRY_CAST converts admitted value errors to typed NULL. Invalid binding,
cancellation and memory denial remain operation failures. Tests force variable
parse-scratch denial, selected CASE/COALESCE behavior, repeated strict failures,
consumer cancellation and last-owner credit release. A returned writer report
itself retains admitted metadata until dropped; the tests check that lifetime.
Shared native owners remain the execution boundary, with no decoded row-map
interpreter or external-engine fallback.

The existing 24 local gate categories pass on the compiled runtime. Default
workspace tests report 3,458 passed; native Vortex tests report 2,289 passed with
23 existing ignored tests; native CLI tests report 1,592 passed. Python runs 868
tests with 144 skips. Counts overlap across configurations. Formatting, strict
default/native Clippy, lean and native-without-write builds, Rust 1.96
compatibility, UAT harness/consumer, governance and documentation/website checks
remain part of the acceptance packet.

The initial broad checks use source `26b2c69f`. Candidate `3e507b97` changes only
the CSV expectation declarations in the new public fixture; all compiled runtime,
Python declaration and tested library bytes are identical, as is the executable
SHA-256. The packet explicitly records that one-path difference for retained
checks. Fresh public acceptance executes the corrected fixture; final documentation
has separate current validation. The release build repeats an existing upstream
debug-symbol stripping warning also present in the control build.

## Paired Full43 observation

Both binaries use the same resident 99,997,497-row Vortex source and all 43
retained result references, with three calls per query and role. The source is
15,682,956,489 bytes with SHA-256
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
Complete bytes, generation and residency are checked before timing and again
during acceptance. Full43 is retained-result regression evidence, separate from
the independent expression oracles. No ingest timing or external-engine
comparison is part of this observation.

The macOS 27.0 aarch64 host has 16 GiB physical RAM and 10 logical CPUs. Both roles
use parallelism 12 and a 24 GiB admission grant, which is not an enforced RSS
limit. Work runs serially under the existing storage, concurrency, process
deadline and cleanup guards. Source prehashing warms bytes; OS cache and ordinary
host activity remain uncontrolled.

Investigation thresholds are fixed before the run and apply symmetrically:
per-query median time changes of at least 10% and 0.1 seconds, median RSS changes
of at least 10% and 32 MiB, or aggregate best/median-sum changes of at least 5%
and one second. Every flagged query repeats with reversed role order; an
aggregate flag requires all 43 to repeat. All observations are retained.

All **258/258** complete comparisons pass. The initial cohort has no aggregate
timing or per-query RSS flag.

| Initial Full43 process measure | Control | Candidate | Candidate change |
| --- | ---: | ---: | ---: |
| Sum of each query's fastest valid call, seconds | 58.100960 | 58.545262 | +0.76% |
| Sum of each query's median call, seconds | 60.469736 | 59.662393 | -1.34% |
| Sum of all 129 raw calls per role, seconds | 188.790631 | 181.445071 | -3.89% |
| Maximum observed individual-process RSS, bytes | 4,759,781,376 | 4,732,567,552 | Observation only |

These sums are not elapsed workflow times. The supervised paired stage takes
453.006561 seconds, including per-call validation, host observations and log
archiving; source preflight/hashing and final packet verification run separately.
Every call times a complete native process through output and exit.

Q26 crosses the timing threshold: median control 1.615860 seconds versus
candidate 1.778651 seconds, an increase of 0.162791 seconds (10.07%). All six
required reversed-order calls pass. Their medians are 1.563827 and 1.594473
seconds, respectively: +0.030646 seconds (1.96%), below the investigation
threshold. Q26 median RSS changes by -1.47% initially and +1.02% on repeat,
also below threshold. The repeat supervised stage takes 12.215466 seconds.
The initial timing flag is not reproduced; both complete cohorts remain in
the packet. No causal explanation or performance gain is inferred.

The initial host one-minute load observations range from 2.15 to 7.43 for both
roles; the repeat ranges from 2.80 to 3.04. These observations do not prove an
idle host or attribute process timing to other applications. Footer-only Q1
retains three native I/O proofs for the exact count, with no answer cache,
scan, decode, row materialization or external engine.

## Retained failed observations

The first complete public attempt stops after 5,533 passing checks at the first
computed-cast CSV comparison. The fixture incorrectly classified every expected
Python string as a tagged binary/decimal CSV cell, including ordinary UTF8.
The corrected declarations name binary/decimal cells explicitly; the saved four
CSV rows match the existing writer contract without any runtime change. Case
coverage, row counts and the full expected-value file remain byte-identical.
The failed run, original scope, build and source fingerprints remain in the
packet. Its passing prefix is not treated as completed acceptance.

Development receipts also preserve the Date32 formatting and conversion overflow
reproductions, the upstream extreme timestamp scalar panic, the unsigned calendar
offset parity repair, and failed validation iterations. The retained-report
memory assertion was corrected to account for its still-live metadata lease;
it did not establish an engine leak. The accepted native tests exercise final
credit release after that report is dropped.

## Review corrections and refreshed acceptance

Review identified two correctness gaps. Python Decimal null-fill could produce
COALESCE branches with different declared precisions, which the binder rejected.
The shared binder now derives a lossless common decimal domain from integer-digit
capacity and scale. Only selected values are rescaled through the existing checked
cast and reserved native allocator. CASE uses the same rule. Empty/all-null
inputs, differing scales, wider integer domains, incompatible precision, lazy
invalid casts and final-owner credit release have regression coverage.

The core reference evaluator also lacked several conversions already admitted by
the native UInt64 cast kernel. Checked UTF8/Int64/Float64-to-UInt64 and
UInt64-to-Int64/Float64 conversions now agree with native execution. Float64-to-Int64
rejects the exclusive upper bound `2^63` instead of saturating to the largest
signed integer. Nonfinite source values remain admission failures for CAST and
TRY_CAST; ordinary admitted conversion errors retain TRY_CAST's typed-NULL behavior.

| Refreshed evidence | Frozen scope |
| --- | --- |
| Candidate source | `895a45c95308552e5163738edb1942c153bdeebc` |
| Candidate executable SHA-256 | `8f39831b4e73f13da94a79123be7f7c92d5f707771db9c89e1bd0df6d9420546` |
| Complete public scope | 6,600 checks; 14,120,333 complete row comparisons |
| Typed subset | 3,295 checks; 6,432,808 complete row comparisons |
| Expression subset | 868 checks; 1,837,436 complete row comparisons |
| Separate direct-unary scope | 202 checks; 131,734 complete row comparisons |

All original 6,510 cases retain their names and row counts, and all 40 original
expression oracles retain their complete expected values. Five additional oracle
entries cover Decimal branch behavior through native Vortex and declared-Arrow
SQL/DataFrame workflows and ordinary source SQL. They add 90 checks and 180 row
comparisons. The complete public and direct-unary runs pass on the refreshed
frozen CLI and fixture executables.

All 24 local gate categories pass with exact equality of all 817 compiled/runtime
source fingerprints; no fixture-source exception is needed for this refresh.
Default tests report 3,460 passed; native Vortex tests report 2,292 passed and
23 existing ignored tests; native CLI tests report 1,592 passed. Python runs
868 tests with 144 skips. These counts overlap across configurations. Fresh
documentation gates cover the final report and support records.

The same typed-key control, compiler, features, lockfile, resident source,
references, role-order policy, resource settings and thresholds defined above
remain the comparator contract. All 258 refreshed paired results match. No
per-query timing/RSS or aggregate threshold is crossed, so no reversed-order
repeat is prescribed for this cohort.

| Refreshed Full43 process measure | Control | Candidate | Candidate change |
| --- | ---: | ---: | ---: |
| Sum of each query's fastest valid call, seconds | 59.612970 | 59.784834 | +0.29% |
| Sum of each query's median call, seconds | 61.539952 | 62.215874 | +1.10% |
| Sum of all 129 raw calls per role, seconds | 202.824001 | 187.676522 | -7.47% |
| Maximum observed individual-process RSS, bytes | 4,862,574,592 | 4,620,730,368 | Observation only |

The supervised paired stage takes 477.179709 seconds, including per-call
validation, host observations and log archiving. Preflight source hashing and
final evidence verification remain outside that clock. One-minute host load
ranges from 2.46 to 6.54; neither an idle host nor a cause for timing variation
is inferred. All raw samples are retained. The raw-time sum is distinct from the
predeclared fastest-call and median-sum measures; no speedup or total-RSS bound
is claimed. Three refreshed Q1 proofs retain the exact footer-only count.

The first refresh preflight stopped before queries because accumulated logs
exceeded its 252-MiB admission threshold, which reserves space below the unchanged
256-MiB ceiling. Twelve files from three completed September 27 profiling screens
were archived losslessly after completion, identity and open-handle checks.
Every archived byte was verified before removing redundant originals; completion
receipts and failed/incomplete runs remain unchanged. This recovered 5,943,296
accounted log bytes, and the fresh preflight passed. The review packet rechecks
all twelve members and retains the failed preflight and compaction manifests.

The original Decimal-fill reproduction first hit an unrelated direct-source
admission boundary. The corrected reproduction prepared Vortex and reached the
reported native binding error; both observations are retained and distinguished.
Development test failures also retain the corrected expectations for nonfinite
source admission and the subsequent passing regressions.

## Decoded-reference review acceptance

Follow-up review found that the decoded evaluator still eagerly evaluated
COALESCE's second argument, rejected variadic calls, returned selected Decimal
branches without their common declared type, and omitted UInt64 numeric
operators. Source `7a97a0c58cc471c837480bfea1232a5a806e577e` resolves result types
without evaluating unused values. COALESCE accepts 1–128 arguments; CASE and
COALESCE rescale only selected decimals. Declared nullable column types survive
NULL rows. UInt64 arithmetic checks overflow and division by zero, and negation
checks the signed result domain. The same repair covers exact signed/unsigned
comparisons, integer string offsets and invalid Boolean TRY_CAST values.

Seven core tests include the six regressions that initially failed, plus bounded
type traversal and nested expressions. Four native/reference tables check both
implementations against independent literal values and declared types, including
empty native plans, typed NULLs, lazy errors, incompatible types and UInt64
boundaries. Default workspace tests pass 3,467 cases; native Vortex tests pass
2,296 with 23 existing ignored tests; native CLI tests pass 1,592. Python runs
868 tests with 144 skips. Counts overlap across feature configurations.

The shared evaluator is also used by the explicit row-based local-source
diagnostic route. Its full CLI and smoke tests are included. An intermediate
CLI test caught an overly narrow comparison check; decoded Int64/Float64 and
decimal comparison rules are preserved independently of native key admission.
The string-offset diagnostic now describes integer operands, and its existing
smoke assertion is updated. Failed reproductions, intermediate compile/lint
observations and both intermediate CLI failures remain in the evidence.

The [reference validation packet](evidence/native-typed-expressions-reference-2026-10-03.json.xz)
retains all 24 gate categories, raw logs, 820 source fingerprints, final document
fingerprints and the exact delta from the preceding acceptance. Only the core
evaluator/private type resolver and test code change. All 28 public expression
helper bodies, native Vortex kernels and binder, CLI/parser implementation,
Python declarations and UAT fixtures remain unchanged. Source inspection finds
the decoded evaluator only in Vortex test modules, outside the native execution
loop; this is a named-export/alias source audit, not an exhaustive dynamic call
graph claim.

The native public matrix and Full43 are not repeated for this follow-up. Their
complete retained packet, binary identity and `895a45c9` source remain unchanged;
those measurements are not relabelled as this source or as version 0.4.0. This
follow-up establishes the scoped correctness repair and adds no performance,
publication, hosted-acceptance or website-advisory approval claim.

The [review packet](evidence/native-typed-expressions-review-2026-10-03.json.xz)
contains the refreshed complete envelopes, independent oracles, paired responses,
identity hashes, local checks, failed observations and review. It links the
unchanged original packet and verifies that the original case coverage and
expected values are preserved. Both packets are separate immutable observations.

## Evidence and remaining scope

The [portable packet](evidence/native-typed-expressions-2026-10-03.json.xz)
contains frozen scope and complete oracles, every accepted raw public envelope,
complete paired responses and references, source/binary/output/archive hashes,
local checks, failed observations, executed drivers and the final review.
Packet creation streams compression, verifies the complete decompressed digest,
and replaces local home paths with portable placeholders. A separate inspection
reopens the finished packet and checks its identities and coverage.

No performance improvement, total-RSS bound, broader SQL/DataFrame parity or
completed competitive gate is claimed. Retained unary-state semantics, nested
keys, richer aggregate/window behavior, wider adapters, state spill/recovery and
remaining reader/provider accounting retain their phase owners. Hosted review,
merge and ledger closure depend on the existing PR stack and website advisory
decision. No package, release or resumed large format/text campaign is implied.
