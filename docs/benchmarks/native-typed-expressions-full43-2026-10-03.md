<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native typed expression acceptance — October 3, 2026

Status: complete local acceptance; hosted review and the inherited website advisory
decision remain pending.

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
