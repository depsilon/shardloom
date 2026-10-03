<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native typed payload acceptance — October 3, 2026

Binary, exact Decimal128, Date32 and timezone-free microsecond timestamp payloads
now traverse the shared native composition and local delivery path, including
admitted nested leaves. The [implementation contract](../architecture/native-typed-payloads-2026-10-03.md)
defines the finite type and format boundaries. This is local correctness and
resource acceptance; hosted acceptance remains pending, and wider operator,
adapter and spill work is still open.

## Frozen implementation and complete workflows

| Evidence | Accepted observation |
| --- | --- |
| Candidate | `8237a90033887107eef06a8736f2f8a559a0b3ba` |
| Candidate executable SHA-256 | `231226350762132f55c0a0a366b4fb0559fa208ad4f02b858cc67a765e287980` |
| Same-window control | `55b14d148f1d7dec8e49ee17be8293bb36cb01d8`, the accepted native report-integrity runtime |
| Control executable SHA-256 | `b7fc281612df107c61f5fc27eb9df84c1ea2dd90a3c8c9ee631b465f9f0abde0` |
| Build | Rust 1.99.0, release profile, `release-user-surfaces`, identical Cargo lockfile |
| Source proof | Clean committed candidate, 804 source hashes; CLI and both fixture executables frozen and reverified |
| Public workflow matrix | 4,109 checks; 8,607,021 complete row comparisons |
| Typed addition within that matrix | 804 checks; 919,496 complete row comparisons |
| Separate direct-unary matrix | 202 checks; 131,734 complete row comparisons |
| Focused typed regression | 14 tests passed |

The public matrix retains all 3,305 preceding cases and adds independent typed
oracles for SQL and DataFrame workflows over native Vortex and declared Arrow
inputs. It verifies projections, filters/order on existing scalar keys, UNION ALL,
outer joins, window payloads, membership and ordered explosion of nested fields.
Each admitted writer is reopened and checked against complete expected values;
unsupported formats are denied without publishing the destination. The 65,541-row
fixture crosses the small-result collection limit while complete local output
remains available. Collection limits are preserved.

Binary covers empty and non-UTF8 values; Decimal128 retains exact precision,
scale and signed unscaled integers. Rust boundary tests also carry the complete
i32 date and i64 timestamp domains through representable writers without calendar
conversion. Invalid selected decimal precision fails through a fallible boundary;
an unselected invalid value does not fail a valid selection. Ownership checks
cover compact selected buffers, sliced/cloned owners, last-owner credit release,
narrow-grant denial and cancellation.

Vortex output preserves native logical types. Arrow IPC, Parquet and Avro retain
their admitted mapped types, with Avro decimal metadata accepted only when its
precision and scale exactly match the expected dtype. JSON/JSONL and collection
use hexadecimal binary strings, exact typed decimal strings and signed temporal
units; CSV follows the existing typed cell convention. Text output does not
persist logical dtypes. ORC admits binary but denies decimal/temporal payloads;
nested CSV and ORC remain denied, including empty results. At this frozen payload
revision, keys, arithmetic, casts, typed predicates and retained unary-state
semantics remain unadmitted. The later [typed key acceptance](native-typed-keys-full43-2026-10-03.md)
records the scoped comparison/key extension without changing this observation.

All 24 selected local gate categories pass. Default workspace tests report 3,447
passed; native Vortex tests report 2,254 passed with 23 existing ignored tests;
native CLI tests report 1,582 passed. Python runs 865 tests with 144 existing
environment-dependent skips. The UAT harness has 19 passing tests, and its
consumer suite runs 30 with one existing skip. Counts overlap across configurations.
Formatting, strict default/native Clippy, lean and native-without-write builds,
Rust 1.96 compatibility checks, governance, documentation and website checks pass.

The portable packet preserves each gate's source manifest rather than claiming
all checks ran on one commit. Native Rust gates cover the final runtime; later
changes were a Python case-label correction and documentation. Earlier unaffected
gates have explicit source-difference checks. Targeted typed acceptance used
`6a9bd301`; full acceptance used `8237a900`. Their 804 source hashes and all three
executable hashes match exactly. The final build was a cache hit after the
documentation-only Rust/Vortex review. That [review](../dependencies/rust-vortex-refresh-2026-10-03.md)
records current global Rust tooling and why Vortex 0.85 remains admitted.

## Paired Full43 observation

Both frozen binaries run against the same resident 99,997,497-row Vortex source,
all 43 retained result references, three calls per query and role. The source is
15,682,956,489 bytes with SHA-256
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
Complete source bytes, generation and residency are checked before timing and
reverified during acceptance. The fixture and reference origins are retained in
the packet; Full43 is retained-result regression evidence, separate from the
independent small typed oracles.

The macOS aarch64 host has 16 GiB physical RAM and 10 logical CPUs. Both roles use
parallelism 12 and a 24 GiB policy grant; the grant is not an enforced process-RSS
limit. Runs are serial under the shared storage, concurrency, deadline and process
cleanup guards. Prehashing warms source bytes; OS cache and ordinary host activity
are uncontrolled. Observed load averages range from 2.249 to 7.275.

Investigation thresholds were frozen before execution: per-query median time
changes of at least 10% and 0.1 seconds, median RSS changes of at least 10% and
32 MiB, or aggregate best/median-sum changes of at least 5% and one second.
Flagged queries require reversed-role-order repeats; an aggregate flag requires
all 43 queries to repeat.

| Observation | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Sum of fastest valid query runs | 53.027083 s | 52.583919 s | -0.443165 s / -0.836% |
| Sum of per-query medians | 54.110669 s | 53.671350 s | -0.439319 s / -0.812% |
| Sum of all raw runs | 162.588452 s | 162.081842 s | -0.506609 s / -0.312% |
| Maximum observed process RSS | 4,806,737,920 B | 4,813,340,672 B | +6,602,752 B |

All 258 complete results match. No timing, memory or aggregate threshold is
crossed, so no repeat is required. These are sums of query process observations,
not elapsed end-to-end time. The paired supervisor takes 409.781 seconds, including
its validation and archiving work. No performance improvement or causal difference
is claimed.

## Preserved evidence and remaining boundaries

The [portable packet](evidence/native-typed-payloads-2026-10-03.json.xz) is
5,701,708 compressed bytes with SHA-256
`bda278c73d0fe3d17f14c579a95b2e028627934e4f1dee2afa693d8397e02e62`.
Its 593,210,099 uncompressed bytes contain raw responses, complete references,
independent oracles, all build/check receipts, development failures and verification
tools. It retains 9,120 full public envelopes and 880 supplemental envelopes,
checks 85,465 field sets for unique names, and rehashes every persisted public
output. All three candidate Q1 calls prove exactly 99,997,497 rows from native
footer metadata, with no row read, decode, materialization, answer cache or
fallback execution. Other successful execution and Native I/O certificates keep
`fallback_attempted=false` and `external_engine_invoked=false` explicit.

Two failed public development runs remain in the packet: an old result-completion
schema gate rejected an exploded typed field after 400 successful checks; its
repair adds field-by-field and empty-output regression tests. The next run
reached a duplicate fixture destination caused by a case-name collision after
400 checks; the corrected harness uses unique names. Earlier compile/test
observations are also retained. The prior report-integrity packet is unchanged.

Reproduce the public cases with `scripts/run_native_relational_uat.py --family all`
and both fixture-generator arguments built from the frozen revision. The embedded
`run_acceptance.py`, `prepare_full43.py`, `compare_full_uat.py` and
`finalize_acceptance.py` preserve exact guarded commands, roots, hashes and
thresholds for the complete observation. Reconstruct local roots and retained
source/reference artifacts before replaying; do not substitute unrelated inputs.

Hosted completion still depends on the nested/pivot base and the website audit's
explicit advisory decision or remediation. This work does not activate its
disabled exception, publish packages, claim total allocator/RSS coverage, admit
new state spill, or complete the broader universal-workflow and competitive gates.
