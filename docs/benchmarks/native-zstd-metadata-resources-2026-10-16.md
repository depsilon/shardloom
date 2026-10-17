<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Zstd frame metadata resource acceptance

Zstd frame-handle and protobuf-description vectors now receive memory admission
before construction. Clones, slices and nullability casts share one immutable
owner; native transfer constructs fresh metadata under the receiving grant.
Local acceptance passes six Rust gates, 67 standalone Zstd provider tests,
100 direct binding methods, 4,827 public cases and one complete 43-query pass.
All 18 compression lifecycle cells satisfy their frozen retention limits and
preserve identical serialized bytes. PR #1562 completes hosted integration after
all 44 checks, three installed-wheel lanes and production verification pass.

The [portable index](evidence/native-zstd-metadata-resources-2026-10-16.json),
[archive](evidence/native-zstd-metadata-resources-2026-10-16.tar.xz),
[member manifest](evidence/native-zstd-metadata-resources-2026-10-16.manifest.json.xz)
and [independent audit](evidence/native-zstd-metadata-resources-2026-10-16.audit.json.xz)
bind 13,728 members containing 84,206,438 raw bytes. Every member is reopened and
hashed; complete outputs, schemas, resource certificates and retained failures
are checked independently. This implements the finite
[metadata contract](../architecture/native-zstd-metadata-resources-2026-10-16.md)
under PERF-03/06. It makes no speedup, process-RSS, whole-gate or release claim.
The published 0.6.0 artifacts remain unchanged.

## Ownership and compatibility

The pinned Apache-2.0 Vortex 0.85.0 provider keeps its existing `ZstdData`, frame
encoding, dictionary representation, serialization and `HostAllocator` boundary.
The requested capacity of both frame vectors is checked and reserved before
fallible exact allocation. Their shared storage drops the vectors before the
reservation. Retained clones and slices therefore keep the original credit
without copying frame tables or acquiring duplicate reservations.

Deserialization scans immutable wire bytes using the already locked Prost
0.14.3 APIs before allocating a message collection. It counts top-level frame
messages, validates wire shape and buffer count, then merges into admitted
capacity. Unknown fields, repeated scalars and legacy defaults retain the
pinned parser's semantics. Malformed nested messages and checked-constructor
errors release temporary credits.

Sparse selection borrows frame descriptions and uses its existing 64-frame
limit for stack-held indices. Only selected metadata capacity is admitted.
Encoded transfer copies receiving payloads and metadata under the receiving
allocator, preserves slice coordinates, and releases the source owner when no
source references remain. The outer transfer plan still owns its schema,
temporary buffer lists and bookkeeping; its report includes provider metadata
without charging those same vectors twice.

Raw `ZstdData::new` and `into_parts` remain explicit caller-owned APIs. Export
can move unique vectors or clone shared vectors; those raw results carry no
query-memory claim. Serialization borrows metadata but its returned bytes
retain their separate output owner. Fixed Arc headers, allocator bookkeeping,
unreported allocation slack, OS allocation success, other codecs, inactive
`ZstdBuffers` and total process RSS remain outside this capacity contract.
Independent IPC still does not admit Zstd.

No dependency version, wire field, unsafe code or query-engine integration is
added. Execution remains ShardLoom-native and Vortex-native, with
`fallback_attempted=false` and `external_engine_invoked=false`.

## Validation

| Gate | Result |
| --- | --- |
| Workspace formatting | Pass |
| Workspace Clippy, warnings denied | Pass |
| Workspace tests | 3,221 pass; zero ignored |
| Vortex read-feature Clippy | Pass |
| Release-user-surface Clippy | Pass |
| Native/provider/CLI tests | 4,294 pass; 29 predeclared manual exclusions |
| Standalone patched Zstd provider | 67 pass; formatting and strict library Clippy pass |
| New focused regressions | Six vendor and nine native tests pass |
| Direct source-package binding | 100 methods pass; zero skips |
| Complete public matrix | 4,827 cases across 36 families pass |
| Lifecycle retention screen | 18 cells; exact serialized identity; no threshold breach |
| Full43 | 43 queries, one execution each; all 381 result rows verified |
| Portable inspection | 13,728 members, complete outputs and source bindings verified |

Focused tests cover exact grants, refusal before construction, count/size
overflow, default and unsupported custom allocators, sharing at a full grant,
final-owner release, producer/session closure, parse failures, empty tables,
selected-only capacity and cross-context transfer denial. Existing exact
primitive-bit, nullable Unicode/binary, dictionary, slice and serialization
controls remain in the passing suites. All 19 encoded-transfer tests also pass.

Public inspection reopens 13,004 compressed JSON records, including 4,495
comparison files with 39,979 expected rows and 6,330 envelopes. The 45 inherited
pressure files contain 1,578,741 verified rows. The complete ordered case list
matches the declaration, including destination/fanout, spill, recovery,
cancellation, unary, static subquery and declared dynamic predicate coverage.
This is regression coverage for the existing public matrix; it does not invent
new public metadata options or expand its operator/type contracts.

Direct tests use the source Python package with explicitly frozen native
artifacts. They are not installed-wheel evidence. The separate hosted evidence
below verifies installed wheels, the merged source and deployment.

The [final local review](evidence/native-zstd-metadata-final-review-2026-10-16.json.xz)
preserves source review, all required documentation/site/license checks, 408
verified local links and actual desktop/phone browser, search and navigation
observations. The first documentation attempt caught a missing canonical phase
classification label and inclusion-matrix row. Correcting those documents
passes the unchanged validators; the prior successful site build and type
checks retain their evidence. No runtime change or query rerun was needed.

## Hosted integration and deployment

[PR #1562](https://github.com/depsilon/shardloom/pull/1562) merges at
`123f19e54ff9e0d9cdb4596f5ff9ff721f6b47f7` after all 44 checks pass on
`477bbf62b51f68edb64d76676727e2582a5c2d37`. The final PR, synthetic-test and
merged trees are identical. All 2,307 accepted runtime source identities and
the local executable identities remain unchanged.

The [hosted index](evidence/native-zstd-metadata-hosted-2026-10-16.json) and
[archive](evidence/native-zstd-metadata-hosted-2026-10-16.tar.xz) retain complete
logs for six runtime/package jobs. The native/provider lane passes 2,929 tests
with 26 predeclared manual exclusions, including all nine new native regressions
and six standalone metadata regressions. The native public lane passes 1,079
tests. Each of the three installed-wheel lanes passes all 100 methods with zero
skips, isolated installation and successful uninstall. Hosted core discovers
626 tests across 57 modules, runs 526 after excluding the separate 100-method
native set, and records 14 skips.

Automated review reports no findings on
`e1c1bca8e0713d7b73d57404deea4deeccd2f413`. Inspection then found that CI's
existing standalone filter omitted the six new metadata tests. The final
four-line CI-only correction selects that module; primary review covers this
delta, and the final logs prove all six tests ran. No runtime source changed.
The record does not represent that earlier automated review as final-head
approval. Superseded check captures and the correction remain in the archive.

All sixteen checked preview/production responses preserve their corresponding
Git source. Raw assets match byte for byte; HTML differences consist only of
the recorded bounded Cloudflare insertion. Production deployment completes as
version `128130ad-e608-4382-8959-b4de3cbc7bbe`. Actual desktop and phone checks
verify both resource guides, five search results for “frame metadata”, mobile
navigation, no body overflow and no browser console errors. This covers the
in-app browser, not a cross-browser matrix. The observed deployed pages retain
their pre-closeout wording; this documentation update has separate local checks
and awaits its own integration.

The hosted archive contains 357 verified regular files, is 1,168,316 bytes and
has SHA-256
`66ddbb8728ac2ab7d88ba465532a079730d9d896205c8ba431ce4882d20ec9f4`.
Independent inspection verifies every member's name, size and hash plus the
nine JPEG signatures. Primary verification covers the source bindings, complete
job logs, served responses and recorded browser states. The original local
`passed_local` index retains its historical hosted-pending fields; the hosted
index provides the subsequent proof. No query rerun or package publication was
used for integration acceptance.

The [documentation closeout](evidence/native-zstd-metadata-closeout-2026-10-16.json.xz)
retains ten passing checks, 574 checked local links and the updated guides'
desktop/phone, search and navigation observations. All accepted runtime sources
remain unchanged. These are local closeout checks, separate from deployment of
the merged implementation above.

## Complete lifecycle cost decision

The baseline was frozen before runtime edits. Both optimized executables use
the unchanged complete compression harness: nullable integers, UTF-8 and binary;
4,096 and 65,536 rows; canonical, untrained Zstd and dictionary Zstd; the same
64-MiB grant and declared frame sizes. Each role runs one recorded warmup and
three samples of 25 complete operations per cell: 1,368 operations per role,
2,736 total. No partial samples or failures are discarded.

An operation includes creating the producer session, compression/training,
serialization and hashing, producer close, fresh-session reopen and decode,
complete reference-value/null checking and owner cleanup. Independent inspection
recomputes all 36 role/cell summaries from 144 raw records. Every cell preserves
the baseline's serialized bytes and SHA-256. No declared cost threshold is
breached, so the predeclared reverse-order trigger is not reached.

Decision: retain for its resource-ownership benefit after the passing broad
acceptance. Detailed cost clocks, process observations, thresholds and samples
remain in the archive. This finite cost screen does not establish acceleration.

## Frozen source and Full43

The source basis is `b1f4397065861d0e7def468b39f706a4610ece80` plus the archived
17-file overlay. All 2,307 frozen source identities remain unchanged through
build, direct/public execution, Full43 and independent archive inspection.
The source manifest SHA-256 is
`8de15354cd09b08a6a6fae21da55ddde940d305b367524d59ef6486e32aedc26`.
The release CLI SHA-256 is
`974b706bddf1965799b3082dd7079d3c7f6f8e7e7baa995f9397f51585cdf6d8`;
the native extension SHA-256 is
`0029043c6e4ba697d8d0e97257278b52e2502fed4872b7d8fbef91fcd6df1808`.

Full43 uses the retained 100-million-row Vortex input, 24 GiB declared query
memory, maximum parallelism 12 and one sequential execution per query. There is
no new ingest. The 15,713,610,513-byte input retains SHA-256
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`,
checked before and after execution. All statements, retained native reference
results, schemas, raw envelopes, companion clocks and cleanup receipts are
bound to the packet. The input was prehashed; OS cache state is uncontrolled.

Total engine time is **61.660146584 seconds**, summing
`engine_execution_seconds` for exactly one execution of every query. The
[engine-only CSV](evidence/native-zstd-metadata-resources-2026-10-16.engine-times.csv)
preserves the individual observations. This is a complete regression pass,
not a matched speedup comparison. Other clocks remain in diagnostic evidence.

| Query | Engine seconds |
| --- | ---: |
| Q01 | 0.000070000 |
| Q02 | 0.192692792 |
| Q03 | 0.874704124 |
| Q04 | 0.389140667 |
| Q05 | 1.353498166 |
| Q06 | 1.351463958 |
| Q07 | 0.000492709 |
| Q08 | 0.145920500 |
| Q09 | 1.568753499 |
| Q10 | 2.544250417 |
| Q11 | 1.119427791 |
| Q12 | 1.755219584 |
| Q13 | 0.736943208 |
| Q14 | 1.799771083 |
| Q15 | 1.664352793 |
| Q16 | 2.601889500 |
| Q17 | 2.344183500 |
| Q18 | 0.340744166 |
| Q19 | 5.274127084 |
| Q20 | 0.316134375 |
| Q21 | 1.186923750 |
| Q22 | 1.394060375 |
| Q23 | 3.598108582 |
| Q24 | 1.252650041 |
| Q25 | 0.367390501 |
| Q26 | 2.119007958 |
| Q27 | 2.203561208 |
| Q28 | 2.405638209 |
| Q29 | 5.810405834 |
| Q30 | 0.268305583 |
| Q31 | 1.281957626 |
| Q32 | 1.509021542 |
| Q33 | 2.066451542 |
| Q34 | 2.994868834 |
| Q35 | 2.958161625 |
| Q36 | 0.402497542 |
| Q37 | 0.487042709 |
| Q38 | 0.466891791 |
| Q39 | 0.252601001 |
| Q40 | 1.165209041 |
| Q41 | 0.669597041 |
| Q42 | 0.222553291 |
| Q43 | 0.203461042 |

## Retained failures and evidence limits

The archive keeps the original focused failures, standalone provider build/lint
attempts and transfer fixture failure. The fixture had assumed an owned slice
was directly a Zstd array; the existing ownership policy deliberately retains
the native slice wrapper. The corrected assertion checks that wrapper, its
range and shared child owner. Provider assertion/import/style corrections and
extraction of unchanged transfer-accounting calculations precede the final
source freeze and all accepted broad runs. Zero-test filter attempts remain
diagnostics and supply no passing test count.

The first portable audit rejected a missing, unchanged lifecycle harness source
member. The corrected archive includes that exact file and verifies all bound
source/helper references. The original failure and packet-repair identities
remain preserved; the completed runtime was unchanged and was not rerun for
this packaging correction. The compressed audit supplement retains the original
pre-acceptance index, successful inspection receipt and primary acceptance.

The archive is 21,807,188 bytes with SHA-256
`f4daf64835d46c784144b80c2e307423d42a7b6a30a15ea830aefef00db51905`.
Large input and executable files stay outside the portable archive with their
full identities recorded. Reopening saved evidence verifies integrity and
consistency; it is not another runtime execution or proof on another platform.

Other allocation trees, resident operator state, broader recovery, conditional
optimizations and encoded transport retain their existing owners under the
[completion scope](../architecture/native-local-completion-scope-2026-10-07.md).
All CG-1 through CG-23 remain visible; this finite unit closes none wholesale.
