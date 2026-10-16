<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native variable-binary buffer-table resource acceptance

Native text/binary arrays now retain shared buffer-table credit through the
reviewed selection paths. Newly constructed tables receive admission before
allocation; the admitted Chunked builder accounts for growth and finalization,
and native transfer creates fresh receiving ownership. Local acceptance passes
six Rust gates, 3,587 standalone provider tests, 100 direct binding methods,
4,827 public cases and one complete 43-query pass. All 32 lifecycle cells
preserve exact serialized outputs and stay within their predeclared cost limits.
Hosted integration remains pending.

The [portable index](evidence/native-varbin-metadata-resources-2026-10-16.json),
[archive](evidence/native-varbin-metadata-resources-2026-10-16.tar.xz),
[member manifest](evidence/native-varbin-metadata-resources-2026-10-16.manifest.json.xz)
and [independent audit](evidence/native-varbin-metadata-resources-2026-10-16.audit.json.xz)
bind 18,023 members containing 124,378,172 raw bytes. Every member is reopened
and hashed. Complete outputs, schemas, source identities, provenance and
retained failures receive separate checks. This implements the finite
[ownership contract](../architecture/native-varbin-metadata-resources-2026-10-16.md)
under PERF-03/06/08/09. It makes no speedup, process-RSS, whole-gate or release
claim. The published 0.6.0 artifacts remain unchanged.

## Ownership and compatibility

The pinned Vortex 0.85.0 providers retain the existing UTF-8/binary DTypes,
16-byte views, buffer order, validity and native serialization. Managed
`VarBinViewData` shares both its immutable buffer table and its reservation.
Slice, take, filter, mask, nullability cast, canonical validity and reviewed
RunEnd expansion preserve that owner when they retain the table. Ordinary
empty-slice reduction discards unused storage and therefore releases its table
credit. An explicitly reconstructed empty view that retains the table retains
its credit too.

The reviewed constructors and FSST, VarBin and Zstd conversion paths admit
requested collection capacity before allocation. The managed non-compacting
Chunked builder reuses existing view rebasing and validity behavior while
reserving old/new capacity overlap before growth. Final conversion charges both
the temporary vector and final immutable table until the temporary allocation
is freed. Invalid input, denied growth and denied finalization preserve the
specified reusable or terminal state and release temporary owners.

Receiving deserialization uses the receiving session's allocator. The transfer
report includes provider-owned table storage while removing its superseded
outer table charge. Schema, temporary lists, slots, payload buffers and fixed
bookkeeping keep their independent contracts. Closing a producer does not
release credit still needed by a managed result; final managed ownership
releases the table credit. Cloning a raw payload buffer does not pin unrelated
table credit.

Raw constructors, `data_buffers()` and `into_data_parts()` remain explicit
caller-owned boundaries. Raw exported table handles do not constitute a
managed query-memory transfer. Legacy general builders, dictionary training,
deduplication, selective compaction, zip scratch, other provider allocation
trees and device execution retain separate boundaries. The accounting covers
requested element capacity, excluding fixed Arc headers, allocator bookkeeping,
unreported slack, OS allocation success and total process RSS. No external
query-engine route is introduced; `fallback_attempted=false` and
`external_engine_invoked=false` remain required.

The already-transitive Apache-2.0 FSST and RunEnd crates are pinned local patches
at the same 0.85.0 versions. Their original registry checksums and all 50 archive
members are verified. Supplemental upstream golden files retain exact Git-blob
and SHA-256 identities. Existing array/Zstd original provenance is unchanged,
and the Python license bundle contains all four exact provider records. The
complete lock graph changes only the two registry sources to local patches.
No codec algorithm or native wire format changes.

## Validation

| Gate | Result |
| --- | --- |
| Workspace formatting | Pass |
| Workspace Clippy, warnings denied | Pass |
| Workspace tests | 3,221 pass; zero ignored |
| Vortex read-feature Clippy | Pass |
| Release-user-surface Clippy | Pass |
| Native/provider/CLI tests | 4,315 pass; 30 predeclared manual exclusions |
| Added native regressions | All 21 pass |
| Standalone array, FSST, RunEnd and Zstd providers | 3,587 pass; two manual exclusions; all eight test/strict-library-Clippy commands pass |
| Release-script tests | 164 pass |
| Direct source-package binding | 100 methods pass; zero skips |
| Complete public matrix | 4,827 cases across 36 families pass |
| Lifecycle retention screen | 32 cells; 4,864 complete operations; exact serialized identity; no threshold breach |
| Full43 | 43 queries, one execution each; all 381 result rows verified |
| Portable inspection | 18,023 members, complete outputs and source bindings verified |

The added tests cover exact and one-byte-short grants, competing owners,
unsupported custom allocators, zero tables, checked overflow, malformed native
metadata/views, invalid iterator extents, table replacement, selection lifetime,
empty reduction, raw export, UTF-8/binary values, buffer rollover, many-leaf
concatenation, receiving-session ownership and final cleanup. Table admission
does not imply device payload reads; the layout-only device control verifies
that boundary without claiming device execution.

The archive binds all 841 provider source files used by the selected standalone
commands to the accepted sources. It preserves each derived standalone lockfile
separately from the workspace lockfile. Default and read-feature checks reuse
earlier passing commits only where the exact subsequent changes lie in
feature-excluded test modules. The portable audit verifies both source maps,
the changed-file sets and unchanged feature gates. Final formatting,
release-feature lint and native tests run on the final accepted source. The
new opt-in lifecycle harness accounts for the one added manual test exclusion
and executes separately in the frozen comparison.

Public inspection reopens 13,004 compressed JSON records, including 6,330
envelopes and 4,495 comparison files containing 39,979 expected rows. The 45
inherited pressure files contain 1,578,741 verified rows. The ordered case list
matches the predeclaration, including destinations/fanout, spill, recovery,
cancellation, unary composition and static/declared dynamic subqueries.
Independent predeclared references remain distinct from retained regression
controls. This is the existing complete public matrix; it adds no public option
or operator/type contract.

Direct tests use the source Python package with frozen native artifacts.
Installed-wheel checks, hosted checks, PR integration and deployment remain
separate pending gates. Documentation/site validation is recorded separately
after this immutable runtime packet.

The [final local review](evidence/native-varbin-metadata-final-review-2026-10-16.json.xz)
retains all 20 documentation/site/license checks, 360 checked local links and
desktop/phone browser observations. The changed guides are readable in dark
and light views without horizontal overflow; search returns both updated guides,
mobile navigation closes correctly, and no browser warning/error is recorded.
The temporary tab and local server are closed. This covers the in-app browser,
not a cross-browser matrix, and leaves all accepted runtime sources unchanged.

## Complete lifecycle cost decision

The baseline and unchanged complete lifecycle harness were committed before
runtime edits. Both optimized executables use Rust 1.99.0 and the same build
command/features. Binary identities and test inventories distinguish the two
roles: no baseline tests disappear and the candidate adds the exact 21 native
regressions. The cost snapshots bind 2,068 baseline and 2,115 candidate
Rust/Python/Cargo source files; the broader acceptance manifest additionally
includes complete vendor and package-provenance assets.

The 32 predeclared cells cover nullable UTF-8/binary, 32/65,536 rows,
canonical/FSST/Zstd-without-dictionary/RunEnd encoding and 2/16 concatenated
chunks. Each role records one warmup and three samples of 25 complete operations
per cell: 2,432 operations per role, 4,864 total. Each operation creates a
producer session, reopens/decodes native input, filters/takes/slices views,
concatenates through the admitted builder, copies into a fresh receiving
session, serializes/hashes output, closes the producer, verifies every value
and null, and releases all result/session owners. Both sessions have a 64-MiB
grant and one admitted CPU. Fixture and reference preparation stays outside
the operation clock.

Independent inspection recomputes all 64 role/cell summaries from 256 raw
records. Inputs and serialized bytes/hashes match exactly for every cell. No
declared elapsed/CPU/RSS threshold is breached, so the complete reverse-order
trigger is not reached. No sample replacement, cache flush, pacing or
battery/thermal admission is used. All raw cost clocks, thresholds, reservation
peaks and host observations remain in diagnostic evidence.

Decision: retain the finite ownership correction after complete local
acceptance. This overhead screen does not establish acceleration or resolve
broader resource coverage.

## Frozen source and Full43

The baseline is `0f8de6a913ca3bb8a170ddeb3c175c24eb521e64`; accepted runtime
source is `3d11b432fee2e36a0057c050fc3a2dc40dd3bc60`. The archived 98-file overlay
and all 2,371 selected source hashes bind the builds, direct/public execution,
Full43 and independent inspection. The source manifest SHA-256 is
`a4fbaaeed915db5fdbd7f6cdefd75d45b2393ea93770ac6679d0a7fd62045f1b`.
The release CLI SHA-256 is
`2914e628e48188c7de40fb0f992a57a1142d085b923b0fbbf6bed2ee8f2a98e2`;
the native extension SHA-256 is
`8fdc958b59c045adca8c84d8ae546627e1efc99847c138a47bda710667f994f2`.

Full43 uses the retained 100-million-row Vortex input, a 24-GiB declared query
grant, maximum parallelism 12 and one sequential execution per query. The run
records macOS 27.0, arm64 and ten reported CPUs. Same-host inspection during
documentation validation records an Apple M5 and 16 GiB physical RAM, separately
from the 24-GiB query grant. The grant is an admission policy, not a physical-memory
or whole-process limit. There is no new ingest.
The 15,713,610,513-byte input retains SHA-256
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`,
checked before and after execution. All query statements, complete retained
native reference results, schemas, raw envelopes, companion clocks and cleanup
receipts are bound to the packet. The input was prehashed; OS cache state is
uncontrolled.

Total engine time is **57.149358419 seconds**, summing
`engine_execution_seconds` for exactly one execution of every query. The
[engine-only CSV](evidence/native-varbin-metadata-resources-2026-10-16.engine-times.csv)
retains the individual observations. This is one complete regression pass;
it does not establish a speedup against an earlier observation. Other clocks
remain in diagnostic evidence.

| Query | Engine seconds |
| --- | ---: |
| Q01 | 0.000070875 |
| Q02 | 0.195852083 |
| Q03 | 0.848230834 |
| Q04 | 0.384158500 |
| Q05 | 1.341039668 |
| Q06 | 1.280173250 |
| Q07 | 0.000473291 |
| Q08 | 0.146235793 |
| Q09 | 1.247640792 |
| Q10 | 2.330001833 |
| Q11 | 1.067520249 |
| Q12 | 1.614645458 |
| Q13 | 0.679209416 |
| Q14 | 1.600270333 |
| Q15 | 1.581215250 |
| Q16 | 2.410797166 |
| Q17 | 2.475330292 |
| Q18 | 0.319970416 |
| Q19 | 4.825446750 |
| Q20 | 0.275268334 |
| Q21 | 1.066926333 |
| Q22 | 1.338090792 |
| Q23 | 3.100302042 |
| Q24 | 1.186086668 |
| Q25 | 0.332557876 |
| Q26 | 1.984672834 |
| Q27 | 2.074389250 |
| Q28 | 2.352541042 |
| Q29 | 5.387015667 |
| Q30 | 0.247950666 |
| Q31 | 1.236808625 |
| Q32 | 1.607235417 |
| Q33 | 2.007209751 |
| Q34 | 2.649358791 |
| Q35 | 2.651136209 |
| Q36 | 0.379880123 |
| Q37 | 0.425485000 |
| Q38 | 0.463057459 |
| Q39 | 0.224686165 |
| Q40 | 0.856901667 |
| Q41 | 0.568238833 |
| Q42 | 0.192740959 |
| Q43 | 0.192535667 |

## Retained failures and reproduction boundary

The archive retains twelve failed supervised build/test/lint attempts with
their original logs and child-cleanup evidence. Early expectation failures
identified newly charged table storage and the existing empty-slice reduction;
the corrected assertions derive exact table capacity and preserve raw-payload
lifetime checks. Custom test allocators implement the new external-reservation
contract. Missing upstream golden fixtures and standalone lint compatibility
are recorded separately from production behavior. Final sources and passing
commands supersede those attempts without discarding their evidence.

The archive is 23,332,364 bytes with SHA-256
`7b1c36f5e10013f4b2ebf212c97a07ee56dc70f60b85fe4c094836deae645a86`.
Large inputs and executable files remain outside it with complete identities
recorded. The manifest maps original paths to archived members. The bundle
contains the exact lifecycle harness, build recipes, query/reference files,
guards, public runners, all 100 helper-closure files and the independent
verification code. Replay requires binding local paths to resident inputs and
fresh output names while preserving the declared workload and references;
completed historical output paths must not be overwritten.

Reopening saved bytes verifies evidence integrity and consistency. It is not
another runtime execution, installed-wheel test or another platform's proof.
Other allocation trees, general builder scratch, broader operator recovery,
conditional optimizations and encoded transport retain their owners under the
[completion scope](../architecture/native-local-completion-scope-2026-10-07.md).
All CG-1 through CG-23 remain visible; this finite unit closes none wholesale.
