<!-- SPDX-License-Identifier: Apache-2.0 -->

# Reviewed native variable-binary buffer-table acceptance

The revised buffer-table implementation passes complete local acceptance.
Safe construction and replacement now reject malformed host-readable views,
including invalid UTF-8, prefixes, ranges and buffer indices. Checked reuse
avoids validating unchanged native values again while preserving table admission,
shared ownership and fresh receiving-session credit. Six Rust gates, 3,587
standalone provider tests, 100 direct binding methods, 4,827 public cases and one
complete Full43 pass succeed. The frozen lifecycle screen has no repeated cost
breach across its two comparison orders. Hosted integration remains pending.

This report supersedes the [initial acceptance report](native-varbin-metadata-resources-2026-10-16.md)
for the revised source. It implements the same finite
[ownership contract](../architecture/native-varbin-metadata-resources-2026-10-16.md)
under PERF-03/06/08/09. The initial evidence and six rejected comparisons remain
available with their original decisions. No speedup, whole-process resource,
competitive-gate or release claim follows; published 0.6.0 artifacts are unchanged.

## Safe values and ownership

The provider decision remains `use_vortex_native_provider`: extend the pinned
Vortex 0.85.0 array, FSST, RunEnd and Zstd providers inside the existing native
feature and certificate boundaries. UTF-8/binary DTypes, 16-byte views, validity,
buffer order and serialization are preserved. No external execution engine or
Arrow normalization is introduced. Native execution retains
`fallback_attempted=false` and `external_engine_invoked=false`.

Safe handle construction validates every host-readable view, including null
slots. Borrowed-buffer construction validates non-null values and normalizes
null payloads. Arbitrary view replacement validates contents; reuse requires
identical complete host view bytes against the unchanged payload table. Full
buffer replacement additionally requires equal lengths, buffer count and every
ordered host payload byte. Newly accessible device-to-host contents are checked.
Opaque device handles remain opaque and imply no device-execution support.

Encoding data records whether it was constructed for UTF-8. Checked public
array-parts reconstruction cannot pair binary data with a UTF-8 dtype without
validating the host-readable values. The marker is not serialized. Proven native
selection and non-compacting append use provider helpers with explicit safety
arguments; their shape, dtype, validity and ownership checks remain. The exact
single-buffer UTF-8 builder checks the complete heap and every value boundary
before exposing constructed views. Changed or unproven contents retain ordinary
validation. FSST attaches its separate payload/view owners through checked
byte-identical replacement rather than repeating validation of those bytes.

The immutable buffer table and its reservation share a lifetime. Reviewed
selection paths retain both; native transfer creates a distinct receiving table.
Requested-capacity admission covers temporary/final table overlap and managed
builder growth/finalization. Denial, malformed input and normal teardown release
the corresponding owners. Raw constructor/export boundaries, general builder,
deduplication/compaction scratch, fixed Arc/allocator overhead and process RSS
remain outside this finite contract.

## Validation

| Gate | Result |
| --- | --- |
| Workspace formatting and strict Clippy | Pass |
| Workspace tests | 3,221 pass; zero ignored |
| Vortex read-feature and release-user-surface Clippy | Pass |
| Native/provider/CLI tests | 4,319 pass; 30 predeclared manual exclusions |
| Added native regressions | All 25 pass |
| Standalone array, FSST, RunEnd and Zstd providers | 3,587 pass; two manual exclusions; all eight test/strict-library-Clippy commands pass |
| Release-script tests | 164 pass |
| Direct source-package binding | 100 methods pass; zero skips |
| Complete public matrix | 4,827 cases across 36 families pass |
| Lifecycle retention screen | 32 cells in each order; 9,728 complete operations; exact input/output identity; no repeated cost breach |
| Full43 | 43 queries, one execution each; all 381 result rows and schemas verified |
| Portable inspection | All 19,538 members and four archive partitions verified |

All six Rust gates and eight provider commands ran freshly on the accepted
revision. The provider audit checks all 841 selected source files and preserves
the standalone lockfiles separately from the workspace lockfile. Registry
provenance verifies all 50 original FSST/RunEnd archive members, license records
and published dirty-VCS markers. Existing array/Zstd provenance and the Python
license bundle remain bound to the source manifest.

Public inspection reopens 13,004 compressed JSON records, including 6,330
envelopes and 4,495 value files with 39,979 expected rows. The 45 inherited
pressure files contain 1,578,741 verified rows. The ordered case list matches
the declaration, including streaming destinations/fanout, spill, recovery,
cancellation, unary composition and static/declared dynamic subqueries.
Independent predeclared references remain distinct from retained native
regression controls. Direct tests use the source Python package with frozen
native artifacts; installed-wheel proof remains a separate gate.

Three older tests deliberately supplied malformed text through a safe
constructor and expected a downstream failure. The constructor now correctly
refuses those fixtures earlier. The tests assert that earlier refusal, retain
valid downstream controls and verify cleanup; they do not inject unsafe arrays
to bypass the new invariant. All three pass in the fresh complete native suite.
The original failing run and focused correction evidence are preserved.

The [final local review](evidence/native-varbin-metadata-review-final-2026-10-16.json.xz)
records all 20 documentation/site/license checks and desktop/phone inspection.
Both revised guides render without horizontal overflow; dark/light views,
three-page search results and mobile navigation pass. No browser warning/error
is recorded. The original theme is restored, the temporary viewport override is
cleared, and the owned tab/server are closed. This is in-app browser evidence,
not a cross-browser or hosted test. Accepted runtime sources remain unchanged.

## Complete lifecycle cost decision

The baseline, harness, 32-cell grid and retention rule were frozen before
implementation. Both roles use Rust 1.99.0, the same optimized build recipe and
the same complete operation. The cells cover nullable UTF-8/binary, 32/65,536
rows, canonical/FSST/Zstd-without-dictionary/RunEnd encoding and 2/16 concatenated
chunks. Each role executes one warmup operation and three samples of 25 operations
per cell: 2,432 operations per role per order, 4,864 per order and 9,728 overall.

Each operation creates the producer session, reopens/decodes input, applies
filter/take/slice, concatenates through the admitted builder, copies to a fresh
receiving session, serializes/hashes native output, closes the producer, checks
every value/null and releases all owners. Each session has a 64-MiB grant and
one admitted CPU. Fixture/reference preparation remains outside that clock.

The first order triggers the predeclared reverse-order gate in two elapsed-cost
cells: RunEnd UTF-8 and binary, 65,536 rows and two chunks. The complete reverse
order has no declared breach. Therefore no cell or process metric breaches its
limit in both orders, and the unchanged rule permits retention. Independent
inspection recomputes 64 role/cell summaries from 256 raw records in each order;
every input and native serialized output matches exactly. No sample replacement,
cache flush, pacing or battery/thermal admission is used. Raw cost clocks,
thresholds, reservation peaks and host observations remain diagnostic evidence.

The six rejected comparisons retain their complete source snapshots, raw
observations and original repeated-breach decisions:

| Archived comparison | Repeated breaches |
| --- | ---: |
| 550 | 13 |
| 552 | 2 |
| 553 | 1 |
| 554 | 1 |
| 555 | 1 |
| 556 | 1 |

A separate FSST profile informed the final checked-reuse correction. Its clocks
are excluded from acceptance. The retained comparison is 557, measured at
`c08e044e19e7e709431bf76e8faa6aab5f46bb4a`. Accepted revision
`2d5d82a1e810a86d7da1827bcc9a2ec08993b65e` changes only the three `cfg(test)`
fixture files described above; 2,368 of 2,371 selected files are byte-identical.
The portable audit checks the direct parent relationship, exact changed files,
unchanged test-module gates and passing replacement tests. Production source
and the lifecycle harness are unchanged, so the cost comparison is retained
without relabeling its commit or rerunning its observations.

Decision: retain the reviewed finite ownership correction after complete local
acceptance. This overhead screen does not establish workload acceleration or
complete broader allocation coverage.

## Frozen source and Full43

The unchanged-production baseline is
`0f8de6a913ca3bb8a170ddeb3c175c24eb521e64`. Fresh builds and broad runtime checks
use accepted source `2d5d82a1e810a86d7da1827bcc9a2ec08993b65e`, with a 101-file
source overlay and all 2,371 selected source identities. The source manifest
SHA-256 is `a01970527dfcd9d5abe48e772750a17d1ddac231bdd67eff3c478541fdbe87a7`.
The release CLI SHA-256 is
`d60a9ddb863dd940bf63bd4ddd87fd7f7129b65d42a43b8ec2759fa441de8fcd`;
the native extension SHA-256 is
`3744bd6c4562f87862d5b18272b27a9ac578c9eb8641f572f671ce1f74929068`.

Full43 uses the retained 100-million-row Vortex input, a 24-GiB declared query
grant, maximum parallelism 12 and one sequential execution per query. The host
records macOS 27.0, arm64 and ten reported CPUs. Same-host inspection during
documentation validation records an Apple M5 and 16 GiB physical RAM, separately
from the 24-GiB query grant. The grant is an admission policy, not a physical-RAM
or whole-process limit. There is no new ingest.
The 15,713,610,513-byte input retains SHA-256
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`,
checked before and after execution. The input was prehashed; OS cache state is
uncontrolled. Complete retained native reference results, schemas, query text,
raw envelopes, companion clocks and cleanup receipts are bound to the packet.

Total engine time is **69.144125536 seconds**, summing
`engine_execution_seconds` for exactly one execution of every query. The
[engine-only CSV](evidence/native-varbin-metadata-review-2026-10-16.engine-times.csv)
retains all observations. This is one complete regression pass and does not
establish a speedup or regression against an earlier unpaired observation.
Other clocks remain in diagnostic evidence.

| Query | Engine seconds |
| --- | ---: |
| Q01 | 0.000077958 |
| Q02 | 0.191365666 |
| Q03 | 0.892322208 |
| Q04 | 0.396039875 |
| Q05 | 1.347795625 |
| Q06 | 1.940504001 |
| Q07 | 0.000513458 |
| Q08 | 0.160258959 |
| Q09 | 1.619219125 |
| Q10 | 2.543748708 |
| Q11 | 1.330986250 |
| Q12 | 1.889346874 |
| Q13 | 0.791392415 |
| Q14 | 1.849976833 |
| Q15 | 1.649030500 |
| Q16 | 2.541906042 |
| Q17 | 2.604921042 |
| Q18 | 0.342852584 |
| Q19 | 6.496495457 |
| Q20 | 0.311568916 |
| Q21 | 1.435037667 |
| Q22 | 1.719297417 |
| Q23 | 4.275289624 |
| Q24 | 1.536895376 |
| Q25 | 0.362669625 |
| Q26 | 2.699459124 |
| Q27 | 2.786737542 |
| Q28 | 2.331475875 |
| Q29 | 7.505235834 |
| Q30 | 0.271426457 |
| Q31 | 1.318990167 |
| Q32 | 1.563754083 |
| Q33 | 2.177571999 |
| Q34 | 3.473973000 |
| Q35 | 3.246928000 |
| Q36 | 0.407304249 |
| Q37 | 0.434527084 |
| Q38 | 0.497810041 |
| Q39 | 0.237050833 |
| Q40 | 0.946816917 |
| Q41 | 0.592642792 |
| Q42 | 0.214400334 |
| Q43 | 0.208509000 |

## Portable evidence and reproduction

The [portable index](evidence/native-varbin-metadata-review-2026-10-16.json),
[core archive](evidence/native-varbin-metadata-review-2026-10-16.tar.xz),
[history partition 1](evidence/native-varbin-metadata-review-2026-10-16.historical-sources-1.tar.xz),
[history partition 2](evidence/native-varbin-metadata-review-2026-10-16.historical-sources-2.tar.xz),
[history partition 3](evidence/native-varbin-metadata-review-2026-10-16.historical-sources-3.tar.xz),
[member manifest](evidence/native-varbin-metadata-review-2026-10-16.manifest.json.xz)
and [independent audit](evidence/native-varbin-metadata-review-2026-10-16.audit.json.xz)
bind 19,538 members containing 473,257,357 raw bytes. Every archive and member
is reopened and hashed; the historical source snapshots receive separate
content verification. The index records each partition's identity and limits.
Large inputs and executables remain outside the packet with complete identities.

The first packaging attempt refused cost receipts from the earlier measured
commit because it incorrectly required the later test-only revision everywhere.
The corrected packager checks each receipt against its declared source revision
and retains all completion, cleanup and log-identity requirements. Focused
negative checks reject a wrong commit, altered log and incomplete cleanup.
The failed attempt and correction evidence remain archived. Runtime observations
and their source identities were not changed to satisfy packaging.

The manifest maps original paths to archived members. The packet contains the
lifecycle harness, build recipes, frozen declarations and references, guards,
public runners, source reviews and portable verification code. Replay requires
binding local paths to resident inputs and fresh output names while preserving
the declared workload and references; completed evidence must not be overwritten.
Reopening archived bytes proves evidence integrity and consistency, not another
runtime execution or another platform's behavior.

Documentation/site checks and local browser inspection pass separately from
runtime acceptance. Hosted checks, installed wheels, PR integration and
production verification remain separate follow-through gates.
Remaining allocation trees, general builder scratch, broader operational work,
conditional optimizations and encoded transport retain their owners under the
[completion scope](../architecture/native-local-completion-scope-2026-10-07.md).
All CG-1 through CG-23 remain visible; this finite unit closes none wholesale.
