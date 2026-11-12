# Local Development Storage

## Current retention policy

The November 7 managed-binary, dependency and fixed-tensor integration closeouts
apply this policy after their exact merges pass all 55 required main checks,
following 58 checks on each PR. The [binary delivery](native-managed-binary-objects-2026-11-06.md)
retires 29,312 obsolete regular files and nine environment links. The subsequent
[dependency intake](dependency-intake-2026-11-06.md) retires another 10,042 regular
files and eight links, replacing the binary runtime/wheel with the accepted
dependency runtime/wheel. Fixed tensor PR #1661 merged as
`740f682eb89191bdd5b85caec96cbb2e2c9abea6`; its 58 PR checks and 55 exact-main
checks passed. Its retirement removed 9,995 regular files and 3 symlinks, and
post-retirement replay passed. Repeated identities, closed-handle checks,
unchanged input generations and post-retirement evidence replay pass. All 88
retired raw dependency-intake files remain byte-exact in the committed archive.
Variable-tensor PR #1662 merged as `c47eeefa05012cce32b053f53af18583041c9f19`
after all 58 PR checks passed; all 55 exact-main checks then passed. Verified
retirement removed 9,996 obsolete regular files and three environment links; post-retirement
replay passed. The current inputs, accepted variable-tensor runtime/wheel,
compact packet and original failures remain retained. No payload backups or
storage-limit changes were made.

The variable-tensor native suite initially stopped at the unchanged free-space
floor before executing tests. Verified retirement removed 22,392 reproducible
debug compiler files after repeated identity and closed-handle checks. The
release cache, current inputs and retained runtime/wheel stayed unchanged;
the restarted native suite passed. The compact retirement inventory and stopped
run remain in the
[variable-tensor evidence packet](../release/evidence/native-variable-shape-tensor-2026-11-07/README.md).

During Float16 validation, verified retirement removed 14,909 reproducible debug
compiler files after identity and closed-handle checks. Current inputs, the
accepted variable-tensor runtime/wheel and the release cache remained unchanged.
The [Float16 packet](../release/evidence/native-float16-lifecycle-2026-11-07/README.md)
preserves that validation retirement, all 20 failed or interrupted observations
and the final passing default/native, feature and installed-package checks.

PR #1663 subsequently merged after 58 PR checks passed; all 55 exact-main checks
then passed. Its separate closeout retirement removed 19,953 regular files and six symlinks
after identity and closed-handle checks. Current inputs and the accepted
Float16 runtime/wheel remained unchanged, and post-retirement replay passed.
At that closeout checkpoint, the tested Float16 runtime and wheel remained
retained; the subsequent vector integration below superseded and retired them. Five independent
Dependabot PRs (#1664–#1668) remain open for separate dependency review. Storage
limits are unchanged, and no payload backups were created.

During November 7 sparse/quantized vector validation, verified debug-cache
retirement removed 11,883 regular files among 13,963 entries after identity and
closed-handle checks. The retained runtimes and wheel, input generations and
release cache remained unchanged; the storage floor did not change and no
payload backup was created. The
[vector packet](../release/evidence/native-vector-representations-2026-11-07/README.md)
preserves this observation. Current inputs and the accepted Float16 runtime/wheel
remained retained at local acceptance. PR #1669 subsequently merged as
`a157a5ff86c50ff253300848950fed207ce3d72d` after all 58 PR checks and all 55
exact-main checks passed with review clear. Its verified closeout retirement
removed 11,596 entries, including 10,226 regular files and three symlinks;
current inputs and the vector runtime-r2/wheel remained unchanged, and portable
replay passed after retirement. The Float16 runtime/wheel were superseded and
retired. No storage-limit change or payload backup was made. Five independent
Dependabot PRs (#1664–#1668) remain open for separate maintenance.

During November 8 provider validation, guarded cache retirement preserved each
stopped run and its diagnostics. The final debug retirement removed 20,077
entries, including 17,173 regular files, after repeated identities and closed-handle
checks. The accepted vector runtime/wheel, the separate small-dependency comparison
runtime and installed proof, current input generations and release cache remained
unchanged. The final array test/lint check and fresh release build then passed.
The [provider evidence packet](../release/evidence/native-provider-upgrade-2026-11-08/README.md)
retains these receipts and all earlier outcomes. Its local acceptance includes
the new runtime and all 386 isolated installed-wheel tests with zero skips.
PR #1670 subsequently merged as `d29bff42c5b3aacf7a61463bf2c8f9cec2bdf216`
after all 58 PR checks passed with review clear; all 55 required exact-main
checks passed. Its verified closeout retirement removed 40,614 entries,
including 35,766 regular files and six environment links. Repeated identities
and closed-handle checks passed; current inputs and the provider runtime-r1 and
installed-tested wheel remained unchanged. Complete portable replay passed
after retirement. The superseded vector and small-dependency comparison
runtimes/wheels, completed install environments and verified temporary source
copies were retired. The current test wheelhouse and compiler cache remain in
use for the next queue unit. All five Dependabot proposals are now resolved.
No storage floor changed and no payload backup was created.

During shared-vector expression validation, verified debug-cache retirement
removed 12,773 entries, including 10,842 regular files, after final native tests
and strict lint completed. Repeated identities and closed-handle checks passed;
the accepted provider runtime/wheel, the earlier vector-expression comparison
runtime, current input generations and release cache remained unchanged. The
fresh release build, public checks and all 397 installed-wheel tests subsequently
passed. The [vector-expression packet](../release/evidence/native-vector-expressions-2026-11-08/README.md)
preserves this retirement and every original failed observation. At local
acceptance, the provider runtime/wheel and the newly tested vector-expression
runtime/wheel remained retained through hosted closeout. The subsequent
retirement below supersedes the provider artifacts. Storage floors are unchanged
and no payload backup was created.

The [proof-supervision follow-up](../release/evidence/native-vector-proof-supervision-2026-11-08/README.md)
retains both original hosted deadline failures, the fresh local 397-test installed
proof and the focused deadline regressions. The original vector-expression packet
is unchanged; the follow-up stores only new observations and source bytes absent
from that packet. The tested runtime is reused byte-for-byte. The original
unexecuted retirement plan remains historical; the subsequent closeout used a
new inventory covering the follow-up and retained the accepted runtime/wheel
and both packets.

PR #1671 merged as `9c4ada03e1c30ed20fd2118c78a8be3fb69b0dd8` after all 58 PR
checks passed with review clear; all 55 exact-main checks then passed. Its
[verified closeout](native-vector-expressions-2026-11-08.md#hosted-integration-and-cleanup)
retired 27,810 entries, including 25,117 obsolete regular files and six environment
links, totaling 1,439,575,790 logical regular-file bytes. Repeated inventories,
identities and closed-handle checks passed. Current inputs, the accepted
vector-expression runtime and installed-tested wheel, both committed packets
and original failed observations remained unchanged. Complete source-bound
replay passed after retirement. Superseded provider and comparison runtimes,
completed install environments and verified temporary source/evidence copies
were retired without a payload backup or storage-limit change.

The associated storage-maintenance pass separately retired 10,214 reproducible
debug files (3,747,425,410 logical bytes), preserving the release cache and all
retained runtime/input identities; both vector packets replayed afterward.
Lossless result compaction retired another 19,720 raw result files
(1,131,320,030 logical bytes). Their exact bytes remain recoverable from 637
existing archives and ten verified compressed JSONL records totaling 1,526,244
bytes; original failed summaries and archives are unchanged. The three
operations retired 55,051 obsolete regular files totaling about 5.9 GiB of
logical bytes, not a measurement of physical space reclaimed. Current input
generations remain protected, including the retained ingest-ownership Vortex
input; unclassified older outputs were not retired by this pass.

The November 3 source milestone retired two temporary copies of the local I/O
packet after relocated replay. All 66 files (375,519,166 logical bytes) matched
the committed collection. Repeated identity checks and closed-handle checks
preceded removal. The source milestone retains the compact retirement inventory
and the original failed and passed replay outcomes. [Acceptance](../release/evidence/v1.1.0-source-milestone-2026-11-03/acceptance.json).

During November 9 wide-schema validation, three verified debug-cache retirements
removed 17,030, 13,741 and 9,426 regular files respectively. Each used repeated
identities and closed-handle checks and retained its exact completed-check
basis. Current input generations, the accepted vector-expression runtime/wheel,
wide-schema comparison runtimes and release cache remained unchanged. The
[wide-schema packet](../release/evidence/native-wide-schema-2026-11-09/README.md)
preserves the inventories, original stopped/failed checks and later passing
regeneration checks. No storage floor changed and no payload backup was created.
These are reproducible debug artifacts. The subsequent wide-schema ready-PR
closeout retired 25,652 regular files and six environment links after repeated
identities and closed-handle checks. Current inputs, both retained runtime/wheel
sets and the compact packet remained unchanged; replay passed after retirement.
PRs #1672 and #1673 then merged, and all 55 checks on exact main
`e85e379588d8ea74a6ce6ee72e3e407ad15a3b49` passed. The local packet and
ready-PR closeout retain their original pre-merge status; the later main result
is preserved with the Variant evidence below.

During November 9 Variant validation, verified debug-cache retirements removed
13,137 and 7,118 reproducible regular files. A separate retirement removed 41
regular files from six exact interrupted-test fixture directories. Repeated
identities and closed-handle checks preceded removal. Current input generations,
the accepted vector-expression and wide-schema runtimes/wheels, the Variant
comparison runtime and the release cache remained unchanged. The original
free-space refusals remain recorded; the complete native suite subsequently
passed with four test workers and unchanged cases, deadlines and storage limits.
The [Variant packet](../release/evidence/native-variant-lifecycle-2026-11-09/README.md)
retains each operation's exact completed or interrupted basis, manifests and
receipts. No storage floor changed and no payload backup was created. The newly
tested runtime/wheel and active predecessors remain retained through hosted
closeout; final runtime retirement is separate from these compiler/test cleanups.

PR #1674 subsequently merged as `0cae77b442c07d62911a50e2dc6b78067130beca`
after all 58 PR checks passed with review clear; all 55 exact-main checks passed.
Verified closeout retired 17,121 entries, including 15,751 regular files and
three environment links, totaling 1,721,762,403 logical regular-file bytes.
Repeated identities and closed-handle checks passed. Current inputs, the
accepted Variant runtime-r2 and installed-tested wheel, committed packets and
original failed observations remained unchanged; complete source-bound replay
passed afterward. Superseded vector-expression/wide-schema runtimes and wheels,
the earlier Variant comparison runtime, completed install staging/environments
and verified temporary source/evidence copies were retired. No payload backup
or storage-limit change occurred. The
[UUID evidence](../release/evidence/native-uuid-lifecycle-2026-11-10/README.md)
retains the exact separate hosted closeout.

During UUID preparation, verified retirement removed 14,187 entries, including
12,071 reproducible regular files, from the debug compiler cache and two
superseded preparation runtimes. Current inputs, retained runtimes/wheels and
the release cache remained unchanged. Full default/native tests, strict lint,
the fresh release build and all 431 installed-wheel integrations subsequently
passed. Original failures and the exact preparation retirement remain in the
UUID packet. At local acceptance, the new runtime/wheel and accepted Variant
runtime/wheel remained retained through UUID hosted integration. The following
separate closeout supersedes that earlier Variant runtime/wheel.

PR #1675 merged as `8df722749e3993bb27a9cdd573de8a2a3860bb34` after all 58 PR
checks passed with review clear; all 55 exact-main checks then passed. Verified
closeout retired 17,022 entries, including 15,663 regular files and three
environment links, totaling 1,679,625,582 logical regular-file bytes. Repeated
identities and closed-handle checks passed. All 220 current input generations,
the accepted UUID runtime-r1 and installed-tested wheel, committed packets and
original failures remained unchanged; complete source-bound replay passed
after retirement. Superseded Variant runtime/wheel, UUID preparation runtime,
completed staging/environment and verified temporary source/evidence copies
were retired. No payload backup or storage-limit change occurred. The Variant
field-expression evidence retains this separate hosted closeout.

During Variant field-expression preparation, the unchanged free-space floor
stopped a compile before tests ran. Verified retirement removed 15,624 entries,
including 13,299 regular files and no symlinks, from the debug compiler cache
and one superseded preparation runtime. Repeated identities and closed-handle
checks preserved eight retained runtime/wheel files, all 220 current input
generations and the release cache. No payload backup or storage-limit change
was made. The complete default/native tests, strict lint, feature/supported-Rust
checks, fresh release runtime and all 441 installed-wheel integrations later
passed. The field-expression evidence retains the interrupted run, original
failures and preparation-retirement manifest; final hosted retirement is separate.

During the [dictionary callback repair](native-dictionary-execution-repair-2026-11-10.md),
the first regression attempt stopped at the unchanged 12 GiB free-space floor.
Verified debug-cache retirement removed 15,597 entries, including 13,280 regular
files totaling 8,960,177,157 logical bytes, with no symlinks. Repeated identity
and closed-handle checks preserved all 220 current input generations, 13 protected
runtime/wheel/evidence files and the release cache. No payload backup or limit
change was made. The original stopped attempt remains recorded. Default/native
checks, a fresh release build and the isolated 517-case repair cohort subsequently
passed. The failed original 10,000-case observation remains immutable evidence,
and accepted/comparison runtimes remained protected through hosted closeout.
PR #1677 subsequently merged as `037b6801320aa0bfe939ba10aed9304198b77a5a`
after all 58 PR checks passed with review clear; all 55 exact-main checks passed.
Verified retirement removed 47 task-owned roots containing 28,975 entries,
including 27,400 regular files and four symlinks. An earlier hardlink stop and
the separate retirement of 54 verified fixture aliases remain recorded; their
five retained originals were preserved. All 220 input generations and 551
retained files stayed unchanged, and all three portable replays passed after
retirement. The repaired runtime remains retained for the
[shared-index comparison](native-shared-index-metadata-2026-11-10.md), alongside
that candidate's tested runtime through its hosted acceptance. No storage limit
changed and no payload backup was created.

Shared-index PR #1678 subsequently merged as
`a640475ae66bc36cefcae42088dc23da6776e8ed` after all 58 PR checks passed with
review clear; all 55 exact-main checks passed. Verified retirement removed
40 task-owned roots containing 5,826 entries, including 5,822 regular files
and no symlinks, totaling 1,508,581,322 logical bytes. Repeated identities and
closed-handle checks passed; 633 retained files and all 220 input generations
remained unchanged. The superseded dictionary runtime, four redundant raw
ledgers and reconstructible packet/source copies were retired with exact
retained replacements. Both post-retirement replays passed. The accepted index
runtime remains retained for the independent window comparison. No storage
limit changed and no payload backup was created.

During [compatible-window validation](native-shared-window-state-2026-11-11.md),
verified debug-cache retirement removed 11,934 entries, including 10,163 regular
files and no symlinks, totaling 7,001,904,381 logical bytes. Repeated identities
and closed-handle checks preserved 146 protected runtime/evidence files, all
220 input generations and the release cache. The fresh release build and fixed
1,212-case candidate subsequently passed. Its tested runtime and the accepted
index runtime remain retained through hosted integration; final retirement is
separate. Original failures and compact retirement receipts remain preserved.
No storage floor changed and no payload backup was created.

During the [rolling DISTINCT investigation](native-rolling-distinct-2026-11-11.md),
verified debug-cache retirement removed 11,933 entries, including 10,162 regular
files and no symlinks, totaling 7,003,094,877 logical bytes. Repeated identities
and closed-handle checks preserved current inputs, accepted runtimes and the
release cache. The candidate subsequently passed all 1,090 requests but was
rejected under the complete-cohort engine-time rule. Its source snapshots and
observations remain retained, and the accepted runtime is unchanged. Retirement
of the rejected runtime is separate and follows portable/hosted closeout.
No storage floor changed and no payload backup was created.

The October 20 maintainer decision applies to **all ShardLoom local generated
storage**, including prior investigations, release verification, build caches and
worktree outputs. Cleanup is not limited to the experiment that reaches a limit.

Retain current source inputs, the latest accepted output and tested runtime,
artifacts still needed by an active comparison, and compact results. Results
include the measured values, correctness outcomes, failures, source revision,
build/features, input identities and the protocol needed to understand them.
Large result records may be compressed with verified readback. Do not retain old
executables, compiler caches, environments, source bundles or generated datasets
as rollback backups. Git source and ordinary project files are separate from
disposable generated artifacts.

After validation, retire completed temporary outputs and verbose per-output
validation dumps once their results have been recorded. A failed or incomplete
run keeps its outcome and useful diagnostics; retaining that result does not
require keeping every generated payload. An active comparison keeps the inputs
and observations needed to reach its decision, then releases those temporary
artifacts. Preserve the existing storage, concurrency and process-cleanup guards;
increasing a limit is not a retention policy.

Before deletion, identify the exact generated paths, confirm that they are no
longer in use, and verify the retained replacement or regeneration basis. Store
a compact cleanup result. Do not create another full archive of the files being
retired. This policy supersedes the older local-residency and backup-retention
statements below; their measurements remain historical results.

The [November 2 adaptive-decision closeout](../benchmarks/evidence/native-adaptive-decisions-closeout-2026-11-02/closeout.json)
retires 32,458 generated regular files and nine environment links after
PR #1644 passes all 58 hosted checks and merges. Five superseded CLI/library
pairs, old wheels and completed package/install environments are removed with
repeated identity and closed-handle checks. The accepted runtime and wheel,
current inputs, compact reports and all original observations remain. The
compressed retirement inventory contains identities only; no payload backup,
query rerun, source-worktree deletion or storage-limit increase occurs.

The [October 31 reduced performance-batch retirement](../benchmarks/evidence/native-performance-batch-retirement-2026-10-31/cleanup.json)
removes 58,542 completed generated regular files and 15 environment links after
complete portable acceptance, 28 corruption controls and isolated source-only
replay. It retires five superseded/reference runtime pairs, four old wheels,
completed package/install environments, duplicate verification copies and raw
output/log trees. All 8,308 retired raw files remain byte-exact in the compact
packet; original rejected results and failed attempts remain visible. Repeated
identity and closed-handle checks pass, and complete replay still passes after
retirement. The accepted B6/B9 runtime and installed-tested wheel, current inputs
and compact evidence remain; the prior ordered-input runtime/wheel stays through
hosted integration. Source worktrees, the shared compiler cache, unrelated files
and storage ceilings are unchanged. No payload backup is created.

The [October 31 typed-ordering rejection cleanup](../benchmarks/evidence/native-typed-ordering-retirement-2026-10-31/cleanup.json)
retires 13,470 generated regular files and three environment links after exact
accepted-source restoration and isolated source-only verification. The rejected
runtime/wheel, completed package environment, duplicate packet and two verification
copies are removed; 2,850 raw comparison/output files remain byte-exact in the
portable packet. Repeated identities, closed-handle checks and post-retirement
replay pass. The accepted ordered-input runtime/wheel, original source/reference
fixtures, all compact observations and failed attempts remain. No compiler cache,
unrelated user files, payload backup or storage ceiling is changed.

The [October 31 ordered-input closeout](../benchmarks/evidence/native-ordered-inputs-closeout-2026-10-31/closeout.json)
records #1632 merged with all 57 hosted checks passing. The earlier temporary
cleanup retires 13,872 regular files and three environment links, retaining the
accepted runtime and compact evidence. After merge, the three superseded
grouped-state CLI/library/wheel files are retired with repeated identities and
closed-handle checks. Ordered-input and grouped-state saved evidence both replay
after retirement. Current inputs, the accepted ordered-input runtime/wheel,
all results and failures remain; no payload backup or storage-limit change occurs.

The [October 31 grouped-state closeout](../benchmarks/evidence/native-grouped-state-closeout-2026-10-31/closeout.json)
retires 2,863 generated files after #1630 merges with all 57 hosted checks passing.
Superseded application/bounded-order binaries and wheels, archived output parts
and completed verification copies are removed with repeated identities and
closed-handle checks. The current grouped-state runtime/wheel, five original
Parquet inputs, public summary and complete compact results remain. Both
grouped-state and cost packets replay after retirement. The failed first
retirement-plan provenance lookup and exact correction remain recorded; no
payload backup, compiler-cache removal or storage-ceiling change occurs.

The [October 31 rejected adjacent-read retirement](../benchmarks/evidence/native-adjacent-parquet-reads-retirement-2026-10-31/cleanup.json)
removes 12,351 generated regular files and three environment links after exact
accepted-source restoration, 11 corruption controls and isolated source-only
replay. It retires the rejected CLI/library copies and wheel, completed package
environment, duplicate packet and verification sources, and archived raw result
parts/logs. Both public summaries, all compact observations/failures/source
evidence, five original Parquet inputs and the current grouped-state runtime
and wheel remain. Repeated identities, closed-handle checks and complete
post-retirement replay pass. The shared compiler cache and unrelated files are
untouched; no payload backup or storage-ceiling change occurs.

The [October 28 numeric-value cleanup](../benchmarks/evidence/native-shared-numeric-values-2026-10-28/cleanup.json)
retires 23,269 completed regular files and six environment symlinks after complete
portable replay, 30 corruption controls and isolated source-only acceptance.
Repeated identities and closed-handle checks cover completed environments,
staged packets, archived outputs and the superseded key-lookup runtime/wheel.
The latest accepted numeric runtime and installed-tested wheel, five current
Parquet inputs, 347 independent references and complete compact results remain.
Replay passes after retirement. Shared compiler outputs and unrelated files
are untouched; no payload backup or storage-ceiling change occurs.

The [1.0.0 release cleanup](../release/evidence/v1.0.0-local-generated-cleanup-2026-10-25.json)
retires 30,484 generated regular files and 21 environment links after release,
installed-package and documentation verification. It covers seven completed
installation environments, completed compiler directories, temporary source
and package copies, 16 verified registry downloads and five superseded or
explicitly dropped runtime files. Repeated identity reads, closed-handle checks
and supervised process cleanup pass. The tested 1.0.0 CLI, native library and
installed-tested PyPI wheel now form the retained runtime set; the Homebrew
installation, current source inputs, accepted Vortex output and compact
source/result/failure evidence remain. The packet records identities and results,
not payload backups. These runtime identities supersede earlier retention
statements below without changing their measurements or storage thresholds.

The [October 21 cyclic-existence cleanup](../benchmarks/evidence/cyclic-existence-2026-10-21/cleanup.json)
applies this policy after final integration: 35,888 regular generated files and
three environment symlinks are retired with exact identities and closed-handle
checks. The latest accepted CLI/library and installed-tested wheel replace the
earlier multiway runtime/wheel; completed public/format payloads, comparison
files and unpacked package environments are regenerated from their recorded
sources when needed. Original Parquet and `fresh-roadmap-663-a1.vortex` remain.
Compact comparison, integration and failure evidence replays independently of
those retired payloads. No storage threshold changes or payload backups occur.

The [structural-provider cleanup](../benchmarks/evidence/native-structural-provider-2026-10-21/cleanup.json)
retires 39,022 generated file/symlink entries after complete portable replay
and 21 corruption checks. Completed public/format outputs and logs, private
reference/install environments, staged package copies and the superseded
comparison runtime are removed with exact identities and closed-handle checks.
The latest accepted CLI, native library and installed-tested wheel remain,
along with the original Parquet and current Vortex input. The compact packet
retains complete results and failures; no payload backup or limit change occurs.

The [October 24 provider-refresh cleanup](../benchmarks/evidence/native-vortex-provider-refresh-2026-10-24/cleanup.json)
retires 49,949 generated regular files and six environment symlinks after
clean-checkout replay and exact identity/closed-handle checks. It removes
completed test outputs, superseded runtimes and wheel, package environments,
duplicate packet staging and the temporary verification clone. The original
Parquet, accepted `fresh-input-r2/vortex/hits.vortex`, current provider runtime
and installed-tested wheel remain. All reports and 262 unarchived helper,
fixture and diagnostic files are preserved; compact results retain failed
and interrupted observations. No payload backup or threshold change occurs.

The [October 25 warm-source retirement](../benchmarks/evidence/native-warm-source-reuse-2026-10-25/cleanup.json)
removes 7,692 generated regular files and three environment links after complete
portable and clean-checkout replay. The rejected runtime/wheel, completed
package/environment and temporary verification clone are retired; 564 raw log
copies are preserved byte-for-byte in the verified compact packet. Current
inputs and the accepted provider runtime/wheel remain. Other compiler outputs
remain for the next active naming build; the three exact rejected release
binaries are removed. No payload backup or storage-ceiling change occurs.

The [October 25 ingest-ownership cleanup](../benchmarks/evidence/native-ingest-ownership-2026-10-25/cleanup.json)
retires 7,702 generated regular files and three environment links after portable
and clean-checkout replay. Superseded tool/runtime/wheel copies, completed
package environments, the byte-identical previous Vortex output and verification
checkout are removed. All 497 retired raw log copies remain byte-for-byte in
the compact packet. The original Parquet, current candidate-produced Vortex,
final combined CLI/library and installed-tested wheel remain. The existing
Cargo cache supports the next active implementation; no rollback payload backup
or storage-ceiling change occurs.

The [October 26 source-demand cleanup](../benchmarks/evidence/native-source-demand-2026-10-26/cleanup.json)
retires 37,250 generated regular files and 12 environment links after portable
and clean-source replay. Completed test/install environments, duplicate source
and input copies, logs and the superseded candidate runtime are removed with
exact identities and closed-handle checks. Current source inputs, the final
source-demand CLI/library and installed-tested wheel remain; external baseline
artifacts, user files and the Cargo cache are untouched. The compact packet
retains complete results and failures, and all 873 recorded observations replay
unchanged after cleanup. No payload backup or storage-ceiling change occurs.

The [October 27 Parquet completion cleanup](../benchmarks/evidence/native-parquet-demand-completion-2026-10-27/cleanup.json)
retires 14,199 generated regular files and six environment links after complete
portable verification and eight corruption controls. Temporary package/install
and build-tool directories, copied verification sources, archived raw logs and
the superseded source-demand runtime/wheel are removed with repeated identity
and closed-handle checks. The latest tested runtime and wheel, five current
Parquet inputs, independent references and compact result/failure evidence
remain. Shared compiler outputs and unrelated files are untouched. No payload
backup or storage-ceiling change occurs, and complete replay passes after
retirement.

## Scope

The [October 28 shared-key cleanup](../benchmarks/evidence/native-shared-key-lookup-2026-10-28/cleanup.json)
retires 23,751 completed regular files and six environment links after native,
installed-package, 23-control and isolated source-only acceptance. Repeated
identities and closed-handle checks cover package/build environments, temporary
verifier sources, staged packets, archived outputs and the superseded
rich-retention runtime/wheel. The current tested key-lookup runtime and wheel,
five original Parquet inputs, 347 independent references and compact complete
results/failures remain. All 507 retired process identifiers and 24 historical
helper/control source versions are preserved. Full replay passes after retirement;
the Cargo cache and unrelated files are untouched, with no payload backup or
storage-limit change.

The [October 27 analytical aggregate cleanup](../benchmarks/evidence/native-analytical-aggregates-2026-10-27/cleanup.json)
retires 14,011 completed generated regular files and six environment links after
native/package acceptance and portable verification. Exact identities and
closed-handle checks cover temporary package/build/install environments, copied
verifier sources, archived benchmark logs/source snapshots and superseded
runtimes/wheel. Current inputs, the latest installed-tested runtime and wheel,
and compact complete/failure evidence remain. Shared compiler outputs and
unrelated user files are untouched; no backup payload or storage-limit change
is introduced. Complete replay passes after retirement.

Bulk generated files must remain outside cloud-synced folders. A checkout in
Documents does not make its ignored build files exempt from iCloud syncing.
This is an operational safety rule, not a query performance optimization.

On this Mac, the local-only paths configured on 2026-09-05 are:

- Cargo outputs: `/Users/dylan/.cache/shardloom/cargo-target`.
- ClickBench workspace: `/Users/dylan/LocalData/shardloom/clickbench-100m-uat`.
- Resident official source: the workspace's `sources/hits.parquet`.

The existing Cargo output directory and resident source were moved, not copied.
The source retained its 14,779,976,446-byte length and allocated block count.
The historical Desktop evidence and cloud-only Vortex artifact were not moved,
downloaded, deleted, or represented as newly validated benchmark evidence.
Relocation does not itself reduce local disk use or repair an existing iCloud queue.

The source checkout remains at its current configured project path. Its local
`.cargo/config.toml` sets `build.target-dir`; `.git/info/exclude` keeps that
machine-specific setting out of commits. Explicit Cargo environment or command-line
overrides take precedence, so agents must verify the resolved output path.
The [September 26 cleanup](local-artifact-cleanup-2026-09-26.md) records retirement
of obsolete local test/UAT artifacts. Local incremental compilation is disabled
after its debug/test cache accumulated 146 GiB. Preserve frozen comparison
binaries and receipts; retire redundant bulk outputs only after identity checks.
The [October 10 cleanup](local-artifact-cleanup-2026-10-10.md) archives 40,365
completed input-growth payloads with complete member verification before removing
their originals, recovering 1.29 GB of accounted space. Source fixtures, summaries,
failed/incomplete evidence and current build outputs remain retained.
The [October 11 cleanup](local-artifact-cleanup-2026-10-11.md) retires 8,404
rebuildable debug compiler intermediates and preserves every other debug file,
including test executables and dynamic libraries, in a fully verified archive
before removing originals. Observed free space increases by 33.60 GB. The release
cache, frozen comparisons, datasets, target-root evidence and failure history
remain retained; storage ceilings are unchanged. Subsequent local debug validation
omits debug symbols to reduce regenerated cache size.
The subsequent [urgent cleanup](local-artifact-cleanup-2026-10-11.md#urgent-cleanup-and-current-retention)
supersedes those local retention statements: the obsolete Desktop workspace,
regenerated debug cache, release compiler intermediates, debug-cache backup,
old wheel environments, redundant package downloads and three superseded large
Vortex files were deleted. The original Parquet, October 11 Vortex output,
current frozen runtime and latest installed wheel remain available. Validation
receipts and historical results remain; retired physical payloads require
regeneration. Observed free space increases from 0.12 GB to 89.32 GB.

For subsequent work, retain only current inputs, the latest accepted output,
artifacts needed by an active comparison, and compact source/result evidence.
Remove completed temporary package environments, duplicate downloads and
rebuildable compiler outputs when they are no longer needed. Do not archive an
entire compiler cache merely to preserve old test executables. Keep failed-run
diagnostics and reproduction inputs without retaining every generated payload.
Recheck free space before and after large jobs and preserve the existing guards.

The [October 18 cleanup](local-artifact-cleanup-2026-10-18.md) retires the completed
debug cache and, after fresh ingestion and complete acceptance, the superseded
`fresh-text-aggregation-588-a1.vortex`. The original Parquet, current frozen runtime
and latest accepted `fresh-roadmap-663-a1.vortex` remain. Compact source/result/failure
evidence and exact retirement receipts are preserved; storage limits are unchanged.
This October 18 inventory supersedes earlier payload-residency statements below;
their source identities and measurements remain historical evidence.

After the [October 17 text aggregation acceptance](../benchmarks/native-text-aggregation-2026-10-17.md),
the superseded `fresh-native-review-579-a1.vortex` payload is retired after full
hash, stable-generation, residency and closed-handle checks. The newly accepted
`fresh-text-aggregation-588-a1.vortex` was byte-identical and was retained until
its October 18 retirement in favor of `fresh-roadmap-663-a1.vortex`.
The 15,713,610,513-byte duplicate retirement preserves source data, original
comparison records, failed observations and every storage limit. Both complete
identities and the cleanup receipt are retained in the batch evidence packet.

After the [October 15 fresh timing UAT](../benchmarks/native-query-timing-uat-2026-10-15.md),
the [scoped cleanup](local-artifact-cleanup-2026-10-15.md) retires two superseded
Vortex outputs, two identical release-archive copies and six completed temporary
wheel package/environment pairs. Free space increases by 38.18 GB to 59.31 GB.
The resident Parquet, current `timed-native-461-a1.vortex`, active comparison input
`persisted-native-343-a1.vortex`, all six wheels, compact evidence, current frozen
executables and compiler caches remain. Historical paths in old receipts keep
their original identities; retired payloads require regeneration or the retained
identical copy. This supersedes older local-retention statements for those exact
paths without rewriting their measurements or changing storage ceilings.

After persisted-output acceptance, the
[follow-up retirement](local-artifact-cleanup-2026-10-11.md#persisted-output-acceptance-cleanup)
removes the superseded 15.7-GB timestamp output and retains the newly accepted
`persisted-native-343-a1.vortex` with the original Parquet. The completed debug
cache, two identical executable copies and 21 duplicate historical fixture
copies are also retired. Every unique fixture and all compact result/failure
evidence remain. Observed free space after the output retirement is 79.94 GB.
Do not move cloud-only bulk artifacts out of a synced folder as a cleanup step:
the move can request hydration. Use explicit, scoped deletion only when authorized
and the artifact has been established as disposable.

An earlier same-day cleanup compacted 558 historical and 516 newly accepted
query-log members with complete hash verification, preserving their summaries
and failure history. It retired the exact superseded October 5 generated Vortex
payload after checking its identity and regeneration evidence; the October 9
replacement, resident Parquet and new October 11 UAT output were retained then.
The [cleanup record](local-artifact-cleanup-2026-10-11.md#cleanup-before-the-requested-fresh-full-uat)
distinguishes the different artifact hashes and unchanged storage ceilings.
On September 27, 5,822 completed historical per-call log files were compacted
losslessly into three `completed-call-logs-20260927.tar.xz` archives beside their
original summaries. Per-member size/SHA-256 manifests preserve old path lookup;
every archived byte was verified before removing its original. This recovered
about 22 MiB of allocated log space without discarding failed-run evidence or
raising the 256 MiB guard. The C2.b benchmark packet retains the compaction receipt.
The C5 compiler packet additionally retains the verified compaction of 760 old
held-out call logs into `completed-call-logs-c5-20260927.tar.xz`, recovering about
3 MiB of allocated space with the original summary and per-member manifest kept.
Before the October 2 aggregate acceptance, 3,096 completed per-call files from
six successful Full43 cohorts were compacted into
`completed-call-logs-aggregate-20261002.tar.xz` archives beside their unchanged
summaries. Each archive has a per-member size/SHA-256 manifest; every archived
byte and original file identity was verified before removing its original.
This recovered 10,620,928 allocated bytes. Failed-run logs remain intact and the
storage ceilings were unchanged. The aggregate acceptance packet records the
six archive identities, source summaries and full compaction receipt.
The repaired public aggregate rerun subsequently reached its unchanged 192-MiB
log ceiling after 342 passing checks. The earlier complete and interrupted public
cohorts' 3,811 completed call artifacts were archived losslessly, with per-member
hashes and original identities verified before removal. Their summaries and the
storage-error log remain unchanged. This recovered 198,561,792 allocated bytes
before a fresh complete run; the
[aggregate acceptance packet](../benchmarks/evidence/native-aggregate-ordering-2026-10-02.json.xz)
retains both archive manifests and the failed observation.
The subsequent unary-composition public matrix reached the same 192-MiB ceiling
after 846 passing checks, following an earlier fixture-correction run. Its 2,756
completed envelope files were archived losslessly with per-member hashes and
unchanged summaries, recovering 199,368,704 allocated bytes. Both failed
observations remain recorded. The public relational runner now accepts
`--compress-logs`: each new envelope is stored as gzip, read back byte-for-byte,
and recorded with its original and compressed hashes. This keeps the expanded
matrix inside the unchanged storage guards.
Before the nested-composition paired Full43 run, twelve completed profiling
sample logs from three successful historical targeted cohorts were compacted into
`completed-profile-samples-nested-20261003.tar.xz` archives beside their unchanged
summaries. Original file identities, per-member hashes and every archived byte
were checked before removal; archive manifests preserve the original paths.
This recovered 5,074,944 accounted log bytes. Failed and incomplete cohorts,
including the interrupted September 30 paired run, remain unchanged. The nested
acceptance evidence retains the compaction receipt; storage ceilings were unchanged.
Before the October 3 typed-key paired acceptance, 1,032 completed per-call files
from two successful historical Full43 cohorts were compacted into
`completed-call-logs-typed-keys-20261003.tar.xz` archives. Original file identities,
per-member sizes/hashes and archived bytes were verified before removal; summaries
remain unchanged. This recovered 3,264,512 accounted log bytes. Failed/incomplete
cohorts and all storage ceilings remain unchanged. The
[typed-key acceptance packet](../benchmarks/evidence/native-typed-keys-2026-10-03.json.xz)
retains the receipt and verifies every archived member.
Before the October 3 typed-expression review refresh, preflight stopped before
queries because accumulated logs exceeded the 252-MiB admission threshold that
reserves space below the unchanged 256-MiB ceiling. Twelve closed log files from
three successful September 27 count-selection screens were archived losslessly
beside their unchanged completion receipts. File identities, no-open-handle
checks and every archived byte were verified before removing redundant originals.
This recovered 5,943,296 accounted log bytes. Failed/incomplete runs and storage
ceilings remain unchanged; the
[review packet](../benchmarks/evidence/native-typed-expressions-review-2026-10-03.json.xz)
retains the failed preflight, compaction receipt and per-member manifests.
The typed-unary expansion subsequently reaches the unchanged 192-MiB public log
ceiling after 7,048 passing checks, despite gzip compression. The public runner
now accepts `--archive-logs` with `--compress-logs`: it batches up to 128 closed
gzip envelopes into an xz-compressed tar archive, verifies original identities,
every compressed byte, member hashes and a sidecar manifest, then removes only
the redundant originals. Existing archives are never replaced and readback
failures preserve originals. The accepted 9,300-check cohort retains 20,025
envelopes in 157 verified archives under the same ceiling. Both interrupted
public attempts remain intact, including the later resource-evidence reader
correction; all three executable artifacts and frozen oracles are identical
across attempts. The [typed-unary packet](../benchmarks/evidence/native-typed-unary-2026-10-03.json.xz)
reopens every archived envelope and preserves the complete failure history.
The October 4 nested-key public expansion reaches the same 192-MiB ceiling
after 13,717 passing checks and 8,628,943 complete row comparisons. Wrapping
already-compressed gzip members in xz saves too little space at this scale.
The archive helper now checks each closed gzip file's identity and stored hash,
decompresses it, and verifies the original JSON size/hash before archiving raw
JSON members together. It reads every member back before removing temporary
gzip files. Manifests explicitly identify `raw_json` members and retain both
the original JSON hash and the source gzip size/hash; archived JSON bytes are
identical to the original reports, while the gzip wrapper is no longer retained.
Original failed-run archives and summaries remain unchanged. A guarded check of
384 copied reports from three original batches reduces archive bytes from
2,265,960 to 203,656 and verifies every original JSON byte. Corrupt input, source
mutation and failed archive/manifest readback preserve all source files. These
are evidence-storage measurements, not query-performance results; storage
ceilings remain unchanged.
On October 9, the input-growth preflight reached the unchanged 252-MiB query-log
admission threshold before executing queries. Two completed historical Full43
cohorts were compacted losslessly into `completed-call-logs-input-growth-20261009.tar.xz`
archives beside their summaries. Original size/hash manifests and exact JSON
and companion-byte checks were retained before redundant files were removed,
recovering 4,546,560 accounted log bytes. Final acceptance independently reopens
all 1,032 archive members. Failed and incomplete observations and all storage
ceilings remain unchanged. The [input growth report](../benchmarks/native-input-growth-2026-10-09.md)
links the compaction receipt and the corrected candidate's fresh passing Full43.
For complete development-folder isolation, relocate the checkout itself to an
unsynced directory in a separate, coordinated project-path migration.

Before the September 27 plain-Vortex format comparison, the superseded
18,591,586,804-byte `performance-pr-ingest-4f2c7b970078-r1.vortex` payload was
retired. Its complete hash matched the retained September 26 evidence, whose
full-value comparison proves all 99,997,497 rows and 112 columns equal to the
current 15,682,956,116-byte artifact. Both hashes and file generations were
rechecked before removal. The original Parquet, current optimized artifact and
protected older `perf-current-c71a558e.vortex` reference remain intact. Replaying
the superseded physical layout now requires regeneration.
Completed Full43 per-call logs were also archived losslessly with per-member
hashes, while summaries and failed-run evidence were preserved. Exact removals,
archive identities and before/after space are recorded in
`/Users/dylan/LocalData/shardloom/release-0.3.1-20260927/format-storage-cleanup.json`.
This supersedes the September 26 note that the 18.59 GB comparison payload remains
locally retained; it does not alter the historical timing or correctness evidence.

Before the fresh October 5 Full43 run, four completed historical Full43 cohorts
were compacted into verified archives of original JSON and companion files.
Existing archive manifests, summary bytes, original file identities and every
member's size/hash were checked before redundant containers or loose files were
removed. The original JSON bytes remain recoverable; superseded gzip wrappers
do not. This recovered 7,122,944 accounted log bytes without changing the
256-MiB ceiling, retaining all failed/incomplete observations. The
[fresh release UAT packet](../benchmarks/release-candidate-fresh-uat-2026-10-05.md)
includes the receipt, prior manifests and new archive identities. The resident
Parquet and new 15,713,610,545-byte Vortex output remain retained.

Before scalar-subquery Full43 acceptance, preflight stopped before queries at the
unchanged 252-MiB log-admission threshold. The latest completed release cohort's
516 per-call artifacts were compacted into a verified archive beside its
unchanged summary. Original identities, no-open-handle checks and every member's
size/hash were verified before removing redundant originals. This recovered
3,362,816 accounted log bytes without raising the 256-MiB ceiling. The
[scalar-subquery acceptance packet](../benchmarks/native-scalar-subqueries-full43-2026-10-05.md)
retains the failed preflight, compaction manifest and all 516 reopened members'
proofs. The continuation reused passed focused checks only after verifying their
receipts, immutable executable and all 901 frozen source hashes. Failed and
incomplete observations remain intact.

Before nested-pivot Full43 acceptance, the same log admission guard stopped
before queries. The completed scalar-subquery cohort's 516 per-call artifacts
were compacted into a verified raw-JSON/companion archive beside its unchanged
summary, recovering 3,362,816 accounted bytes. Original identities, closed-handle
checks and every member's size/hash were verified before redundant originals
were removed. Failed/incomplete evidence and storage ceilings remain unchanged.
The [nested-pivot acceptance packet](../benchmarks/native-nested-pivot-state-full43-2026-10-06.md)
retains the failed preflight, continuation proof and compaction manifest;
finalization reopened all 516 members. The continuation reused semantic and
golden checks only after verifying the executable and all 910 frozen source assets.

Before the October 7 Zstd workspace Full43 acceptance, the unchanged 252-MiB
log-admission threshold again stopped preflight before queries. Two completed
historical Full43 cohorts' 1,032 per-call files were compacted into
`completed-call-logs-codec-workspaces-20261007.tar.xz` archives beside their
unchanged summaries. Original identities, closed-handle checks and every member's
bytes/hash were verified before redundant originals were removed. This recovered
4,517,888 accounted log bytes. Failed/incomplete evidence, resident inputs and
storage ceilings remain unchanged. The
[codec acceptance packet](../benchmarks/native-codec-workspaces-2026-10-07.md)
retains the refusal, compaction manifests and independent reopening of all
1,032 members before accepting the fresh Full43 continuation.

Before native builder Full43 acceptance, the same 252-MiB admission threshold
stopped preflight before queries. Three completed historical cohorts were
repacked from gzip-wrapper archives into verified archives of original JSON and
companion bytes. Original manifests and summaries remain intact; all 1,548
members were reopened and hash-checked before acceptance. This recovered
3,452,928 accounted bytes. The first two-cohort attempt still failed storage
admission, and both that failure and the final successful continuation are
preserved in the [builder packet](../benchmarks/native-builder-resources-2026-10-07.md).
Only redundant completed containers were removed. Failed/incomplete evidence,
resident inputs and storage ceilings remain unchanged.

Before the October 8 stateful aggregation/ordering Full43 acceptance, the
unchanged 252-MiB log-admission threshold stopped preflight before queries.
The completed October 7 FSST cohort's 516 original JSON/companion call files
were compacted into `completed-call-logs-stateful-20261008.tar.xz` beside its
unchanged summary. Original identities, closed handles and every archived byte
were verified before redundant originals were removed, recovering 3,362,816
accounted log bytes. Failed/incomplete evidence and all ceilings remain unchanged.
The [stateful acceptance packet](../benchmarks/native-stateful-aggregation-ordering-2026-10-08.md)
retains the refusal and compaction manifest and independently reopens all 516
members before accepting the fresh Full43 run.

Before the October 8 native join Full43 acceptance, the unchanged 252-MiB
log-admission threshold again stopped preflight before queries. The completed
stateful aggregation/ordering cohort's 516 original JSON/companion files were
compacted into `completed-call-logs-join-20261008.tar.xz` beside its unchanged
summary. Original identities, closed handles and every member's bytes were
verified before redundant originals were removed, recovering 3,362,816 accounted
log bytes. Failed/incomplete evidence and all ceilings remain unchanged. The
[join acceptance packet](../benchmarks/native-join-pressure-2026-10-08.md)
retains the refusal and compaction manifest and independently reopens all 516
members before accepting the fresh Full43 run.

Before the October 8 native window Full43 acceptance, the same 252-MiB
log-admission threshold stopped preflight before queries. The completed join
cohort's 516 original JSON/companion files were compacted into
`completed-call-logs-window-20261008.tar.xz` beside its unchanged summary.
Original identities, closed handles and every archived byte were verified
before redundant originals were removed, recovering 3,362,816 accounted log
bytes. Failed/incomplete evidence and all ceilings remain unchanged. The
[window acceptance packet](../benchmarks/native-window-pressure-2026-10-08.md)
retains the refusal and manifest and independently reopens all 516 members
before accepting the fresh Full43 run.

The October 9 pivot acceptance retained two further storage events. A pressure
preflight stopped under the free-space guard; its partial results receive no
acceptance credit. Guarded cleanup removed only the unused Cargo debug cache
and 384 generated Vortex/reopen payloads from three completed historical
pipeline cohorts, preserving their commands, identities, summaries, failure
history, regeneration fixtures and frozen executables. Free-space observations
increased by approximately 68.5 GB across those removals. A fresh pressure
preflight then passed all 3,626 checks on unchanged sources and expectations.

Full43 admission separately stopped before any query at the unchanged 252-MiB
log threshold. Two complete historical cohorts were compacted with identity,
closed-handle and complete member verification, recovering 4,575,232 accounted
bytes. The accepted pivot packet independently reopens all 1,032 original
JSON/companion members. Its [report](../benchmarks/native-pivot-pressure-2026-10-08.md)
retains both refusals and successful continuations. Failed/incomplete evidence,
resident inputs, unrelated files and storage ceilings remain unchanged.

Before the fresh October 9 pre-bump UAT, the unchanged 252-MiB log threshold
stopped admission before ingest, target creation or query work. The completed
pivot Full43 cohort's 516 original JSON/companion members were compacted into
`completed-call-logs-release-050-prebump-20261009.tar.xz`. Source generations,
closed handles and every member were verified before redundant files were
removed, recovering 3,362,816 accounted bytes. The original summary and failed
admission remain intact. The complete second attempt freshly ingested all rows
and passed all query/workflow comparisons. The
[pre-bump packet](../benchmarks/release-candidate-fresh-uat-2026-10-09.md)
retains and independently reopens the archive; no limit was raised and no
incomplete runs were combined.

## Ingest Guard

`scripts/run_clickbench_ingest_uat.sh` defaults to the local-only workspace and
resolves the compiled binary through Cargo metadata unless `--binary` is supplied.
`--uat-root` also relocates the default source and target.

Before creating logs or replacing an artifact, the runner:

1. Rejects macOS Desktop, Documents, Mobile Documents, CloudStorage, and CloudDocs
   paths, including existing symlink aliases. This conservatively rejects Desktop
   and Documents even when their current sync setting is unknown or disabled.
2. Requires output and logs to stay inside the declared UAT workspace.
3. Checks existing workspace bytes plus a candidate reservation against 100 GiB.
4. Reserves the configured maximum artifact size, interpreted conservatively as
   GiB for admission, plus 12 GiB of free disk headroom.
5. Rejects more than 1 GiB of accumulated logs or 256 MiB in the active run's
   log directory. Retained history counts toward the first budget, including
   archived and failed-run evidence; it does not consume the active-run budget.
6. Takes an exclusive workspace lock before running the writer. A stale lock after
   a crash requires inspection; it is not deleted automatically.

The checks run again at each progress sample and after child completion.
Budget failure stops the native CLI and produces a nonzero result; a child that
ignores normal termination is forcibly stopped after a short grace period.
Interrupting the harness also stops the native process group and releases the
owned lock. A small supervisor records process duration independently of the
watchdog polling interval; `native_process_seconds` is the comparison clock,
while `elapsed_seconds` includes polling and harness overhead.
`native_peak_rss_bytes`, when present, is the OS-reported child-process high-water
mark, with macOS byte and Linux KiB units normalized to bytes. It is measured by
the single-child supervisor after exit, not estimated from progress samples and
not an enforced process-memory limit. Earlier records without this measurement
must not be treated as having zero peak memory.
Source residency checks still happen before removal of an existing artifact.
`--replace-existing` removes only the exact requested target. It does not delete
backups, source files, numbered copies, or unknown staging files. Existing staging
files remain visible to artifact/workspace accounting and require explicit review.

The limits can be set explicitly with `--min-free-gib`,
`--max-workspace-gib`, `--max-log-mib` (retained history plus active logs), and
`--max-run-log-mib` (active directory only), or the corresponding
`SHARDLOOM_CLICKBENCH_UAT_*` environment variables. The existing artifact limit
and runtime limit also remain in effect. The workspace count uses the larger of
logical and allocated file bytes, counts hardlinks once, and does not follow
arbitrary directory symlinks.

The October 13 policy adjustment separates retained evidence from excessive
output in one run. The shared defaults are 1,024 MiB retained and 256 MiB active,
with finite nonnegative overrides; an explicit flag takes precedence over its
environment variable. Single and paired ClickBench query runners and the local
read-only proof runner use the same policy. Query compression and paired summary
writes reserve their temporary bytes against both log budgets as well as disk
and workspace space. The query watchdog's separate 8-MiB per-command output
ceiling, free-disk headroom, workspace limits, child cleanup and exclusive lock
remain in force. Storage snapshots record both limits and counters. These are
sampled watchdog limits, so a writer can overshoot before it is stopped.
The final summary or receipt is checked before success is reported; exceeding
either log budget at that last write fails the run and retains its evidence.

The earlier dated reports retain the ceilings used for those observations.
Runners with their own experiment-specific 192/250/256-MiB budgets keep those
explicit limits; this policy does not reinterpret old manifests or increase
their budgets. Future ClickBench drivers should use the shared argument helper
and pass both budgets instead of duplicating the old 252-MiB admission constant.
No automatic deletion accompanies the larger retained-history allowance.

These are sampled watchdog ceilings, not an APFS quota or an allocator guarantee.
Writes can overshoot between samples. They do not impose a limit on iCloud,
other applications, other workspaces, or commands that bypass the runner.
No automated deletion of old runs, cloud files, or user data is performed.

## Current runtime observation procedure

Routine Full43 observations follow the October 21
[isolated procedure](../skills/benchmarking.md#isolated-full43-procedure):
one execution per query, with unrelated task work idle. Keep the standard
runner's active-output storage checks and the lightweight workload guard.
Run full input hashes and build/evidence directory inventories before or after
query execution; do not layer recurring whole-cache scans over the query
watchdog. Existing limits, cleanup and complete-result checks remain in force.

Routine timing communication follows the standing
[engine-time reporting decision](../skills/benchmarking.md#timing-communication-decision):
report engine time only, with individual query engine times when requested. Other clocks and
resource observations remain in diagnostic evidence and are discussed only when explicitly
requested, except for failures or disturbances that invalidate the result. Preparation and
post-run verification must be described as their actual stages, separate from query execution.
The historical observations below retain their original clock definitions.

The [October 14 fresh observation](../benchmarks/native-fresh-unpaced-uat-2026-10-14.md)
records the current decoder/context correction: 54.603468 seconds native ingest,
194.125941 seconds summed over the predeclared 129 query calls, and 331.624211
seconds supervised ingest-plus-query work. All 18 format/workflow cohorts pass.
It has no artificial pacing, battery/thermal admission or automatic retry.
Future full-query UAT defaults to one call per query (43 calls).

The historical September 30 current-main observation is recorded in
[the September 30 report](../benchmarks/current-runtime-e2e-2026-09-30.md):
65.806017 seconds native ingest, 55.251837 seconds for one Full43 pass,
121.057854 seconds combined native work, and 136.696318 seconds actual
supervised workflow wall time. Keep this single observation separate from the
earlier paired cohorts. It measures merged main after the 0.3.3 release, not
the published package binaries.

For a future authorized observation:

1. Freeze the exact revision, build receipt/features, executable SHA-256 and
   source-file hashes. Resolve build outputs with Cargo metadata if rebuilding.
   Freeze the complete query file, resident input identity/hash and all 43
   reference results. Record hardware, physical RAM and policy settings separately.
2. Give the run a unique manifest, log and target name in local-only storage.
   Admit it through the existing storage limits. Following the October 28
   maintainer update, run the sequential Full43 directly against resident original
   Parquet through the shared native source-demand path, with no ingest or
   preparation command. Use `--input-format parquet` and retain the exclusive UAT
   lock for the query pass. Reject overlapping native builds/tests/queries
   with the workload guard before and during execution. Keep process-group
   deadlines and cleanup evidence. Do not raise storage ceilings for a rerun.
3. Capture host load and process CPU/RSS observations before and during the
   run without recording unrelated command arguments. Keep ordinary host
   activity visible in diagnostic evidence and do not describe sampled process CPU as exclusive
   attribution. Record profiler/build overlap or other disturbances explicitly.
   Host metadata is observation context. Do not gate these runners on battery
   charge, power source or thermal state. Those readings do not establish the
   cause of a performance difference. The October 14 experimental host gate was
   removed following maintainer direction; its interrupted measurements remain
   in the [investigation record](../benchmarks/native-performance-gap-2026-10-14.md).
   Do not insert artificial delays or cooling pauses between queries, repeated
   trials or UAT stages. Run the next operation after required validation and
   cleanup finish. Watchdog polling while a child is running is not a pacing
   policy and must not delay an already completed child deliberately.
4. Record engine-owned prepared execution and result serialization separately
   for every query. Keep complete public process wall time, remaining process
   work, supervision, CPU, peak RSS and validation alongside those clocks in
   diagnostic evidence. Routine user-facing timing summaries report engine time only.
   Engine elapsed includes query I/O and source validation; CPU is a separate,
   overlapping measure. Remaining process work includes startup, preparation,
   report rendering and exit; it does not isolate those individual components.
   Both Full43 runners require engine clocks by default and write the complete
   `query-timings.csv`. Explicit historical comparisons may allow missing clocks,
   which must remain unavailable. Version 2 summaries use engine time for primary
   totals and paired comparisons, with `secondary_process_wall` retaining the
   complete process comparison. Result completeness and clock availability are
   separate fields; no partial clock cohort is ranked. Also time the actual workflow. Declare the exact treatment of
   input hashing, validation, archiving and final output hashing. Use one call
   per query by default. Both query runners accept `--repeats N` for explicitly
   requested repeats and record that count; a single pass reports hot-run
   metrics as unavailable. Preserve the declared symmetric scoring rule for
   comparative cohorts and never rewrite historical three-run observations.
5. Validate every complete result and record no-fallback certificates. Preserve
   raw output, timing, PID, result/reference hashes and every sample. Record
   source prehashing and newly written-input cache effects; do not infer a
   cold-cache run. Compare the complete generated Vortex hash before retiring
   an identical duplicate and verify all owned processes/locks are gone.
6. Add a new immutable report and portable evidence packet, then update the
   phase/profile pointers. Keep historical samples intact. Retire only exact
   superseded artifacts with saved identity/hash, open-handle checks and an
   explicit retained replacement or regeneration record.

The portable packet linked from the report includes the executed driver and
guard helpers. Its local paths use placeholders; any replay must bind them to
resident local inputs and a new run identity. Do not run the historical driver
against its already completed paths. The
[cleanup record](local-artifact-cleanup-2026-09-30.md#current-runtime-follow-up)
identifies the current retained reference and superseded-file disposition.

## Storage guard verification

The [October 7 input-completion acceptance](../benchmarks/native-input-completion-2026-10-07.md)
retains a later storage-admission refusal before any Full43 query ran. Two
completed historical Full43 cohorts were compacted only after every original
member was copied, hashed and reopened in its archive and open handles were
checked. This recovered 6,725,632 accounted log bytes; summaries and all 1,032
members remain represented in the immutable packet. The existing ceiling was
unchanged, and the fresh 129-query observation then passed admission. Failures,
resident source data and unrelated files were preserved. This is a recorded
completed-run compaction, not permission for blanket cleanup or guard bypass.

The [FSST integration correction](../benchmarks/native-fsst-admission-2026-10-07.md)
preserves two further preflight refusals under the same 252-MiB threshold.
Two verified compactions of three completed historical cohorts recover
5,582,848 accounted bytes and retain all 1,548 original JSON/companion members
with unchanged summaries. The intervening incomplete 91-call cohort is preserved
in full after its outer supervisor disappeared; its orphaned runner and children
were stopped and its lock removed. It receives no acceptance credit. A fresh
fully supervised pass validates all 129 results. No failed/incomplete evidence,
resident inputs or unrelated files are deleted, and no limit is raised.

```sh
python3 -B -m unittest discover -s scripts -p test_local_uat_storage.py -v
bash -n scripts/run_clickbench_ingest_uat.sh
```

Tests cover safe destinations, macOS synced paths, symlink/case bypasses, sparse
files, hardlinks, free space, candidate reservations, workspace and log budgets,
root override behavior, preservation on failed preflight, runaway log output,
exact-target replacement, backup/source preservation, and concurrent-run exclusion.
Fixtures are small and require no engine build.

Every full-size run must remain blocked until its storage admission passes.
Do not infer that the existing CloudDocs backlog is disposable from these checks.

## Provider References

- [Cargo build-cache location configuration](https://doc.rust-lang.org/stable/cargo/reference/build-cache.html).
- [Apple iCloud Drive file management](https://support.apple.com/en-ie/guide/mac-help/-mchl1a02d711/mac).
