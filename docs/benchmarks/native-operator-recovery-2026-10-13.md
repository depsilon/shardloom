<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native operator recovery acceptance

Status: finite unit complete in [PR #1549](https://github.com/depsilon/shardloom/pull/1549),
with local checks, all 44 hosted checks and production verification passed.
This unit closes a caller-cancellation gap in relational run cleanup
and adds actual-query interruption acceptance for the older admitted spill
families. The [contract](../architecture/native-operator-recovery-2026-10-13.md)
defines scope under PERF-03/06/12 and CG-5/19/21. Version 0.5.1 is unchanged.

## Behavior and evidence

`VortexRelationalSpillPolicy::cleanup_abandoned_controlled` shares the caller's
existing cancellation token. The original cleanup method remains a compatible
wrapper. Valid requests with explicit cancellation, inherited cancellation or
an expired deadline fail before filesystem access. Across sort, weighted COUNT,
integer DISTINCT and relational run namespaces, cancellation after the first
deletion preserves the marker and any remaining run. One-run cases exercise the
last check before marker deletion; two-run cases exercise interrupted deletion.
Fresh cleanup tokens complete both cases while inherited directory descriptors
remain open. Configuration validation and ownership checks retain their previous
contracts.

Four actual queries run in child processes against parent-owned native sources.
A test-only hook parks each child after two native runs have been written and
validated. The parent checks active-owner refusal and byte preservation, kills
and reaps the child, checks that an unknown file prevents deletion of all entries,
then removes only that control file and performs the matching public cleanup.
The source hash is unchanged. A fresh complete query then checks every returned
value and required ordering against independently computed expected results:

| Dispatch case | Source and requested result | Fresh completion checks |
| --- | --- | --- |
| Numeric sort | 50,000 rows; signed priority, unsigned sequence and UTF-8 destination; descending priority with last-tie order; offset 45,003, limit 7 | All three fields and seven ordered rows; more than eight native runs and at least one merge |
| Single-key worker COUNT | 16,384 rows with 128-byte dictionary strings, unique keys and a late winner; soft state-item budget 1; offset 1, limit 7 | Exact frequencies and key order; worker execution, actual spill rather than fitted resident selection |
| Compound-key COUNT | 65,536 rows; signed cohort and 128-byte dictionary strings; offset 7, limit 7 | Exact frequencies, both keys and global order; actual compound spill |
| Integer COUNT DISTINCT | 131,072 rows; signed 16-bit group and unsigned members above 2^61, repeated across input batches; offset 123, limit 7 | Exact group/count values and order; at least two native runs |

Every fresh case checks written/validated run counts, admitted memory and disk
bounds, owned cleanup, native-I/O certification and absence of Arrow conversion
or fallback. These are scoped Rust public execution tests. They prove owned
cleanup and a fresh restart, not resuming interrupted work. They do not establish
a whole-process RSS ceiling, power-loss durability or adversarial filesystem CAS.

## Validation

The final frozen source contains 2,240 selected files. All six gates pass with
unchanged source hashes and drained child processes:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets -- --test-threads=2
cargo clippy --offline --locked -p shardloom-vortex -p shardloom-cli \
  --all-targets --features release-user-surfaces -- -D warnings
cargo clippy --offline --locked -p shardloom-vortex -p shardloom-cli \
  --all-targets --features vortex-local-primitives -- -D warnings
cargo test --offline --locked -p shardloom-vortex --lib \
  --features vortex-local-primitives,vortex-write,universal-format-io \
  recovery -- --test-threads=2
```

The workspace has 3,219 passing tests across 106 groups, with no failures or
ignored tests. The focused native run has 35 passing tests, no failures or ignores,
and 2,720 filtered tests. The filtered tests were not run by that command.
Child entry tests also appear in the count and return immediately when called
without their parent-owned environment. The four real process-death cases are
executed by the three parent tests. Counts are not additive with earlier suites.

The initial focused run passed 32 tests and failed three at the new test helper's
path comparison: macOS exposes the same directory through `/var` and
`/private/var`. Comparing the canonical workspace corrected the helper; no runtime
ownership check was weakened. The next complete focused run passed. The first
Clippy attempt then rejected the extended cancellation test's 106-line size.
A scoped allowance keeps its cancellation, inherited-lock, marker and retry
assertions together; the complete six-gate rerun passes. Original failures remain
retained. The local linker reports its existing oversized unwind-section warning;
there is no Rust Clippy warning or test failure in the final gates.

Process-death and controlled relational cleanup tests require Unix file identity
and the existing native writer/local-primitives feature gates. No new Windows
recovery support is claimed. No query operator semantics, provider format,
dependency or package version changes. Full43 and the Python streaming matrix
are not rerun for this cleanup API/test-only change; the separately source-bound
[preceding composition acceptance](native-stream-input-composition-2026-10-13.md)
keeps its own complete workload results and timings.

The [portable evidence index](evidence/native-operator-recovery-2026-10-13.json)
binds all 2,240 selected source files, the 11-file source overlay, six final local
gates and original failure history. Every archived member is read back and
verified, as are the copied archive and compressed manifest. User-reference,
public-status, backlink, contribution and whitespace checks pass. This unit
does not change guide source, generated pages or search assets, so the preceding
rendered guide proof remains applicable to those unchanged files.

## Hosted integration

The reviewed head `528aa23ed7f09442b664bc07ca44b5c48a8d5c0f`, hosted synthetic
merge and actual merge `483cbaa982dd300dec45bb50066cdbf452686b36` share Git tree
`346dcea90f924aa1a977c1cc42b66e50f4f2bc69`. All 2,240 accepted source blobs
were independently read and SHA-256 checked. All 44 checks pass, including native
features, both CodeQL lanes and deployment. Automated review completes on that
head with no inline findings or review submissions.

Three installed-wheel jobs cover Ubuntu Python 3.10/3.13 and macOS Python 3.13.
Each reports 80 native tests, zero failures/errors/skips and a verified isolated
load. The packet binds the exact workflow and proof-script source to those logs.
The native method names are not logged individually; wheel artifacts were not
downloaded, and no uninstall proof is claimed.

The [hosted receipt](evidence/native-operator-recovery-hosted-2026-10-13/index.json)
retains all raw check/review, wheel and production records in a 26-member archive.
Every member is reopened and verified by both the packager and primary review.
The exact merged production deployment passes. All three served guide pages
preserve their source with only the recorded Cloudflare insertions, and search
metadata is byte-identical. The website and generator source did not change, so
the preceding desktop/mobile/search proof applies without a duplicate UI run.
The wheel packet's original pending tree-check note is preserved as historical;
the later primary check supplies that proof.

Broader reader/codec allocation, unadmitted state, streaming
destinations/fanout, platform acceptance and conditional investigations retain
their existing owners.
