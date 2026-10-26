# October 26 compression dependency correction

## Reason and scope

The dependency gate for [PR #1611](https://github.com/depsilon/shardloom/pull/1611)
rejects the existing `zstd 0.13.3`, `zstd-safe 7.2.4` and
`zstd-sys 2.0.16+zstd.1.5.7` graph. The
[original CI job](https://github.com/depsilon/shardloom/actions/runs/40378486657/job/121768817266)
reports four upstream advisories:

| Advisory | Affected mechanism | Upstream fixed version |
| --- | --- | --- |
| [RUSTSEC-2026-0391](https://github.com/RustSec/advisory-db/blob/main/crates/zstd/RUSTSEC-2026-0391.md) | Abandoned stream state carried into a reused context | zstd 0.14.3 |
| [RUSTSEC-2026-0398](https://github.com/RustSec/advisory-db/blob/main/crates/zstd-safe/RUSTSEC-2026-0398.md) | Seekable decompression output length beyond capacity | zstd-safe 8.2.0 |
| [RUSTSEC-2026-0403](https://github.com/RustSec/advisory-db/blob/main/crates/zstd-safe/RUSTSEC-2026-0403.md) | Legacy streaming failure commits uninitialized output | zstd-safe 8.2.0 |
| [RUSTSEC-2026-0410](https://github.com/RustSec/advisory-db/blob/main/crates/zstd-sys/RUSTSEC-2026-0410.md) | WebAssembly allocation failure handling | zstd-sys 2.1.2 |

Dependency presence is established. A supported ShardLoom exploit path has not
been established: the inspected IPC, Parquet and Vortex consumers use bulk
operations, ORC uses the read decoder, and the product ships native platforms.
This limited static review is not a waiver or runtime security proof. The
correction removes the affected dependency versions and adds no advisory ignore.

## Compatibility decision

Retain the single Arrow/Parquet 59.3.1 graph, ORC 0.9.0, Vortex 0.90.0 and bundled
libzstd 1.5.7. The original Arrow IPC, Parquet and ORC manifests constrain zstd to
0.13; even the published Arrow/Parquet 59.3.2 metadata retains that constraint.
A partial Arrow 60 upgrade would violate the existing shared graph boundary.

Import the exact previously locked Apache-2.0 source archives for Arrow IPC,
Parquet and ORC, with per-member and legal provenance. Change their normalized
manifests to select `zstd =0.14.3` and prohibit publication. IPC and Parquet Rust
sources remain unchanged. The new corruption test exposes two existing ORC
`Decompressor::read` unwraps; the only ORC source changes propagate those codec
errors as `std::io::Error` instead of panicking. Valid decoding is unchanged,
and both the first and later compressed-block boundaries are tested.
The existing Vortex Zstd patch selects the same wrapper and
`zstd-sys =2.1.2`. Root and affected standalone-test locks must resolve the
reviewed versions. The wrapper licenses change to BSD-3-Clause, already allowed
by the repository policy; package notices must preserve the exact license text.

This is a dependency prerequisite for item 20 under the existing release and
security owners. It does not accept the unfinished item 18 ORC implementation,
change SQL or DataFrame semantics, add a source format, close a competitive gate,
or establish a performance improvement. The frozen broad-query observations
remain attributed to their original runtime. No benchmark repeat is required to
restate those historical results.

## Vortex-first provider check

- Subject: the compression library beneath the admitted native Zstd provider.
- Decision: `use_vortex_native_provider`; preserve the existing Vortex 0.90.0
  provider and its ShardLoom allocation, selection and certification boundary.
- Existing consumers: native Vortex frames, Arrow IPC, Parquet and ORC adapters.
  All four converge on one reviewed Zstd dependency chain.
- Residual handling and data representations: unchanged. Compatibility formats
  remain adapters; Vortex remains native input and output.
- Source assumptions: static decoder workspaces require C version 1.5.7, reject
  legacy frames, retain their allocator owners and fail explicitly on errors.
- Technique review: preserve PulseWeave, capillary units, work shaping,
  metadata-first execution and existing timing/evidence tiers. No new execution
  strategy or performance admission is introduced by this dependency update.
- Evidence obligations: exact graph and source provenance, format round trips,
  malformed-input rejection, provider ownership tests and package notices.
- Fallback boundary: `fallback_attempted=false`,
  `external_engine_invoked=false`; no external engine dependency is introduced.

## Acceptance

Local acceptance is recorded below. Hosted CI remains the final merge gate:

1. The root graph contains the fixed wrapper versions and no parallel affected
   Zstd chain; original parent versions and the native C version are retained.
2. Every imported source file and license is verified against its pinned archive
   or recorded upstream revision, with only the declared manifest changes and
   two ORC error-propagation statements.
3. Native codec/resource tests and compressed IPC/Parquet/ORC round trips pass,
   including malformed input and repeated use after failure where applicable.
4. Default workspace formatting, lint and tests, native release lint/tests,
   affected provider configurations and installed-package proof pass.
5. Dependency/advisory, provenance, package notices and hosted CI pass without
   new advisory exceptions or weakened checks.

The maintainer subsequently directed that #1611 be fixed and merged before
continuation. The correction is therefore included in that PR, with merge gated
on successful checks. The original baseline observations remain frozen and
attributed to their measured runtime; this correction does not retime them.

The [review evidence packet](../benchmarks/evidence/native-broad-query-review-2026-10-26/manifest.json)
retains exact commands, source identities, original failures, corrected checks,
dependency/source verification and installed-package results:

- The dependency release gate passes without new exceptions. Local tools are
  `cargo-deny 0.19.8`, `cargo-audit 0.22.1` and `pip-audit 2.10.0`; hosted CI
  independently uses its pinned versions. The three pre-existing advisory
  allowances are unchanged. The root and standalone Zstd-provider locks use
  the corrected chain; historical fixture locks and the unused optional chain
  in the standalone Vortex-file test lock are not claimed to be upgraded.
- Both borrowed-context regressions fail against the original `zstd 0.13.3`
  chain and pass against the corrected provider. All 93 provider tests pass.
  The additional bulk-decoder recovery control passes on both versions.
- Formatting, default workspace lint and 3,261 default workspace tests pass.
  Native release and standalone provider lint pass. The final native Zstd
  selection passes 68 tests with two existing manual ignores; the format-I/O
  selection passes all 67 tests, including four new compression/error cases.
- The initial complete native attempt records 3,300 passes, one ORC panic and
  32 manual ignores. After the two error-propagation corrections, the focused
  compression suite and final format suite pass. This is not represented as
  a new complete local native run; hosted native partitions remain required.
- The corrected release CLI/library passes a fresh isolated wheel installation:
  183 integration tests, zero skips, five-interface query coverage, load and
  uninstall. Notices preserve 827 license-file records across 436 registry and
  11 vendored packages, with 249 unique license/notice texts.
- Temporary package copies, installation/build/audit environments and duplicate
  dependency downloads are retired after identity and closed-handle checks.
  The tested runtime/wheel, original baseline, inputs and compact results remain.

The packet preserves the initial missing-audit-tool attempt, incorrectly scoped
provider-lint environment, native-test panic, replay reproducer and native-only
test naming lint failure. Successful follow-up records identify the corresponding
corrections; none of those attempts is overwritten or credited as passing.

## Replay review correction

PR review found that replay parsed worker output before inspecting its supervisor
receipt. A timeout or crash can leave empty, truncated or invalid output while
the original parent correctly records the failure. A shared receipt-first
classifier now gives live observation and offline replay the same precedence.
Output, error and job hashes still have to match. Failed reference preparation
and interrupted cohorts retain their classifications and available receipts;
they cannot become complete baseline results.

Ten replay tests cover native, independent-reference and Polars failures,
unusable output, changed receipts/hashes and incomplete cohorts. The unchanged
baseline packet still verifies all 2,331 complete results and 2,334 receipts.
Eleven mutations of actual evidence are rejected. Relocated replay succeeds
with original workspace/data access, native/Polars/PyArrow imports and process
or network effects denied. No query is rerun and the original six provenance
warnings remain explicit.
