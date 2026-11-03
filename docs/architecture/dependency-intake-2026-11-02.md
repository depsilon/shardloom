<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compatible dependency batch

Status: accepted and merged in PR #1639 as `529b1980` after all 58 hosted checks
pass on the complete dependency graph at `66153267`.

The November 2 maintainer request is to repair failed open PR checks and merge
compatible work. This batch preserves the original heads of five dependency PRs
and combines their changes with current main. The paired ADBC update avoids two
incompatible versions of the same public types. Future ADBC updates share one
Dependabot group.

| Proposal | Previous version | Combined version |
| --- | --- | --- |
| [#1635](https://github.com/depsilon/shardloom/pull/1635) | jiff and jiff-static 0.2.35 | 0.2.38 |
| [#1636](https://github.com/depsilon/shardloom/pull/1636) | rusqlite 0.40.2 / libsqlite3-sys 0.38.2 | 0.41.1 / 0.39.1 |
| [#1637](https://github.com/depsilon/shardloom/pull/1637), [#1638](https://github.com/depsilon/shardloom/pull/1638) | adbc_core and adbc_ffi 0.24.0 | 0.25.0 together |
| [#1639](https://github.com/depsilon/shardloom/pull/1639) | tonic 0.14.6 | 0.14.8 |
| Final audit correction | async-executor 1.14.0, withdrawn upstream | 1.14.1 |

The lockfile replaces exactly eight registry package versions and adds no
package names. Arrow/Parquet remain on 59.3.1, Vortex on 0.90.0 and tonic-prost
on 0.14.6. The published ADBC 0.25.0 manifests admit Arrow versions `>=58, <61`;
adbc_ffi requires adbc_core 0.25.0. The combined lockfile contains one ADBC core
version. Tonic adds an edge to the already locked tokio-util package.

These remain existing transport, timezone, compatibility-boundary and diagnostic
dependencies. This change adds no input format, execution provider, external
engine fallback, side effect or publication authorization.

## Failed checks and corrections

The Linux package jobs on #1635, #1636 and #1639 completed compilation but
rejected stale dependency notices because the bundled inventory no longer
matched Cargo.lock. The regenerated bundle covers all 434 locked registry
packages and 13 vendored components, with 826 file records and 249 unique texts.
Unchanged components retain their exact records and text bytes.

The #1637 build reports distinct `adbc_core::error::Error` identities and an
unsatisfied `FFIDriver` bound. Updating core and FFI together removes the split
dependency graph; combined compilation and runtime checks now pass. The #1638
failure log was unavailable, so its separate failure is not
assigned a more specific cause from its manifest alone.

The combined installed-wheel check exposed a further ADBC 0.25 compatibility
change: the exporter stores separate `CancelHandle` objects when connections
and statements are created. Its cancellation callbacks no longer invoke the
old mutable `cancel` implementations. The first installed check retained two
errors from the default unsupported handle. ShardLoom now supplies the new
handles: a connection uses its existing bridge, and a statement shares a slot
that follows the current stream across execution, reset and close. The slot
lock is released before entering Python. Independent-manager controls cover
statement cancellation from another thread, reuse, connection scope, peer
isolation and retained Arrow batch ownership.

The added error controls also reproduced a native cleanup crash after an
unhandled, correctly rejected overlapping operation. A Python exception's
traceback could retain an Arrow reader and be released later by PyO3's deferred
reference pool. Consuming that exception inside its attached conversion scope
removes this deferred ownership. A subprocess regression preserves the expected
busy-session diagnostic and verifies that cleanup completes without crashing;
the before-fix crash and after-fix result remain in the evidence. This correction
adds no unsafe code or new execution concurrency.

The [#1636 CodeQL Rust run](https://github.com/depsilon/shardloom/actions/runs/41251717934/job/124686452892)
failed during analysis after its disk cache exhausted the runner filesystem.
The saved log reports `No space left on device` and 577.3 MB remaining. The
scan was incomplete and provides no alert-clearance conclusion. The combined
head must pass CodeQL; the earlier failure is neither ignored nor accepted.

The first final local release audit passed cargo-audit and the Python runtime
dependency check, but cargo-deny rejected `async-executor 1.14.0` as withdrawn.
Upstream published [1.14.1](https://github.com/smol-rs/async-executor/releases/tag/v1.14.1)
on November 2 with a fix for a waker retaining invalid executor state during
shutdown. This transitive Vortex/Smol dependency now uses that patch. Its
dependency declarations and features are unchanged, its declared minimum Rust
version is 1.63, and both license texts match 1.14.0 byte-for-byte. The lockfile
update changes only this version and checksum beyond the original seven
replacements. The failed audit and earlier passing runtime tests remain
historical evidence; final acceptance requires the updated graph. The upstream
executor issue is separate from the reproduced ADBC exception-ownership crash.

## Source and notice evidence

All eight downloaded registry archives match their lockfile SHA-256 checksums.
Archive manifests, packaged VCS identities and embedded license/copyright files
were read without running package code. The initial filename inventory omitted
Jiff's two `UNLICENSE` members; the final bundle includes both exact archived
texts, and the initial and corrected verification records remain preserved.

Jiff permits MIT or Unlicense use. Rusqlite, libsqlite3-sys and tonic declare MIT;
async-executor permits Apache-2.0 or MIT, and ADBC declares Apache-2.0. Both ADBC
archives omit their root license and notice
files, so their package VCS revision supplies the exact
[LICENSE.txt](https://github.com/apache/arrow-adbc/blob/1f9f5540dbcb695c540dfcdafeea421c7d94adb1/LICENSE.txt)
and [NOTICE.txt](https://github.com/apache/arrow-adbc/blob/1f9f5540dbcb695c540dfcdafeea421c7d94adb1/NOTICE.txt).
Their bytes match the GitHub blob identities and are bundled in full, including
the upstream third-party notices. The dependency licenses satisfy the existing
Apache-2.0-compatible intake policy. No upstream implementation code is copied
into ShardLoom.

## Deferred Arrow major upgrade

[#1642](https://github.com/depsilon/shardloom/pull/1642) changes selected Arrow
crates to 60.1.0 while other consumers retain 59.3.1. Its MSRV build fails on
Arrow type/trait identity mismatches. That partial graph is not part of this
batch. A future major upgrade needs a compatible Vortex/Arrow/Parquet/Python
provider review and consumer validation; conversion bridges are not introduced
solely to accept a bot update. The earlier
[coordinated-intake requirement](dependency-intake-2026-10-25.md) remains in force.

## Acceptance

- [x] Review the exact version delta, published archives, manifests and notices.
- [x] Preserve one ADBC core version and update the complete notice bundle.
- [x] Validate the combined default/native workspace and affected public bindings.
- [x] Verify an installed native package and applicable local release contracts.
- [x] Pass hosted checks, inspect review state on the final combined head, then merge.

The [hosted acceptance record](../benchmarks/evidence/dependency-intake-2026-11-02/hosted-acceptance.json)
preserves all 58 passing checks, the absence of submitted review findings at
merge, and ancestry of the original heads of #1635–#1639. All five PRs are now
closed and merged; the consolidated normal merge retains their commits. The
partial Arrow-major proposal #1642 remains deferred under the contract above.

The [local acceptance packet](../benchmarks/evidence/dependency-intake-2026-11-02/acceptance.json)
binds the final source snapshot, CLI/library identities and installed wheel.
Strict default and enabled workspace Clippy, formatting, 3,311 default Rust tests
and 6,857 enabled Rust tests pass. The enabled suite retains 36 existing opt-in
ignores. All 303 installed-wheel integration tests pass without errors, failures
or skips, including the new cancellation and subprocess cleanup regressions;
binary identity, loading and uninstall checks pass. The release dependency audit
passes cargo-deny, cargo-audit, Python dependency auditing and the no-fallback
dependency check. Notice validation and its seven regression tests pass.

The generated API reference is refreshed for the changed Cargo source identities.
The packet preserves every failed attempt, including the original ADBC failures,
the withdrawn executor audit result and storage-guard interruptions. The failed
cleanup regression and its passing counterpart use identical Python test source;
their native source differs only in the exception-conversion ownership fix.
Final installed-package coverage includes the subsequent executor patch.

Completed test executables, obsolete generated libraries, temporary package
environments and incomplete runtime copies were retired after repeated identity
and closed-handle checks. Current tested runtimes/wheels, all inputs and all
verification records remain. No payload backup or storage-limit increase was
used. No benchmark was requested or run for this maintenance batch.
