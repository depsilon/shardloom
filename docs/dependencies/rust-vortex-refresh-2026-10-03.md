# Rust and Vortex release review — October 3, 2026

Rust stable is now **1.99.0** on the development Mac. Vortex **0.87.0** is the
latest released, non-yanked provider, but ShardLoom retains **0.85.0** because
the newer allocation boundary cannot preserve its recoverable budget-denial
contract. This review admits no new runtime feature or performance claim.

## Version and toolchain evidence

The [official stable manifest](https://static.rust-lang.org/dist/channel-rust-stable.toml)
dated October 1 and the [Rust release announcement](https://blog.rust-lang.org/2026/10/01/Rust-1.99.0/)
identify Rust 1.99.0. Updating the existing global `stable` toolchain advanced
it from 1.98.0 to 1.99.0. Bare `rustc` and `cargo` in the user's source checkout
resolve through rustup to that toolchain; no directory override is installed.
The active worktree has no pinned older toolchain file. A fresh login-shell
check found macOS `path_helper` putting Homebrew Rust 1.98.1 ahead of rustup
after `.zshenv` ran. The new local `.zprofile` restores rustup's PATH precedence;
fresh login and non-login shells in both checkouts now resolve Rust 1.99.0.
The existing Homebrew installation is left intact. Normal CI jobs already select
`stable`; the separate MSRV job derives its version from the workspace.

The workspace's `rust-version = "1.96"` is the supported compiler floor, not
the compiler used for normal development. All member crates inherit it. Keep
that compatibility floor until an adopted API requires a deliberate increase.
The explicit 1.99.0 toolchain used by the current runtime acceptance and the
updated global stable toolchain report the same compiler commit
`b940084d7eb6a299eb4bfeb8e34901bc051e7ac4` and LLVM 23.1.1.

The explicit 1.99.0 installation lacked `llvm-tools`; installing that component
restores the bundled `rust-objcopy` executable that had failed to load its LLVM
library during earlier release builds. A separate compile/strip/run smoke test
checks the repaired tool installation. It does not reclassify an earlier build
warning as a successful invocation or change any frozen benchmark executable.

Rust 1.99's raw-pointer layout and C-variadic APIs do not fit this workspace's
safe-Rust contract. The inspected queues, rolling windows and ordered readers
do not need reverse-order retention. Lossy UTF-8 conversion would change the
exact data contract. No use of the discouraged leak/reclaim pattern was found
in the Rust crate sources. This review therefore keeps the existing algorithms
and adopts the current compiler/tooling without raising MSRV for an unused API.
Any compiler performance gain still requires a controlled benchmark.

## Released Vortex metadata

The live [GitHub release](https://github.com/vortex-data/vortex/releases/tag/0.87.0)
was published October 2 at 20:51:54 UTC, with `draft=false` and
`prerelease=false`. The live crates.io records for
[vortex](https://crates.io/crates/vortex/0.87.0) and
[vortex-zstd](https://crates.io/crates/vortex-zstd/0.87.0) agree on 0.87.0 as the
latest stable version and report Apache-2.0, Rust 1.95 and non-yanked status.
Cargo metadata inspection confirms those declarations. Older indexed pages
that still describe 0.87 as a draft do not override these release records.

All 34 Vortex packages in the current lockfile remain on 0.85.0; the two direct
workspace requirements and inherited adapter dependencies agree. The existing
provider-version derivation remains the authority for executable certificates.
Vortex 0.87 requests Arrow/Parquet 59.2, while the admitted 0.85 bridge remains
on 58.3.0. A later migration must update that bridge coherently and review
changed file features, compression registration and serialized encoding IDs.
The presence of DataFusion in the upstream workspace manifest is not permission
to add its integration to ShardLoom.

## Allocation admission decision

The published 0.87 `memory.rs`, `builders/mod.rs`, `allocation.rs` and
`buffer_mut.rs` match the exact release-tag source bytes. The resource failure
found in the [0.86 admission review](../architecture/vortex-086-upgrade-admission-2026-09-27.md)
remains:

- [`MemorySession`](https://github.com/vortex-data/vortex/blob/0.87.0/vortex-array/src/memory.rs)
  accepts `BufferAllocatorRef`, whose custom allocator implements the unsafe
  `allocator_api2::Allocator` trait. The old safe `HostAllocator` hook is absent.
- [`Allocation::allocate_impl` and `grow`](https://github.com/vortex-data/vortex/blob/0.87.0/vortex-buffer/src/allocation.rs)
  send `AllocError` to `handle_alloc_error` instead of returning a typed error.
  The public mutable-buffer constructors and growth operations use this path.
- [`try_from_trusted_len_iter` and `try_extend_trusted`](https://github.com/vortex-data/vortex/blob/0.87.0/vortex-buffer/src/buffer_mut.rs)
  return iterator errors; they do not make allocation fallible. `try_into_mut`
  tests whether a buffer has unique ownership. Neither repairs quota refusal.
- The new [`builder_with_capacity_in`](https://github.com/vortex-data/vortex/blob/0.87.0/vortex-array/src/builders/mod.rs)
  does forward its allocator. This improves allocation coverage over 0.85, whose
  corresponding wrapper ignores it, but does not restore recoverable denial.

ShardLoom's existing `ReservedHostAllocator` reserves before allocation,
preserves a distinguishable typed denial and holds credit until the last buffer
view is dropped. The shared pressure transition depends on that distinction.
Wrapping an already allocated byte owner covers external ownership only; it
does not intercept provider allocations. A blanket pre-reservation has no
proven bound for all provider temporaries and growth. Neither is an equivalent
replacement. Catching an unwind does not turn the allocation-error handler
into the required `Result` boundary.

Disposition: `blocked_until_vortex_or_shardloom_evidence` for 0.87; retain the
existing `use_vortex_native_provider` disposition for 0.85. Reopen with an
upstream safe, fallible allocation hook, or a replacement design proving typed
denial, cancellation, allocation/growth accounting and final-owner credit
lifetime. No unsafe exception, allocator bypass or provider fork is introduced.

## Feature adoption and verification

The [dated API map](../architecture/vortex-public-api-inventory.md#october-3-2026-vortex-087-intake)
attaches new features to existing PERF owners and distinguishes direct callers,
ShardLoom implementations and currently absent callers. Repeated probes,
sparse filtering, run-end reduction and compressor statistics are concrete
candidates after allocation admission. Availability alone does not establish
that any current query runs faster. The broader optimization/breadth queue and
CG-1 through CG-23 stay open under their existing evidence requirements.

The [machine-readable review](rust-vortex-refresh-2026-10-03.json) records source
hashes, release metadata, toolchain identity, commands and verification receipts.
Retained-provider memory tests cover typed denial through error wrappers,
slice/clone credit lifetime and release after failed or unfinished allocation.
Docs and version-source checks cover the unchanged central version contract.
No 0.87 compile, feature-matrix run, query benchmark, package publication or
upgrade-compatibility claim is made: admission stopped before a dependency edit.
