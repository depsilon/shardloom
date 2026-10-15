<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native compression resource acceptance

The existing Vortex Zstd compressor and default dictionary trainer now receive
admission before allocating their covered storage. Local acceptance passes the
six broad gates, 72 vendored-provider tests, 80 direct binding tests, 757 public
cases and one complete 43-query pass. All 18 compression lifecycle cells stay
within their frozen cost limits, with identical serialized bytes between the
old and corrected implementations. [PR #1555](https://github.com/depsilon/shardloom/pull/1555)
is merged after all 44 checks pass. The
[hosted packet](evidence/native-compression-hosted-2026-10-15.json) preserves
the complete native and installed-wheel logs, source equality, resolved review
finding, and exact-merge production documentation checks in 37 verified members.

The [portable packet](evidence/native-compression-resources-2026-10-15.json)
contains 3,068 verified members. This is the finite
[compression contract](../architecture/native-compression-resources-2026-10-15.md)
under PERF-03/06. It follows the immutable 0.6.0 packages and makes no new
package-release, general allocation, process-RSS or speedup claim.

The later [fresh timing UAT](native-query-timing-uat-2026-10-15.md) verifies the
same compressor with shared engine/result clocks, fresh ingestion, all 43 queries
once and all 18 format/workflow cohorts. It supplies the new per-query baseline
and exact additional source hashes; this report's original process-only samples
remain unchanged. The [cleanup record](../architecture/local-artifact-cleanup-2026-10-15.md)
documents retirement of superseded generated files after that complete run.
The [current-source acceptance supplement](evidence/native-query-timing-acceptance-2026-10-15.json)
also verifies the timing-enabled binding, all 757 public workflows and the
expanded 4,222-test native suite. Its original failures and environment
corrections remain recorded; the compressor's cost evidence is unchanged.

## Corrected behavior and ownership

Reuse Vortex 0.85.0 `ZstdData`, the caller's `ExecutionCtx` and its existing
`HostAllocator`. Length-prefixed variable-width input, offsets, sample lengths,
compression scratch, retained frames and dictionary output use actual admitted
buffers. Compression and training reserve their audited foreign-heap bounds
through that same allocator before constructing C state. The compressor frees
its C storage before releasing the reservation; the synchronous trainer frees
its temporary storage before returning. Retained slices keep their payload
credits after the producer context closes.

The pinned graph remains Zstandard 1.5.7 through `zstd` 0.13.3, `zstd-safe`
7.2.4 and `zstd-sys` 2.0.16. Linked version and default-level checks protect
the source-audited bounds. Default codec parameters, one-shot safe owners,
by-copy dictionary loading and sequential FASTCOVER training remain unchanged.
Unknown custom allocators refuse foreign reservations; the explicit upstream
default allocator retains its unbounded policy. ShardLoom's allocator and credit
wrapper retain their typed denial and ownership behavior.

Five original regressions fail on the preceding implementation: missing
compressor admission, empty primitive and text frame defaults, oversized frame
targets and over-aligned primitive frame counts. All pass after correction.
Additional tests cover every named allocation-denial stage, exact grants,
competing owners, malformed buffer lengths/alignment, cleanup/retry, every
primitive type's exact bits, nullable Unicode/binary, slices and serialization.
The trainer rejects a too-short training partition before entering C, while
preserving valid small samples and native training errors. It does not retry
failed dictionary training with a different encoding.

Structural frame/protobuf vectors, infallible metadata clones, allocator
bookkeeping, C stacks, imported-array ownership and other codecs retain separate
owners. The inactive experimental `ZstdBuffers` encoding is outside this unit.
Conservative foreign bounds may refuse a high compression level even when a
particular invocation would allocate less. These limits are explicit in the
contract; the change does not turn a query grant into a process-memory limit.

## Validation and complete workflows

| Gate | Result |
| --- | --- |
| Workspace formatting | Passed |
| Workspace Clippy, warnings denied | Passed |
| Workspace tests | 3,219 passed; none ignored |
| Native read-feature Clippy | Passed |
| Release-user-surfaces Clippy | Passed |
| Native CLI/Vortex tests | 4,219 passed; 29 named manual/platform exclusions |
| Vendored Vortex owner/memory/Zstd tests | 10 + 7 + 55 passed |
| Direct source-package/native-library tests | 80 passed; no failures, errors or skips |
| Public workflow matrix | 757 passed across 19 families |
| Full43, one execution per query | 43 passed; all 381 returned rows checked |

The native suite executes all 19 added ownership/value regressions. Its 29
exclusions retain their exact source declarations; the new opt-in compression
cost test runs separately in the frozen release comparison below. The packet
retains exact commands and logs, including `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --all-targets -- --test-threads=2`.
Native CLI/Vortex testing uses
`vortex-local-primitives,vortex-write,universal-format-io`; the frozen CLI,
binding and lifecycle executables use `release-user-surfaces`.

Direct tests use Python 3.12.14 and PyArrow 25.0.1 with the source package and
an explicitly frozen native library. They are not an installed-wheel test.
Public evidence reopening checks 724 expected-value files containing 22,002
rows, 1,062 envelopes, 20 pressure files containing 677,251 rows and all 136
referenced artifact identities. The runner separately reopens the actual
format outputs. These overlapping counts are not additive. Streaming,
late failure, cancellation, quota, cleanup and native/no-fallback certificates
retain the existing complete-case inventory.

## Frozen compression lifecycle comparison

Compare base `e8611a3c00af2a7b3f3f742c8b7128c3a258fbc7` with the corrected
source, using the same optimized harness. Its 18 cells cross nullable Int64,
UTF-8 and binary; 4,096 and 65,536 rows; and canonical, Zstd without a dictionary
and Zstd with a dictionary. Each cell records one warmup and three samples of
25 complete operations: 1,368 operations per executable. Frame targets are
128 and 1,024 valid values, level zero, with a separate 64-MiB producer and
consumer grant.

Fixtures and reference values are prepared outside the operation clock. The
clock includes session creation, compression/training, native message
serialization and hashing, producer closure, fresh-session reopen/decode,
every-value/null verification and cleanup. Native process clocks additionally
include startup, output and exit. This is in-memory serialization; the public
matrix supplies separate file-workflow evidence.

The cell gate is an increase exceeding **both 10% and 100 microseconds per
operation** in the median of the three sample totals. Process CPU exceeding
both 10% and one second, or peak RSS exceeding both 25% and 64 MiB, also triggers
a full reverse-order comparison. A repeated breach blocks retention. No gate
is crossed, so the frozen protocol requires no reverse-order run. All warmups
and individual operations remain in the packet. No pacing, cache flush,
battery/thermal admission or automatic retry is used.

Times below are milliseconds per complete operation, derived from each cell's
median sample total. Dictionary means the default trainer is included in every
operation. Serialized bytes are identical in both roles, including their hashes.

| Nullable input | Rows | Encoding | Base ms | Corrected ms | Change | Serialized bytes |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Int64 | 4,096 | Canonical | 0.2273 | 0.2274 | +0.02% | 33,452 |
| Int64 | 4,096 | Zstd, no dictionary | 0.4500 | 0.4505 | +0.11% | 29,333 |
| Int64 | 4,096 | Zstd, dictionary | 0.7229 | 0.7631 | +5.55% | 28,291 |
| Int64 | 65,536 | Canonical | 0.4261 | 0.4261 | +0.01% | 532,652 |
| Int64 | 65,536 | Zstd, no dictionary | 2.2445 | 2.2305 | −0.62% | 301,251 |
| Int64 | 65,536 | Zstd, dictionary | 20.7962 | 20.6605 | −0.65% | 277,933 |
| UTF-8 | 4,096 | Canonical | 0.3930 | 0.3937 | +0.17% | 343,650 |
| UTF-8 | 4,096 | Zstd, no dictionary | 0.7019 | 0.7012 | −0.10% | 26,027 |
| UTF-8 | 4,096 | Zstd, dictionary | 3.3208 | 3.3387 | +0.54% | 42,163 |
| UTF-8 | 65,536 | Canonical | 3.2146 | 3.2023 | −0.38% | 5,496,094 |
| UTF-8 | 65,536 | Zstd, no dictionary | 5.8676 | 6.0460 | +3.04% | 386,805 |
| UTF-8 | 65,536 | Zstd, dictionary | 34.1208 | 35.9421 | +5.34% | 422,130 |
| Binary | 4,096 | Canonical | 0.3733 | 0.3746 | +0.35% | 358,246 |
| Binary | 4,096 | Zstd, no dictionary | 0.7739 | 0.7816 | +0.99% | 37,109 |
| Binary | 4,096 | Zstd, dictionary | 3.3707 | 3.3858 | +0.45% | 56,615 |
| Binary | 65,536 | Canonical | 2.8861 | 2.8637 | −0.78% | 5,729,726 |
| Binary | 65,536 | Zstd, no dictionary | 6.6547 | 6.8887 | +3.52% | 565,114 |
| Binary | 65,536 | Zstd, dictionary | 39.2558 | 41.4003 | +5.46% | 597,133 |

The base and corrected native processes take 9.636306 and 9.945134 seconds,
consume 9.592681 and 9.897544 CPU seconds, and peak at 66,748,416 and
68,714,496 RSS bytes. Maximum corrected producer admission is 18,501,710
bytes; maximum retained payload is 603,241 bytes; maximum consumer admission
is 6,931,902 bytes. The old producer's zero reported credits mean that those
allocations were unaccounted, not absent. Admission and RSS are different
measurements. This screen supports retention of a resource correction, not a
speedup or broad engine comparison.

## Single-pass Full43 observation

All queries use the retained 99,997,497-row, 15,713,610,513-byte native file,
SHA-256 `004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`.
Input generation and hashes are checked before and after the run. All 43
queries execute once in fresh native processes against the retained native
regression reference, with exact values, schemas and resource/no-fallback
envelopes independently reopened. No new ingest is part of this observation.

Summed native process time is **155.890987 seconds**; supervised suite time is
**203.923640 seconds**. Every per-query elapsed/CPU/RSS sample is retained.
There is no repeated-run median or hot-run score. The earlier fresh-ingest UAT
used a different native-file generation and three executions per query; these
single-pass timings are not a controlled comparison with that cohort and do
not establish a new cause for historical timing differences.

The host is macOS 27 arm64, ten logical CPUs and 16 GiB physical RAM, with
Rust 1.99.0/LLVM 23.1.1. Full43 declares 24 GiB and maximum parallelism 12;
these are policy limits, not physical-memory measurements, a process-RSS
ceiling or an observed worker count. The resident input is prehashed and OS
caches are uncontrolled. Exclusive-workload, process cleanup, 120-second
query deadlines, 12-GiB disk headroom, 100-GiB workspace and 1,024/256-MiB
retained/active query-log limits remain enabled.

## Provenance, failed attempts and reproduction

The accepted source map contains 2,255 selected files and a complete 15-file
changed-source overlay. Base commit alone does not identify the candidate.
The packet binds the exact source bytes, frozen premeasurement contract,
commands, helpers, compiler outputs and executable identities:

| Executable | SHA-256 |
| --- | --- |
| Base lifecycle harness | `b832682297895daef661ceab21d6a2fe1a05222119a71be7f245f820c4343404` |
| Corrected lifecycle harness | `614bb756ffd54d0246a5ba08c391880b8f066bc43ab03f76e78057d3e6eb0369` |
| Public/Full43 CLI | `14df2773f9660c7a94b6ffc0250a85014671792a7823944ac698ac86bd197136` |
| Direct Python native library | `d681c19fd4b56525c8ed2880d38b6aa73a241fc42caa765091b93cd541f94c43` |

An initial candidate build incorrectly reused the base executable from the
shared Cargo target. Byte identity and the missing 19 candidate tests exposed
that setup error **before any timing**. The corrected setup invalidates the
affected packages, records their actual fresh compilation, freezes distinct
executables and verifies the complete test-inventory difference before running.
The rejected setup is retained without measurements. The benchmarking skill
now records this shared-target hazard.

The first acceptance coordinator also expected 17 owner tests where the exact
vendor target contains 10. Its tests had all passed; a source-checked correction
reuses the 10 owner, seven memory and 55 Zstd results without rerunning them.
The original coordinator failure remains preserved. Original red tests and
their exact source, intermediate compilation/lint/fixture failures and all
corrected receipts remain in the archive.

The [archive](evidence/native-compression-resources-2026-10-15.tar.xz) retains
162,049,407 original bytes in 3,068 members. Every member was reopened and
checked against the [manifest](evidence/native-compression-resources-2026-10-15.manifest.json.xz)
before the repository copy was verified unchanged. The
[verification receipt](evidence/native-compression-resources-2026-10-15.verification.json)
binds both outputs. Bulk executable and dataset payloads are excluded with
their identities; logs, values, original failures and changed sources remain.
Reproduction uses the recorded base plus source overlay, the identical lifecycle
harness, resident input/reference identities and guarded commands. Rebind
recorded local paths to unsynced storage and choose new output names; completed
evidence files are immutable. No native work was repeated merely to copy proof.

This closes local compression acceptance. Hosted checks, installed-wheel
observations and deployed guide verification remain required for integration.
The [six areas and eight investigations](../architecture/native-local-completion-scope-2026-10-07.md)
remain open under their existing owners.
