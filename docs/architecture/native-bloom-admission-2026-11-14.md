<!-- SPDX-License-Identifier: Apache-2.0 -->

# Pinned Bloom provider: admission declined

Status: **complete finite admission review; declined before performance**.
This resolves the existing Vortex 0.93.0 `vortex.bloom_filter.sbbf` provider
intake in the November 13 metadata sequence. The broader predicate-summary
parent stays open. The subsequent [necessary text-signature experiment](native-text-signatures-2026-11-14.md)
has a complete drop disposition with accepted source restored. The finite
unconditional-callback batching screen is complete and declined before
performance; the existing batch contract remains for FSST. The
[survivor admission report](native-survivor-admission-2026-11-15.md) records
this bounded result. Broader I pruning/read/consumer attribution and narrower
batching remain open. Item-19 spatial
execution, item 20, RFC 0044 and CG-1 through CG-23 retain their obligations.

The [portable evidence](../benchmarks/evidence/native-bloom-admission-2026-11-14/README.md)
preserves 28 fresh native admission checks, four signed-zero counterexamples,
the pinned provider and registration/resource reviews, exact probe/build
identity, and restoration. Its saved-evidence verifier passes 38 corruption
controls and relocated verification with historical sources, runtime access
and network access blocked. This is an admission decision, not a timing drop.
No public query, persisted Bloom file, benchmark or external engine was run.

## Vortex-first decision and boundaries

Classification: `blocked_until_vortex_or_shardloom_evidence` for this current
unversioned persisted provider. In the pinned
[`BloomFilter` source](../../vendor/vortex-layout/src/layouts/zoned/aggregates/bloom_filter/mod.rs),
the provider explicitly forbids persistence under the current identifier because
hashing, salts and block layout can change without a format-version change.
The production registration and default writer do not enable this Bloom path.
Ordinary supported queries retain their current ShardLoom-native execution;
there is no new public unsupported error or registration toggle.

The [merged core implementation](https://github.com/vortex-data/vortex/pull/9398)
does not establish an admitted writer/reader integration. The
[earlier index proposal](https://github.com/vortex-data/vortex/pull/8933) and
[writer-interface proposal](https://github.com/vortex-data/vortex/pull/9413)
were closed unmerged at the recorded observation. Their descriptions are
historical context; the locked source determines actual behavior. In particular,
the historical core PR description is not evidence of Decimal support in this pin.

A new versioned ShardLoom wrapper was considered. It would own a persisted
format, typed hash/equality contract, rewrite registration, writer selection,
resource limits and read-side retention. Renaming the upstream identifier would
not establish those contracts. This intake does not invent a replacement format
or change a dependency to bypass the persistence warning.

## Fresh correctness and resource findings

The temporary feature-gated Rust example uses the actual locked aggregate and
session APIs. The source and executable were frozen before its single execution.
All 28 declared checks pass, including the expected counterexamples:

- F32 and F64 positive and negative zero compare numerically equal. For each
  width and insertion direction, native Bloom accumulation rejects membership
  of the opposite zero representation. A naive equality rewrite can therefore
  create false negatives; the production runtime never enabled that rewrite.
- Integer and UTF-8 accumulation, empty/all-null filters, matching-option union,
  and saturated-filter controls behave as declared. These are small semantic
  controls, not a false-positive-rate measurement.
- Malformed options, zero blocks, unknown hash identifiers, malformed partial
  lengths and incompatible block counts fail. The largest u32 block option is
  accepted without a memory-policy check; its implied 137,438,953,440-byte
  allocation was **not attempted**.
- A null partial represents an empty filter. It cannot stand in for unknown or
  missing statistics when proving that a zone has no possible matches.

The source review follows `BloomPartial::from` through `BufferMut::zeroed` to
the static allocator and allocator-api2 global allocation. This path does not
carry the session allocator. It neither rules out separately held caller credit
nor measures process RSS. The zoned reader loads the entire auxiliary statistics
child once per reader: a per-filter limit would not alone bound retained metadata.
Pinned twox-hash 2.1.2 source matches its locked archive and uses the default secret
and zero seed for the observed one-shot hashing path.

## Evidence, restoration and remaining acceptance

The probe built with the guarded offline, locked release configuration and
`release-user-surfaces`. Build and observation receipts verify unchanged sources
and a drained process group. The exact example and temporary manifest registration
remain in the archive. Both were removed from runtime source afterward; the
restored tree equals `14d79c67b28d265ebab0f0697d8ff221176f9747` at integration
base `494b41cca98349b21dfabd09a129a205c6664ecb`.

Production code and the accepted C runtime remain byte-identical. C's completed
workspace/native/provider/package checks remain the existing runtime evidence;
they were not rerun for this documentation and saved-evidence change. The fresh
checks here are the native admission probe, source/reference checks, saved-evidence
verification, corruption controls, portability and documentation validation.
The portable verifier verifies saved observations and cited source bytes; it
does not independently rerun the provider or reconstruct the complete build.

A later persisted Bloom candidate needs all of the following before it can
reach a separately frozen complete-operation comparison:

1. Stable versioned identity for hash/seed, block layout, dtype and normalization.
2. Necessary-condition proofs for typed equality and IN, including nulls,
   numeric conversions, signed zero, nonfinite policy and extensions.
3. Credited writer construction, complete zone-table storage and reader lifetime,
   with deterministic refusal and cleanup.
4. Source-generation binding, conservative missing/inexact metadata treatment,
   exact residual results and certificates through the actual public path.
5. Complete construction/persistence/read/consumer evidence and the standing
   complete-cohort engine-time gate.

These requirements leave other safe summary designs open. This finite decline
does not authorize a general claim that Bloom filters are unsuitable or slower.
