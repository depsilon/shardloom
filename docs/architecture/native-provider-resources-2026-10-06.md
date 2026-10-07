<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Provider Resource Ownership — October 6, 2026

Status: accepted local source at `ae4e3398e07d58c059780d218c17b7eea7fa31ef`,
with regression acceptance repeated on combined commit
`99e0a4b304c506a2b24f605eb5156859b356486a`. Hosted integration completed in
[PR #1526](https://github.com/depsilon/shardloom/pull/1526) after all 39 checks
passed; published v0.4.0 predates this addition. This closes the finite `NATIVE-PROVIDER-RESOURCES`
implementation unit under PERF-03/06/07/11/12 and CG-3/5/19/20/21, not those
owners' broader obligations.

## Decision and scope

Native FSST and Zstd decoding now admit the reviewed payload allocations through
the query's shared native memory owner. Reservations precede allocation, and
escaping buffers keep their credits through clones and slices until their final
owner is dropped. Source admission, provider selection, execution and delivery
stay inside the existing Vortex-native pipeline. Unsupported allocation paths
fail explicitly; no external query engine supplies residual execution.

The finite inventory includes:

| Allocation or lifetime | Accepted contract |
| --- | --- |
| FSST canonical payload, string views and validity | Checked size evaluation through admitted integer metadata providers, including masked validity and nullability-only casts, before native allocation |
| Concatenated FSST, Zstd and canonical string views | Native concatenation with the same retained allocation credits |
| Zstd payload, view and nullable scatter buffers | Fallible session `HostAllocator` admission, including scalar access and append paths |
| Selected native spill sessions | Clone the provider registry and replace only the memory owner; preserve the selected concrete native providers |
| Retained results and consumers | Buffers, clones and slices retain reservations across session/consumer handoffs; denial and failure release owned allocations |

The shared pool governs these allocations; it does not impose a process RSS
ceiling. Zstd C decoder contexts and dictionary preparation scratch, other
unreviewed codecs/builders, metadata headers and allocator bookkeeping remain
outside this finite accounting scope. Existing aggregate/join/window/pivot
state limits and admitted spill families are unchanged.

## Pinned provider and provenance

The workspace retains Vortex 0.85.0. A local Apache-2.0 patch of its published
`vortex-zstd` crate routes the reviewed decode buffers through the existing
fallible allocator. It preserves concrete encoding, VTable, slicing and file
serialization identity. This avoids replacing the provider with a second
decoder or introducing a query-engine integration.

The authoritative source is the published registry archive, SHA-256
`03cae43b171152fd403b884d5e0b764e860261d38b2fd80dfd4e03678beac837`.
Its upstream VCS metadata reports a dirty tree, so the Git revision alone is not
claimed to reproduce the archive. Original file hashes, license identity and
the patch boundary are retained in
[`upstream-provenance.json`](../../vendor/vortex-zstd/upstream-provenance.json).
The word “Prototype” in that original intake record describes its intake state;
the accepted disposition is recorded here and in the immutable acceptance
receipts. No dependency upgrade or upstream adoption is implied.

## Acceptance and limits

Constrained-grant tests cover deterministic denial before the reviewed
allocations, exact nullable values, retained-credit lifetimes, cancellation,
source replacement and failing consumers/writers. The source and public/native
regression suites, all eight local writers and complete Full43 results pass.
The [acceptance report](../benchmarks/native-engine-acceptance-2026-10-06.md)
links source/binary identities, original failures, complete raw envelopes and
independent archive inspection.

An initial Zstd lifecycle cost screen observed candidate/stock ratios from
0.994526 to 1.041709; no cell crossed its predeclared 10% repeat trigger. That
screen used the initial source snapshot; the vendor bytes remained unchanged
in the corrected accepted snapshot. It is an allocation-safety acceptance
check, not a current whole-engine speed claim.

Remaining work includes complete reader/codec scratch accounting, general
operator spill and recovery, other-platform runtime parity and any stable
production support envelope. See [local resource safety](v1-local-resource-safety.md).
Vortex remains native input/output; compatibility formats remain explicit
translation boundaries, and fallback remains disabled.
