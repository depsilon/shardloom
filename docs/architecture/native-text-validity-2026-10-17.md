<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native text validation handoff

Status: fully implemented and merged in PR #1567 at `5bcee25a`, with passed matched
performance, local and hosted acceptance. The [measurement record](../benchmarks/native-text-reconstruction-2026-10-17.md)
retains both the unsuccessful getter-only comparison and the combined result. This continues the shared
engine investigation under PERF-01/04/05/10 after the five-mechanism text batch.
All broader PERF obligations and CG-1 through CG-23 remain open.

The expert comparator is an encoded-columnar maintainer reviewing immutable
buffer lifetimes, UTF-8 validity, hostile reconstruction and complete query cost.
The matched comparison of the previous batch found only a 0.46% reduction in the
sum of per-query means across 43 queries, with two calls per query per build.
It does not establish a material complete-workload improvement. The present
change targets repeated validation across provider/consumer boundaries.

## Provider decision and invariant

Vortex-first classification: `use_vortex_native_provider`. The pinned Apache-2.0 Vortex
0.85 `VarBinViewArray`, immutable buffer handles and `BufferString` already own
the values. Extend that provider's safe borrowed/retained text access instead of
introducing a ShardLoom trust wrapper or workspace unsafe code. Existing native
feature gates, provider identity and execution/Native I/O certificates apply.
`fallback_attempted=false` and `external_engine_invoked=false` remain required.

A logical UTF-8 dtype is necessary but insufficient for the new fast access.
Some generic upstream builders accept raw bytes and invoke constructors whose
validation runs only in debug builds. The existing construction-type marker
therefore cannot serve as a release-build validation receipt.

Add a private provider receipt for immutable values actually checked by a
reviewed constructor. Checked host construction and deserialization establish
it after validating every physical view, including null slots. The existing
length-to-view builder may establish it after its whole-heap UTF-8 and character
boundary checks. Arbitrary public unchecked construction does not establish this
receipt. Generic builder output retains checked text access.

Cloning, safe selection and byte-identical replacement may carry the receipt
only with the same value bytes and dtype. Changed/newly host-readable content
passes the existing checked constructor. Binary-to-UTF-8 array-parts rebuilding
does not acquire the receipt just by changing a type label. Device handles stay
opaque: accessing device views or a selected device payload fails explicitly.

Safe text access checks dtype, selected index, host residency and byte range.
With the receipt, it may reuse the established UTF-8 invariant inside the
excluded provider. Without it, the selected value passes ordinary validation.
A borrowed string lives no longer than the array borrow. A retained
`BufferString` keeps the same payload owner as `bytes_at`; it does not claim to
retain unrelated buffer-table or outer-array credit. Nullness remains a caller
responsibility, as for the existing physical byte getter.

## Consumer scope

Use the native accessors in shared chunk dictionaries, string and integer/string
partial aggregation, dictionary DISTINCT, direct text materialization and native
sort/key preparation. Preserve complete byte equality,
hash collision handling, row/dictionary order, integer widths, counts, floating
point evaluation, cancellation and resource admission. Escaping global keys
still acquire independent storage. Raw untrusted bytes still require validation.
Admission depends on types and operator contracts, never query IDs, column names
or benchmark constants. There is no persisted format, compression policy, SQL
or capability change.

## Acceptance

First test checked and generic construction, inline/outlined Unicode, empty and
null slots, binary dtype rejection, malformed raw builder output in release
mode, bounds and device rejection, hostile array-parts reconstruction, slicing,
selection, buffer replacement and retained payload lifetime. Exercise both
receipt and ordinary-validation routes. Retain existing collision, overflow,
nullable, dictionary and independently owned aggregate fixtures.

Run focused adapter/provider checks and strict provider lint, then freeze a
complete-query comparison with the current accepted binary, matching source/input
identities and resource policy. Complete required workspace gates before
retention. The five priority
families remain anchors; all 43 queries are regression controls. Use existing
runners, exact full-result checks and all samples. Include any construction cost
in ordinary query measurements. Retention requires a reproducible useful gain;
the receipt itself is not performance evidence.

## Complete text reconstruction and validation

The first implemented handoff passed all 172 complete results and schemas in
the frozen whole-workload comparison. The sum of per-query means decreased
from 65.5297315465 to 64.0689343415 engine seconds (2.23%), below its predefined
5% standalone retention gate. It is not accepted as a standalone optimization.
Fresh diagnostic samples from that exact binary still attribute substantial
text work to Zstd view reconstruction and the provider's per-value validation.
Sampled timings receive no benchmark credit.

Extend the same provider/consumer work unit with a safe native builder for
length-prefixed text records. This is `use_vortex_native_provider`: the pinned
Vortex Zstd decoder, writable host allocation, binary views, validity masks and
allocator-owned tables remain the implementation boundaries. No codec format,
runtime route, dependency or compressed source artifact changes.

For canonical UTF-8 materialization that fits one native view buffer, retain
exclusive access to the already admitted decompression allocation until views
are built. Walk every stored record with checked lengths and exact count
validation, constructing only views within the requested contiguous value
range. Null rows receive empty views. After consuming a record's length,
replace its four length bytes with zeros in this new allocation. Values and
their order remain byte-identical. The compressed input is never modified.

Validate only the selected immutable byte span in one SIMD UTF-8 pass. The
zeroed record headers prevent an invalid partial character at one value's end
from borrowing continuation bytes from the next value. Outlined strings no
longer need independent per-value UTF-8 scans. Invalid bytes
outside the requested values remain unobserved, while malformed record extents
or counts anywhere in the decompressed frames still fail. Every output view is
constructed from checked bytes. After both payload and views are frozen, the
checked native constructor checks every view's buffer index, bounds, prefix and
character boundaries. Inline values receive independent UTF-8 checks. A custom
allocator's freeze method is not itself a content-validity proof. Only these
checks may establish the existing private validation receipt.

The immutable output retains the original allocator owner and full allocation
charge even when its visible buffer is sliced. Output views and buffer tables
use the same allocator; no unaccounted lengths vector or second payload copy is
introduced. Admission failures release all owners. Primitive, binary,
noncontiguous sparse selection and raw builder-append paths keep their existing
prefix representation. Oversized text heaps use the existing checked native
reconstruction path. This changes the mechanism of the previously rejected
count-only reconstruction trial by fusing value reconstruction and validation.

Before measurement, test mixed inline/outlined Unicode, zero and boundary
lengths, empty/all-null/mixed-null arrays, slices and framing boundaries,
invalid selected versus unused text, invalid sequences split across records,
truncated/overrunning prefixes, incorrect counts and ranges, allocation refusal,
payload ownership and release. Retention still requires at least 5% lower
whole-workload engine time, improvement in both paired totals, no predefined
material per-query regression, complete results/schemas and broad validation.

The combined implementation passes the same frozen comparison rule against
the accepted runtime: the sum of per-query two-sample means decreases from
67.4536999765 to 58.1389694790 engine seconds, a 13.81% reduction. Both paired
totals improve by more than nine seconds. The five priority families improve
16.81% and the other 38 queries improve 12.14%; no query crosses the predefined
material-regression rule. All 172 complete results and schemas match, including
the 1,524 output rows and every archived timing companion. This is a bounded
matched observation, with all slower samples retained, not a single-pass Full43
result or an external-engine superiority claim. Separate local acceptance passes
4,374 workspace/reduced-writer test executions, 4,025 native test executions
(27 unchanged manual exclusions), 100 direct binding checks, 4,827 public cases
and 18 format/workflow cohorts containing 704 native and 704 independent oracle
records. All 2,411 selected source files match the measured executable. The
retained packet includes the initial modular fixture refusal caused by an
omitted existing dependency path; that attempt ran zero workload records.
