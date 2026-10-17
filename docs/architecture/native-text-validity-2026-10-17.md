<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native text validation handoff

Status: prototype, with no retained performance claim. This continues the shared
engine investigation under PERF-01/04/05/10 after the five-mechanism text batch.
All broader PERF obligations and CG-1 through CG-23 remain open.

The expert comparator is an encoded-columnar maintainer reviewing immutable
buffer lifetimes, UTF-8 validity, hostile reconstruction and complete query cost.
The matched comparison of the previous batch found only a 0.46% reduction in the
sum of per-query means across 43 queries, with two calls per query per build.
It does not establish a material complete-workload improvement. The present
candidate targets repeated validation across provider/consumer boundaries.

## Provider decision and invariant

Vortex-first classification: `wrap_vortex_concept`. The pinned Apache-2.0 Vortex
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
or benchmark constants. There is no file-format, codec, SQL or capability change.

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
