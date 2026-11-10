<!-- SPDX-License-Identifier: Apache-2.0 -->

# UUID columns through native workflows

Status: native lifecycle implemented and finite runtime verification complete.
This is the standalone UUID obligation in
[item 19](native-rich-data-lifecycle-2026-10-24.md). Source-bound acceptance and
hosted integration have separate evidence gates. Variant field expressions,
useful spatial types and functions, and resource-admitted deep traversal remain
required work. No performance or whole-item completion claim is made.

## Provider and logical contract

Use the existing Vortex 0.93 `vortex.uuid` extension and its registered Arrow
import/export provider. The native storage is a fixed-size list of sixteen
nonnull UInt8 elements, with SQL nullability on the complete value. Arrow uses
canonical `arrow.uuid` over `FixedSizeBinary(16)`. Values are sixteen bytes in
big-endian order. No new UUID storage format or query engine is introduced.

Admit the canonical unrestricted UUID domain: all 128 bits are preserved,
including nil, all-one values, and unrecognized version/variant bits. Vortex's
optional version restriction is rejected at the shared schema boundary,
including empty input, because Arrow's canonical UUID type cannot preserve that
restriction. Arrow extension metadata must be absent or empty. Incorrect
width, nullable byte elements, foreign extension identity and unexpected
metadata fail explicitly. Unannotated binary does not become UUID by inference.

UUID columns remain typed through selection, aliases, duplicate and outer joins,
bounded result delivery, actual native spill, Vortex write/reopen and explicit
Arrow IPC/Parquet export. This includes admitted nested fields, dictionaries,
constant/repeated storage, nonzero slices, empty results and NULL parents.
Current depth and physical-wrapper limits remain unchanged.

## Native operations and consumers

Use the existing native key, selection and aggregate owners for equality,
ordering, joins, grouping, DISTINCT, MIN and MAX. Comparison is lexicographic
over the sixteen big-endian bytes. Hashing uses all sixteen bytes and a distinct
type tag. UUID and binary/text/numeric/temporal domains are incompatible. SQL
NULL follows the established operator policy; NULL is distinct from nil UUID.
Nested keys preserve their complete structure and leaf types.

UUID parsing, generation, arithmetic, implicit casts and explicit conversions
to other scalar domains are outside this unit. Reject unsupported operations
during binding, including empty-input queries. Existing numeric sorting,
statistics and RANGE-window shortcuts retain their numerical contracts.

Native relational execution keeps Vortex arrays and fixed-width key values.
Arrow conversion is a terminal interoperability boundary. Explicit Python row
conversion produces `uuid.UUID`; diagnostic JSON/text uses lowercase canonical
hyphenated UUID strings with a typed schema. Terminal rendering does not change
the native value into text. Exported arrays and children retain their credited
buffers until the final owner releases them.

## Implementation and verification

Provider decision: `use_vortex_native_provider`. Use the existing optional
Vortex dependency and initialize its UUID provider at the shared extension
registration boundary. Preserve the lightweight default dependency closure.
Enable the pinned Parquet canonical-extension mapping only within the existing
format feature. Shared input validation must continue refusing unknown and
unsupported extensions; feature availability does not itself admit a type.

Arrow 59.3.2's C stream schema conversion drops extension metadata when the
declaration belongs to dictionary values. The existing vendored Arrow array
provider now preserves that declaration on the Rust field, recursively through
container fields and dictionary chains. Conflicting index/value declarations
fail explicitly. Original physical types, field flags, payload buffers and
release ownership are retained; native schema admission still decides which
extensions are supported. This is a correction to the existing interoperability
provider, with recorded source provenance and provider regressions.

The native owner checks dtype, row count, byte count and validity before reading
fixed-width keys. Allocation and expanded schema traversal use the existing
operation grant; cancellation and denied growth must release all new owners.
There is no external residual evaluator and `fallback_attempted=false` remains
required in runtime evidence.

Freeze independent complete-byte fixtures before running the candidate. Cover
nil/max UUID, arbitrary version bits, equal prefixes with different suffixes,
NULL, all-null/empty inputs, dictionaries, slices, nested parents, duplicate and
outer joins, group/distinct/order/min/max and actual spilled keys/payloads.
Verify a separate-process typed reopen, ordinary SQL and DataFrame surfaces,
Arrow IPC/Parquet readers and writers, retained final-child ownership and
closed-context/cancellation cleanup. Negative controls cover metadata, widths,
type mismatches, unsupported casts, low grants and altered persisted results.

Acceptance requires focused regressions, complete default/native checks, strict
lint and formatting, feature/MSRV checks, a frozen release runtime, isolated
installed-wheel proof, and portable source-bound evidence preserving original
failures. Hosted review, PR/main checks and verified artifact retirement remain
separate gates. No benchmark or package publication is part of this unit, and
it cannot close item 19, item 20, RFC 0044 or CG-1 through CG-23.

## Verification record

The final implementation passes 3,322 default and 7,052 native Rust tests,
with zero failures and 37 existing native opt-in ignores. This includes all
seven UUID regressions. Formatting and strict default/native lint pass, as do
seven feature configurations and both supported-Rust checks. The standalone
Arrow provider passes 778 tests with one existing ignore and strict lint;
six new provider regressions cover recursive dictionary extension metadata.

A fresh release build passes all 19 UUID public-interface and result-schema
tests. Its isolated installed wheel passes the complete 431-test integration
selection with zero failures, errors or skips, followed by successful uninstall.
The separate Python core run passes: 1,291 declared cases, 868 reported runs and
62 optional/environment skip records. Those skips are not counted as installed
native proof. UUID and schema fixtures are frozen independently of returned
candidate values.

The [portable acceptance packet](../release/evidence/native-uuid-lifecycle-2026-11-10/README.md)
binds the exact source, lockfiles, test selectors, runtime and wheel. Preparation
observations keep their original labels and unchanged before/after inventories.
Final reuse requires identical relevant source and a completed preceding Variant
closeout; only specifically named status prose may change. The prior PR head and
merge have the same tree, and base adoption must preserve the index, working diff
and dirty-file identities. Final documentation and repository-contract checks run
after that adoption. Replay and intentional corruption checks validate this
binding without rerunning native execution. Original failures remain evidence.

## Source grounding

- Pinned `vortex-uuid` 0.93 `lib.rs`, `vtable.rs` and `arrow.rs` define registration,
  version metadata, fixed-size-list storage and native Arrow conversion.
- [Arrow canonical UUID](https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid)
  defines sixteen big-endian bytes and empty extension metadata.
- Pinned Arrow schema 59.3.2 validates the canonical declaration; pinned Parquet
  59.3.2 gates UUID logical annotation mapping behind
  `arrow_canonical_extension_types`.
- RFCs 0011, 0013, 0014, 0017, 0021 and 0044 govern typed operations, streaming,
  memory ownership, cleanup and the shared native execution boundary.
