<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dense embedding and inline rich-column lifecycle

Status: first delivery locally accepted November 5; hosted checks and review
gate integration. This amendment implements the first group in the
[rich-data lifecycle plan](native-rich-data-lifecycle-2026-10-24.md). Managed
objects, tensors, lower precision, sparse/quantized representations and exact
vector expressions retain their separate later contracts. RFCs 0011, 0013,
0014, 0017, 0021, 0033 and 0044 govern this work; item 20 and CG-1 through CG-23
remain independent.

## Persistent type and public declaration

Ordinary dense vectors remain native fixed-size lists of Float32 or Float64.
Their outer and component nullability are independent. An embedding is the
registered Vortex extension `shardloom.embedding.v1`, with exactly that storage
and a positive dimension. The native uint32 list width and an adapter's actual
representation limits apply; there is no additional dimension ceiling.

The extension metadata is a UTF-8 JSON object. `version` is the integer 1. The
optional fields `space_id`, `model_id`, `model_version`,
`representation_version`, `normalization`, `preprocessing_id` and
`source_provenance` are nonempty strings or null. Omitted/null identity is
unknown, not inferred from dimension or model name. Normalization is a supplied
description, never a request to alter values. Per-row provenance belongs in
ordinary companion columns; this metadata describes homogeneous column values.
Unknown keys or versions fail explicitly. Canonical serialization sorts keys,
uses UTF-8 without ASCII escaping and omits whitespace; no identity strings are
normalized. Storage owns dimension, element type and both nullabilities, avoiding
contradictory copies in the metadata. Metadata consumes the existing schema grant.

Python uses the existing schema mapping with `type="embedding"`, `item`, `size`
and `metadata`, or `types.embedding(item, size, metadata=..., nullable=...)`.
Plain fixed-size lists remain available without embedding
identity. Result schemas carry the same declaration. Arrow uses its standard
extension-name and extension-metadata field keys over FixedSizeList storage,
including nested fields. Native Vortex persists the registered extension, and
Arrow IPC/Parquet preserve the declared logical metadata where their adapters
are admitted. Other sinks must reject unsupported logical metadata before
publication rather than silently producing an unannotated list.

## Shared ownership and value semantics

Use the existing native payload, selection, relational, streaming, spill and
result owners. Registered extensions recurse through their storage for copying,
validation and output conversion, then retain their exact logical dtype. Do not
make embeddings scalar keys or arithmetic operands in this delivery. Joins on
ordinary columns carry rich payloads without comparing them.

Reuse the existing session `float_policy`: strict continues rejecting observed
nonfinite input; extended admits it. Carriage preserves Float32/Float64 bits,
including signed zero and NaN payloads, with the existing exact widen/narrow
helpers at Python scalar boundaries. JSON cannot represent nonfinite numbers;
its existing finite-only refusal remains explicit. Native, Python-object and
Arrow delivery carry extended values without passing them through JSON cells.
No rounding, padding, truncation or normalization occurs.

Reservations precede allocation and follow retained metadata/buffers through
source/session drop, sharing, spill and output. Error and cancellation release
owned reservations and query-local temporary files. This does not claim a
whole-process RSS bound or managed-object lifetime support.

Registered native-file metadata decoding reserves JSON workspace before parsing
through the session's existing host allocator. The immutable canonical metadata
keeps its retained reservation through dtype clones and final release. Inert
caller declarations retain their existing bounded-metadata boundary; Arrow and
typed-input adapters hold their schema workspace before invoking the codec.

The pinned Arrow provider expands parent validity when validating a nonnullable
fixed-list child. Controlled intake reserves the largest aligned validation
bitmap before that allocation. The provider bridge discards only child masks
proven unreachable by original Arrow validation, preserving parent validity,
logical nullability and shared value buffers. Denied scratch admission releases
its credits without changing the producer's arrays.

## Pinned provider check

Checked Vortex 0.90.0's local `ExtVTable`, `ExtDType`, dtype serialization,
`ExtensionArray`, take/slice and Arrow importer/exporter registries. It supplies
the extension storage and dispatch mechanism, but no embedding identity codec.
Struct fields carry names and dtypes, not arbitrary Arrow field metadata. Use
the extension metadata instead of a sidecar schema or a parallel payload owner.
ShardLoom's duration adapter is the existing registration pattern. The new
adapter is original code over those APIs, behind the existing native features.
No runtime dependency or external query engine is added.

The first-party `shardloomembedding:2026-11-0` edition admits only this dtype;
frozen Vortex core array/layout/aggregate membership stays unchanged. Spill
sessions copy the dtype registry before rebinding this plugin's allocator, so
receiving metadata belongs to the spill grant without changing its parent.

At the Arrow intake boundary, validated null child slots beneath null Struct or
FixedSizeList parents may occur even when the child is declared nonnullable.
The pinned provider imports children independently. The shared adapter removes
only those unreachable child validity masks after original Arrow validation;
it preserves parent masks, logical nullability, value buffers and their owners.
Columns without such masks keep their existing buffers and representation.

The [Arrow extension specification](https://arrow.apache.org/docs/format/Columnar.html#extension-types)
grounds the field metadata convention; the
[canonical extension catalog](https://arrow.apache.org/docs/format/CanonicalExtensions.html)
keeps tensor shape separate from this application-specific identity. Pinned
source controls actual behavior, rather than current documentation alone.

## Acceptance

One mixed fixture contains ordinary IDs, nullable metadata, inline Binary
(empty, null and nonempty), List/Struct, plain dense vectors and declared
embeddings. Prove import, retained ownership, select/reorder, ordinary filtering,
duplicate/outer joins, bounded delivery, forced native spill, native write,
separate-process reopen and Arrow export against independently constructed full
values and metadata. Exercise Float32/Float64 special bits, child/parent nulls,
slices, empty sources, malformed metadata/dimensions, unknown provenance,
source changes/drop, denied allocation, cancellation and owned cleanup.

Freeze finite controls before measurement. Required default/native Rust checks,
installed-package integration, metadata-preserving adapter tests and independent
artifact inspection precede support claims. No Full43, expanded query campaign,
external baseline rerun, publication or performance claim is implied.

The pinned independent PyArrow 25.0.1 Parquet reader rejects null fixed-size-list
parents, including its own generated files. Keep that failed observation. The
finite controls separately require full native Parquet readback, independent
Parquet production of all rows, independent Parquet readback without null vector
parents, and complete nullable Arrow IPC readback. Do not claim that PyArrow
version reads the nullable Parquet fixture.

Existing depth 24/node 4096/schema metadata bounds remain. The tracked iterative,
resource-backed traversal generalization needs complete lifecycle proof beyond
those boundaries. This first payload delivery does not close that work or the
whole of item 19. The [public guide](../reference/rich-columns.md) records the
precise supported routes and remaining obligations.

## Local verification

The [acceptance record](../release/evidence/native-dense-embedding-lifecycle-2026-11-05.json)
indexes the commands, exact build source identities, original failures and
hash-verified compact observations. No performance result or package publication
is claimed.

| Check | Result |
| --- | --- |
| Default workspace tests | 3,319 passed; no ignored tests |
| Native workspace tests | 6,910 passed; 36 existing opt-in tests ignored |
| Final focused native embedding tests | 9 passed |
| Final rich-column Python tests against the release library | 9 passed |
| Python schema and neighboring delivery regressions | 32 passed |
| Installed native package | 312 passed; no skips; install, identity and uninstall verified |
| Formatting, default/native strict lint, minimal write-feature compilation, generated references, public status and contribution checks | Passed |

The native workspace run precedes the final Arrow validity-scratch reservation.
The focused native tests, strict lint and installed-package suite exercise that
final addition. The hosted checks remain a separate integration gate. The
finite fixture checks complete values and metadata through SQL and DataFrame
selection, ordinary filters/joins/order, actual spill, bounded delivery, native
persistence, separate-process reopening and independent Arrow IPC reading.
Controls cover malformed identity/shape, strict floating policy, denied memory,
retained owners, source changes, early close, cancellation, failed publication
and explicit embedding-key refusal.

Preserve the initial missing wire-edition and unreachable-child-validity
failures, the corrected SQL fixture aliases, the interrupted large assertion
diff, formatting/lint corrections and the independent Parquet limitation above.
They are observations with explicit dispositions, not discarded trials.
