# Native encoded transfer and independent interchange

Status: local runtime, public regression, independent packet inspection and
installed-wheel acceptance pass; hosted integration remains pending. The
[acceptance report](../benchmarks/native-encoded-interchange-2026-10-12.md)
records the frozen builds, complete values, copy/lifecycle gates and preserved
failures. The later [query revisit](../benchmarks/native-query-revisit-2026-10-12.md)
records three retained changes and one drop, with cumulative native, Full43 and
package acceptance passing. Its later timings remain separate from the earlier
encoded-transfer comparison; hosted integration is still pending.
The subsequent [Null-schema review correction](../benchmarks/native-query-revisit-2026-10-12.md#pr-review-correction)
passes fresh native, package and complete Full43 checks with a separate source
and evidence packet. Historical comparison artifacts retain their original identities.
This finite unit follows persisted-output reuse in PR #1544. It covers the
foreign-owner, reusable-source, native-recognition and independent-producer
items in PERF-07 under PERF-02/03/11/12 and CG-19/20/21. Other composition,
resource, operator and compatibility-sink work remains with its existing owners.
Version 0.5.1 stays fixed; this work does not authorize another publication.

## Problem and existing owners

`ResidentMemorySource::in_session` shares same-owner sources and reconstructs
generated ranges compactly. Before this unit, a foreign source or completed result
passed through `MemoryBatchSourceBuilder::push_native_copy`: slice into at most
2,048 rows, detach into receiving-owned native buffers, then compose a resident
source. Values and isolation are preserved, but compressed input can expand and
repeated dictionary payloads can be copied many times.

The accepted receiving `NativeSource` and prepared handles already survive
later operations. Extend that path and prove its reuse; do not introduce a
global result cache or treat a previous query answer as later execution.
Source intake, planning and execution retain the explicit allocation contract.

Native files already enter through `read`, `read_vortex`, context/session
methods and admitted SQL file references. Native objects enter through the
existing Arrow-compatible intake: exact ShardLoom result wrappers unwrap to
`NativeResult`, and direct `Session.from_arrow` recognizes `NativeResult` and
`NativeSource` before asking for an Arrow stream. This is an existing native
special case, not proof of arbitrary upstream Python-object support.

## Vortex-first decision

The decision is `use_vortex_native_provider`. Pinned Vortex 0.85 already exposes
`ArrayRef`, typed construction parts, array plugins, a session registry,
serialized arrays and native IPC messages. Use those physical representations
inside `shardloom-vortex`, with checked construction, receiving allocation and
explicit evidence. No external query-engine integration participates.

The source inventory covers canonical arrays, Constant, Chunked, Dict, Sparse,
RunEnd, BitPacked, FoR, ZigZag, Sequence, ALP/ALPRD, FSST and Zstd. Native file
reads also use `SharedArray`; transfer reconstructs its encoded child and a
fresh cache, discarding the producer's cached arrays or errors. Serialization
does not mean allocation-free: Constant synthesizes a scalar buffer, Chunked
reconstructs cached offsets, and some codecs reconstruct dictionaries or other
metadata. Admitting copied payload buffers alone cannot account for that state.

Upstream Python 0.85 publishes `serde.encode_ipc_array_buffers`, returning array
and dtype message buffers. The released wheel's relevant public wrappers and
stubs match the inspected commit, but their compiled behavior has not yet been
executed. Its mandatory package dependencies include Substrait, and normal
package import reaches it. This work does not add that dependency. The private
`__vortex_array_metadata__` capsule is not an admitted public interchange ABI.

Use an independent, unpatched crates.io Vortex 0.85 producer process for runtime
feasibility. Its fixture/export utility is a test producer, built outside the
ShardLoom workspace and without a query-engine integration. Record its exact
lockfile, toolchain, source and executable identity. Public native files remain
the existing interoperability route; IPC is admitted only after the tests below.

Sources: the local pinned provider and
[Python 0.85 package metadata](https://pypi.org/pypi/vortex-data/0.85.0/json),
the [pinned Python serde implementation](https://github.com/vortex-data/vortex/blob/afb005379dd9a1b7dc7ae2cb49f4f465d91cc2b3/vortex-python/src/serde/mod.rs),
and [Vortex serialization concepts](https://docs.vortex.dev/developer-guide/internals/serialization).
The current documentation is conceptual guidance, not evidence that a later
provider version has the same behavior.

## Receiving ownership

Copy a supported physical array tree into the receiving session without
canonicalizing its leaf encodings. Use operation-local identity maps to avoid
recopying shared array nodes and identical immutable buffers. Identity is valid
only while the original tree is retained during that transfer; it must not
become a cross-operation address cache. Preserve dtype, validity, offsets,
dictionary codes, row order and admitted encoding identities.

Reserve traversal containers, schema, serialized metadata and provider scratch
before their allocations. Copy host buffers through the existing reserved
allocator in cancellable byte windows. Charge alignment and allocation capacity,
not only logical lengths. Device-resident buffers remain unadmitted here.
Discard source statistics during reconstruction unless independently copied
under an explicit bounded rule; a source statistic must not keep its foreign
allocation alive in a receiving object.

Use the safe plugin construction path for admitted serializers. Treat constants
and other synthesized-buffer cases explicitly so a large scalar is not expanded
or serialized before reservation. Classify an unsupported physical strategy
before transfer and retain the existing bounded native materialization path
where its semantics are already admitted. Cancellation, corruption and memory
denial are errors, never reasons to retry with an unreported strategy.

Consecutive relational selections use native slices instead of building a dense
u64 gather index. Gaps, repeats and reordered rows retain the indexed path. This
keeps scan and filter output compact before it reaches the transfer boundary.
For foreign sources/results whose record fields are all integers or booleans,
the receiving plan compares deduplicated physical buffer bytes with the bounded
materializer's value bytes plus worst-case nullable bitmaps. When physical copies
would be larger, it selects that existing native materializer before allocating
copied payloads; its batch and row counts remain explicit in the intake report.
The comparison set uses pre-admitted bookkeeping credit and is released before
copying. Equal costs retain encoding. This handles short packed views and small
chunk containers without copying unused parent data or redundant offset buffers.
Floats, decimals, extensions and nested values keep their physical strategy so a
size decision cannot narrow their admitted value semantics. Independent IPC
reconstruction preserves its validated encodings rather than applying this
foreign-object strategy. These byte comparisons select representation only;
they do not substitute for allocator capacity, alignment or workspace admission.

The provider needs a small lifetime hook for non-buffer state: an optional opaque
owner retained by each reconstructed array node. It must survive array clones,
child extraction, slices, construction-parts round trips and structural rewrites,
and release after the encoding data. Copied buffers retain their own receiving
credit when extracted independently. Attach owners to newly constructed arrays;
do not mutate shared source arrays or attach receiving credit to foreign buffers.
The hook changes no encoding, serialized format or execution semantics.
An ownership-bearing slice remains a native `SliceArray` view until admitted
execution, including an empty or repeatedly sliced view. Eager reduction of a
Constant would otherwise rebuild its bufferless metadata without the owner.
The view retains both its parent and the existing metadata credit; ordinary
arrays without this owner keep their existing eager slice optimization.
Explicit slice execution reduces the retained view once its child is canonical;
returning a fresh retained view at that point would otherwise repeat execution.

The focused implementation checks pass 75 resident-source tests, three relational
selection tests and 10 provider ownership tests. Cached patch coordinates are
preserved through checked native parts for Sparse, ALP, ALPRD and BitPacked;
ALPRD dictionaries use the receiving
allocator even when extracted separately. Cancellation between 64-KiB copy
windows and denial after a completed child release all partial receiving credit.
The measured common-array allocation increase is 16 bytes on the current
64-bit target, with the `ArrayRef` handle unchanged. The separate acceptance
report records the completed local retention and integration gates; hosted
acceptance remains pending.

This hook governs native array and buffer lifetimes, not arbitrary copies of a
bare dtype or encoding-specific Rust data object. New allocations made by later
execution remain governed by that execution's own admission. Test the actual
escape paths exposed by ShardLoom rather than claim coverage of all upstream
allocations or whole-process RSS. Record the common-array footprint change and
include unchanged same-owner workloads in the regression gate.

## Public recognition and independent IPC

Preserve current API spellings and their supported behavior. Centralize exact
native-wrapper recognition where shared entry points currently duplicate it;
document that native results/sources need no Arrow export. Do not inspect an
arbitrary object's similarly named attribute, implicitly import upstream Python
packages, or accept its private capsule. Existing native files keep their lazy
file-source path and validation.

The IPC feasibility boundary consumes the pinned public Vortex message format,
not Python object internals. Admit the receiving grant before inspecting producer
callbacks or copying buffers. Bound total transport bytes and structural work
before decoding. Validate message lengths, versions, dtype, rows, registry names,
alignment, children and depth before expensive construction; reject unknown
encoding names before the provider interns them globally. Preserve cancellation
and deterministic errors for truncated or inconsistent input.

Do not publish a new stable Python spelling until the independent producer and
resource/corruption gates pass. A feasible boundary must lower into the same
`ResidentMemorySource` and execution owners. If a required public contract is
unavailable, retain an exact evidence-backed external blocker and the working
native-file alternative; a generic deferral does not close feasibility.

The independent unpatched producer has now exported four complete cohorts:
4,099 mixed nullable rows, 65,539 encoded rows, 257 nested rows and empty records.
The twelve retained files comprise public native files and separate dtype/array
IPC messages, with producer source, lockfile and provenance in
[`tests/fixtures/independent-vortex-0.85`](../../shardloom-vortex/tests/fixtures/independent-vortex-0.85/README.md).
Receiving tests compare every field and value, schema and row order, preserve
the claimed physical encodings, and run different later projections using
prepared handles after dropping transport buffers. Native-file readback is
checked independently. These are correctness and ownership results; the fixture
export observations are not comparative performance samples.

The source implementation exposes `shardloom.from_vortex_ipc`,
`ShardLoomContext.from_vortex_ipc` and native `Session.from_vortex_ipc`.
The Python helpers accept immutable `bytes` containing one complete array
message and one complete dtype message, use the native transport and require
receiving resources. The admitted boundary is Vortex 0.85 message version V0:

- Combined transport at most 64 MiB; each header, dtype body and array metadata
  at most 8 MiB. Schema and array depth are at most 24, with at most 4,096 nodes
  each and 4,096 physical buffers. FlatBuffer verification has its own bounded
  depth, table and apparent-size limits.
- Nonnullable record rows with the existing
  [native typed input schema](native-typed-input-2026-10-10.md), plus physical
  `DType::Null` fields and nested Null list/struct elements. Physical schema
  retention does not admit Null execution keys or widen typed input binding.
  Unknown extension identifiers are rejected before interning.
- The closed physical registry admits Null, Bool, Primitive, Decimal, VarBin,
  VarBinView, List, ListView, FixedSizeList, Struct, Extension, Constant, Dict,
  Chunked, RunEnd and FastLanes BitPacked. This IPC registry is deliberately
  narrower than trusted in-process transfer's provider set.
- Host buffers with uncompressed transport and alignment up to 65,536 bytes.
  Unknown/duplicate registry names, unsupported versions, trailing bytes,
  malformed coordinates and unsupported compression fail explicitly.
  A nonempty RunEnd array must contain run ends, including when it is nested
  below another record. Zero-row arrays may retain empty run ends.

Wire statistics and dictionary optimization assertions are not trusted. Derived
BitPacked patch caches are reconstructed without trusting their wire cache
coordinates. Validating dictionary codes, offsets, run ends and patches can
execute coordinate arrays; their explicit execution count and checked values
are reported separately from retained encoded copies. Provider-internal checks
are outside those counters. No claim of universally decode-free import is made.

The immutable source intake receipt records encoded array/buffer copies and
reuse, bounded materialization, retained metadata and provider scratch. IPC adds
transport bytes, its temporary buffer copies, decoder workspace and coordinate
validation work. The receiving allocator separately accounts for capacity and
alignment. Source reuse does not increment intake counters or imply reuse of a
query answer. All counters describe this intake boundary, not process RSS,
foreign producer allocations or all internal provider work.

## Verification and retention gate

Before accepting the unit:

- Compare complete schema, values, validity and row order for empty, all-null,
  constant, dictionary, run-end, packed numeric, string, temporal and nested
  inputs, including shared children and buffer slices. Verify physical encoding
  identities where preservation is claimed.
- Drop producer/result/session handles before consuming receiving sources and
  escaped children/buffers; prove foreign credits return and receiving credits
  remain until their last supported owner drops. Cover failed admission,
  cancellation during a large copy, partial construction and operation reuse.
- Run later different queries against one completed receiving transfer and one
  prepared source. Show actual later execution with no repeated transfer; keep
  same-owner and compact generated-range controls.
- Produce native file and IPC fixtures in an independent process. Verify export,
  transport/import, first execution and subsequent execution separately, then
  compare their complete lifecycle. Exercise malformed headers, unknown
  registries, unsupported versions, alignment and depth/size pressure.
- Record source/receiver bytes, encoded buffer copies, materialized/decoded work,
  metadata/scratch reservations and observed lifecycle time. A resident array
  byte count is not a process-RSS or physical-device-byte measurement.
- Freeze paired fixtures, sample count and gates before measuring. Require a
  material reduction in copied bytes on expanding encoded cohorts; first-transfer
  and complete-lifecycle costs must not regress by more than the greater of 5%
  or 5 ms. Apply the same tolerance to same-owner, generated-range and small
  primitive controls. Retain all samples and failed attempts. A failed candidate
  needs a measured revision or explicit drop, not a relaxed threshold.
- Pass focused provider/ownership tests, required Cargo format/lint/workspace
  gates, native-feature and Python integration tests, complete public and
  retained Full43 regressions, relevant writer/readback checks, packaging/license
  validation, docs and hosted integration. Timings remain bound to their actual
  builds. Existing evidence cannot be reassigned to changed runtime code.

No file-format fork, hidden cross-owner sharing, query-answer substitution,
external execution fallback or package release is included. Keep
`fallback_attempted=false` and `external_engine_invoked=false`. All six remaining
areas, eight investigations and CG-1 through CG-23 keep their existing owners.
