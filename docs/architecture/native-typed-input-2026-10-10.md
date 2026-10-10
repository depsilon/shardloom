# Exact typed input into the shared native source

Status: implementation contract for the next cohesive PERF-11 unit,
with PERF-03/07/12 and CG-19/20/21 obligations. This draft is not an acceptance
record. Keep version 0.5.1 fixed and preserve the remaining six areas, eight
investigations and CG-1 through CG-23 in the phase plan.

## Scope and compatibility

Extend ordinary Python `from_rows` and `from_batches`, their context methods,
and their CLI memory declarations with exact signed/unsigned integer widths,
finite Float32/Float64, Boolean, UTF8, binary, Decimal128, Date32, microsecond
timestamps without a timezone, lists, fixed-size lists and ordered structs.
The same declarations must work in resident rows, buffered batch composition
and the existing finite, single-use streaming mode.

The current four scalar tokens, their aliases, inference, row wire values and
stable resident declaration identities remain compatible. Rich types require
an explicit schema. Input transport remains bounded to 2,048 rows and 8 MiB
per batch; resident row declarations retain their 65,536-row and 8-MiB envelope.
The cumulative batch and top-level field counts remain admitted by the shared
grant, without reinstating a fixed total. Nested value schemas keep their
current depth, node and metadata policy while traversal work remains open.

This unit does not admit repeated or multiple batch producers, dynamic schemas,
streaming compatibility destinations/fanout, arbitrary extension types, Float16,
Decimal256, nonfinite numbers, timezone interpretation or another engine.
Those source, destination and type families retain their existing owners.

## Python schema contract

Keep ordinary scalar names as strings, including `decimal128(precision,scale)`,
`binary`, `date32` and `timestamp_micros`. Nullable remains the default. A mapping
with a `type` key can specify `nullable: False`; list and struct parameters use
the same mapping form recursively. Reject unknown keys rather than guessing.

```python
schema = {
    "id": {"type": "uint64", "nullable": False},
    "amount": "decimal128(18,2)",
    "payload": "binary",
    "day": "date32",
    "observed_at": "timestamp_micros",
    "samples": {"type": "list", "item": "int32"},
    "coordinates": {"type": "fixed_size_list", "item": "float64", "size": 2},
    "detail": {"type": "struct", "fields": {
        "label": "utf8", "enabled": {"type": "bool", "nullable": False},
    }},
}
```

Use Python integers with exact range checks, finite floating values with exact
Float32 representability when requested, `Decimal` without contextual rounding,
bytes-like binary input, dates and naive datetimes. Integer day/microsecond
storage values also represent temporal values outside Python's calendar range.
Lists accept sequences of values and structs accept mappings with exactly the
declared names. Parent and child nullability are independent. Do not reinterpret
an input string as a decimal, binary value or temporal value without its explicit
type/value contract.

The existing general `WorkflowSource.schema` remains ordered string hints for
planning and reporting. Exact recursive memory types live in the already
separate `memory_input` declaration; the native source supplies the authoritative
schema to SQL and relational binding. Do not change CSV/JSON schema-hint meaning
as a side effect of this unit.

## Native representation and provider decision

Reuse the existing recursive result-type metadata logic at the Python boundary.
Transmit rich input types using the pinned `vortex.dtype.serde.v1` representation
inside `{"native":{"encoding":"vortex.dtype.serde.v1","dtype":"<dtype JSON>"}}`
in the memory schema. Retain the existing four string variants. Retain positional
optional-string cells in the transport;
rich cells contain exact typed JSON values, with decimal and binary payloads
using the existing lossless result representation. This is an explicit input
conversion boundary, not an alternate query evaluator.
The JSON parser retains numeric tokens before conversion: integer tokens must
fit the int64/uint64 domain, and floating literals use the standard exact
decimal-to-Float64 parser before any Float32 representability check. Python
emits floating literals for declared floating values. This avoids generic JSON
number conversion silently rounding an overflowing integer or changing the bits
of a representable Float32 value.

The implementation must preserve ordered fields, precision/scale, widths,
nullability, fixed-list width and recognized extension metadata. Validate type
shape, name/type arity, duplicates, unknown fields, limits and admitted extension
parameters before invoking upstream constructors or deserialization. Pinned
Vortex's struct-field deserializer can otherwise call an asserting constructor
with unequal names and types. Malformed input must yield an ordinary diagnostic
and release all acquired resources.

Vortex-first decision: `use_vortex_native_provider`. Reuse Vortex 0.85.0 DType,
PrimitiveArray, BoolArray, VarBinArray, DecimalArray, ExtensionArray, ListArray,
FixedSizeListArray, StructArray and ChunkedArray under the current feature gates
and session resource owner. Keep upstream API calls isolated in shardloom-vortex.
The pinned generic `builder_with_capacity_in` discards its allocator, so it is
not an admitted construction route. Reuse ShardLoom's reviewed allocator-owned
scalar column construction and safe native array constructors for nested values.
Use the checked Boolean buffer-handle constructor for zero-row values and
validity arrays: the ordinary Boolean constructor shrinks an empty buffer into
an unowned empty slice, which discards its schema-credit anchor. Apply the same
helper to the existing resident and owned Boolean intake paths.
The closed `MemoryColumnValues::TypedJson` conversion variant feeds the existing
`ResidentMemorySource::copy_columns` path. It accepts a serialized native DType
and borrowed cells, never an arbitrary preallocated array. Reuse the existing
scalar result-column builder for native storage, and the existing reserved
container and array constructors for nested assembly. Parse and validate the
restricted native DType shape directly before constructing Vortex types; do not
pass unvalidated input to the provider's general deserializer.
The duplicate-field-rejecting JSON visitor uses the already locked Serde
1.0.229 package (MIT OR Apache-2.0) through an optional direct dependency under
the native feature. The shared manifest retains the existing compatible version
requirement; no package version or default-build dependency footprint changes.
No new execution IR, Arrow execution substrate or query-engine integration is
required. JSON conversion is confined to input admission.

The pinned timestamp scalar provider validates microseconds through a calendar
range narrower than the admitted Int64 storage domain. File or zone minimum/
maximum computation can panic before returning a diagnostic at the endpoints.
Keep full-domain timezone-free microsecond timestamps in the provider's existing
chunked/flat writer, including timestamps inside the already-preserved nested
fields. Reuse one temporal metadata recognizer across intake and persistence.
For a schema containing these timestamps, omit file-wide minimum/maximum
statistics and retain the other pruning statistics: Vortex 0.85.0 exposes only
a global `with_file_statistics` selection, not a per-field selector. Record this
omission and uncompressed timestamp storage in the existing writer certificate.
Ordinary fields retain their layout/encoding policies and zone statistics;
files without these timestamps retain their default file statistics. Date32
storage does not use the restricted timestamp calendar validator. This is
provider admission through the existing native writer, with no invented bounds,
calendar narrowing, external execution or performance claim.

## Resource and lifecycle contract

Reserve schema parsing and cell conversion workspace before allocation. Account
for decoded input trees, child coordinates, growing containers, fixed-list
expansion, values, offsets, validity and construction overlap. Build native
buffers with the session allocator. Retain nested schema and payload credits
through every array/child/buffer alias and slice, including empty input. Reuse
the existing source's pool identity, private streaming ownership and release
witness; do not create a parallel execution source or admit arbitrary arrays
whose allocations have not been reviewed.

Resident composition must validate every batch's complete native schema and keep
the growing shared-grant container. Streaming must bind the typed empty schema
before opening the producer, keep completion provisional, validate every batch,
release each source before further demand and preserve cancellation/cleanup.
Check native logical size before buffer construction, including fixed-list
expansion under null parents. Attach both top-level and nested schema credits
through the existing allocator wrapper. Copy counters must distinguish actual
intake payload copies from bitmap/offset construction and ownership transfer.
Caller-owned Python objects, conversion-library retention, allocator overhead
and process RSS are not thereby covered by the query grant.
Completed batch-source output keeps the standard completed-output byte bound
independent of compact input bytes; chunk metadata and requested JSON syntax can
be larger than narrow native payloads. Null-parent defaults count as storage
initialization, not copied caller payload.

## Acceptance

Extend existing Python declaration/protocol tests and native source tests.
Exercise exact values and types through ordinary public declarations, filtering,
projection, existing typed operators, bounded collection/iteration and native
write/reopen. Check every supported compatibility destination using its existing
fidelity contract; unsupported type/format combinations must reject explicitly.
Include empty/all-null inputs, nested null parents with nonnullable children,
integer endpoints, decimal precision/scale boundaries, binary zeros, Unicode,
full-domain temporal storage, empty lists, fixed-size shape, field order and
typed empty streams. Include malformed/duplicate schemas and values, unknown
extensions, deep input, arithmetic overflow, resource denial, retained aliases,
schema drift, late producer failure, cancellation and destination cleanup.

Run the applicable formatter, clippy, workspace and native feature gates, public
workflow/regression suite and complete-operation acceptance before integration.
Preserve failed observations. Report exact source/executable identities and
resource/certificate evidence; this capability unit makes no speedup or global
roadmap-completion claim and does not authorize another release.
