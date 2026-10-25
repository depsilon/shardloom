<!-- SPDX-License-Identifier: Apache-2.0 -->

# Vectors, tensors and binary objects through ordinary data workflows

Status: accepted October 24 maintainer addition, following
[full-ingest ownership and preparation](native-ingest-ownership-preparation-2026-10-24.md),
then the [connected programming model](native-connected-programming-model-2026-10-24.md),
then [universal I/O](native-universal-io-performance-coverage-2026-10-24.md),
under the subsequent October 24 ordering decision. Rich-data lifecycle support
immediately follows universal I/O. Surface parity, the
[controlled provider refresh](native-vortex-provider-refresh-2026-10-24.md),
warm source reuse and public naming retain their preceding positions. The subsequent
[everyday analytical completion](phased-execution-plan.md#everyday-analytical-completion)
adds statistics, temporal fidelity, broader numeric semantics and batch functions.
The later [AI-data package](native-ai-data-workflows-2026-10-24.md) connects these
same types and exact expressions to model batches, fitted features and retrieval;
it does not duplicate their storage or arithmetic implementation.
This is queued implementation, not a claim of complete rich-type support.
RFCs 0011, 0013, 0014, 0017, 0021, 0033 and 0044 govern extension semantics,
streaming, ownership, recovery, expressions and shared native execution.
CG-1 through CG-23 remain independent certification gates.

## Decision and source grounding

ShardLoom must store and carry ordinary columns, nested values, vectors,
embeddings, tensors and opaque binary payloads through the same native engine.
Storage and relational preservation come first; useful exact operations follow.
Model generation, OCR, transcription, media decoding and approximate indexes
are separate optional capabilities. None is a prerequisite for storing or
exporting caller-supplied values. A URI alone does not establish stored payloads.

The supplied review targeted #1595 at `16ff752d`; the current worktree confirms
these concrete foundations and gaps:

- [`native_arrow_conversion.rs`](../../shardloom-vortex/src/native_arrow_conversion.rs)
  maps Binary, List, FixedSizeList and Struct recursively. Its `data_type`
  rejects F16 and admits extension types only through the existing temporal
  mapping. Accepting a physical list does not prove preservation of tensor or
  embedding semantics.
- Python [`schema.rs`](../../shardloom-python/src/schema.rs) describes fixed-size
  lists and [`rows.rs`](../../shardloom-python/src/rows.rs) converts their values.
  These are existing interfaces to extend, not proof of complete metadata or
  lower-precision round trips.
- [`unstructured_workflow.rs`](../../shardloom-core/src/unstructured_workflow.rs)
  explicitly defines report-only boundaries. It supplies useful provenance and
  effect requirements, but does not implement media storage or native retrieval.

Recheck actual reader, result, join, selection, spill and writer consumers when
implementation begins. Reuse their ownership and type machinery. Inspect the
pinned Vortex provider before introducing a type wrapper, layout or operation;
upstream provider support must still pass ShardLoom admission and certificates.
Do not add a parallel storage engine or query-engine integration.

The subsequent 0.90 intake establishes a concrete Parquet Variant provider,
including shredded values and Arrow/JSON conversion. Extend this package's
fidelity work to Variant, UUID and useful spatial types through the same
admission and native lifecycle. Preserve extension metadata through dictionary
import as well as ordinary fields. Add Variant field access and a bounded useful
spatial function set only after preservation, with explicit coordinate and
distance semantics. Do not build a second semi-structured store or a complete
GIS product. Tensor/vector providers and lower precision retain the same
read–compute–write–reopen proof requirements below.

## Competitive baseline and separate obligations

The maintainer's subsequent October 24 clarification makes ordinary rich columns
competitive completeness. Storing vectors or document/media bytes is already an
implemented capability in established engines; it does not establish a new
category or a ShardLoom advantage. Official documentation checked for this plan
provides these concrete comparison targets:

| Reference | Implemented comparison surface | Boundary to verify separately |
| --- | --- | --- |
| Polars | [Fixed-shape multidimensional Array and variable-length List](https://docs.pola.rs/user-guide/expressions/lists-and-arrays/), plus [Binary values](https://docs.pola.rs/api/python/stable/reference/api/polars.datatypes.Binary.html) | Type/shape guarantees in memory do not establish every writer/reader round trip. |
| Spark | [SQL arrays and binary values](https://spark.apache.org/docs/latest/sql-ref-datatypes.html), [sparse vector size/indices/values](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.linalg.SparseVector.html), and [binaryFile content with file metadata](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html) | SQL array types do not declare fixed length; binaryFile does not write records back to the original individual files. |
| DuckDB | [Fixed ARRAY, variable LIST and BLOB](https://duckdb.org/docs/preview/sql/data_types/overview), [read_blob](https://duckdb.org/docs/2.0/sql/functions/blob), and [exact array inner-product, distance and cosine functions](https://duckdb.org/docs/current/sql/functions/array) | Exact vector operations have their own dimension/null contracts; indexes are separate from storing or scanning arrays. |

These links establish comparison capabilities, not executed comparative results.
Pin actual versions and supported equivalent representations before measurements;
documentation channels are not a common benchmark version. External engines
remain explicit references/oracles only, never ShardLoom execution providers.

Accept baseline Binary/List/fixed-size-list/Struct preservation independently
from managed large-object storage. The latter needs its own lazy-range,
publication, live-reader, retention and cleanup proof. Neither a binary type nor
a general “unstructured support” label certifies those guarantees. Decoded pixel
arrays and original encoded image bytes are distinct supplied representations;
neither requires an image model. Plain numeric arrays need no embedding identity.
Apply model/space/preprocessing contracts when a value is declared an embedding,
without pretending that matching dimensions prove semantic compatibility.

The opportunity to establish is complete-workflow efficiency, fidelity, resource
behavior and usability. Any advantage requires the mixed workload below and
measured reads, copies, decoding and materialization, not a list of type names.

## Ordered implementation and acceptance

1. **Baseline rich columns, dense vectors and embedding metadata.** Carry actual
   inline binary values, numeric lists and nested structs alongside fixed-size numeric lists
   for Float32 and Float64, retaining exact element type, dimension, values and
   separate vector/component nullability. Preserve non-finite values and signed
   zero according to the declared representation; storage must not normalize,
   pad, truncate, round or replace values. Validate sizes with checked arithmetic
   and real representation/resource limits, without a product-level arbitrary
   dimension cap. A kernel limitation must not make otherwise valid data
   unstoreable. For declared embeddings, preserve embedding-space/model identity, representation version,
   dimension/type, normalization state, relevant preprocessing identity and
   source-content provenance. Column/dataset metadata may hold homogeneous
   information. Unknown embedding provenance may be carried; matching dimensions
   alone never authorize semantic comparison across spaces.
2. **Managed binary objects and lazy access.** After baseline inline-byte
   preservation passes, give managed storage its own acceptance. Support actual inline bytes,
   locally managed large-object bytes and explicitly externally owned references
   as distinct ownership cases. Define the persisted descriptor, identity,
   range, publication and retention contracts before implementation. Ordinary
   metadata filtering/projection must avoid unselected payload reads where the
   layout permits. Selected payloads require complete-byte retrieval and bounded
   streaming/range access; missing or truncated bytes must fail explicitly,
   never become a successful empty placeholder. A live dataset, retained result
   or reader must protect its owned payloads. Publication cannot expose a
   dataset with missing required bytes. Cancellation and failed publication
   reclaim only task-owned uncommitted objects; deleting one reference cannot
   remove another live owner's content. Cleanup must remain within its managed
   namespace and must not delete external objects. Portable export must declare
   whether it includes bytes, requires a companion bundle or retains external
   references, and complete owned export must reopen after its source is gone.
   Establish the local contract before adding remote storage providers.
3. **Tensor shape and interoperable export.** Preserve element type, shape,
   dimension order/names, nullability and values. Implement fixed-shape first,
   then variable-shape through the same type framework. Validate shape products,
   offsets and permutations against storage without treating a flattened list
   as sufficient. Use Arrow's canonical tensor extension definitions as the
   interoperability target, checking pinned provider support. Unknown extension
   identity and metadata must be preserved when admitted as opaque storage or
   rejected explicitly; they must not silently become an unannotated list.
4. **Multiple, sparse, quantized and lower-precision values.** A collection of
   fixed-dimensional vectors is distinct from arbitrary dimensions in one
   vector column. Preserve sparse dimension, index width, values and the stated
   duplicate/ordering policy without silent sorting, merging or densification.
   Preserve supplied quantization codes and interpretation parameters exactly;
   original floating-point reconstruction is a separate operation. Complete
   admitted lower-precision mappings, including Float16, across the full
   lifecycle. Any new lossy conversion requires explicit selection and reporting.
5. **Shared exact vector expressions.** Add dimension/shape inspection, element
   access, slicing, dot product, L2 distance, cosine similarity and explicit
   normalization through the existing shared expression/registry machinery.
   Define result type, accumulation precision, dimensional/space compatibility,
   null components, zero norms, non-finite values and failure behavior first.
   SQL and DataFrames use the same native semantics. Exact filtered similarity
   scans must work without an ANN index; indexing remains a separate later
   strategy with its own quality and lifecycle evidence.

The mixed dense-vector/inline-binary/nested lifecycle is the first cohesive delivery. Managed objects
need a substantial persistence/ownership implementation, and tensor fidelity
depends on metadata handling across existing consumers. Do not describe these
as mere public type aliases. Keep ingestion's allocator/provenance correction
as the prerequisite; do not reopen rejected compression or overlap candidates.

## One mixed lifecycle workload

Coordinate logical metadata and non-finite/lower-precision representation with
the temporal and numeric completion packages. Preserve timestamp precision and
timezone identity through capable inputs/outputs; do not require applications to
strip metadata to enter an otherwise supported workflow. Value preservation and
arithmetic admission are separate claims. Broader NaN/infinity semantics and
explicit decimal rounding extend the shared type/expression contract while
retaining strict validation; they must not silently change this package's exact
round-trip expectations. Replace demonstrated nested depth/node ceilings with
iterative accounting and resource admission, covering complete lifecycle and
cleanup beyond the previous bounds rather than simply deleting checks.

Use a dataset containing document and portfolio IDs, ordinary nullable fields,
nested attributes, a dense vector, an embedding-space identity, a tensor and
opaque binary content. Include empty and missing payloads as distinct cases.
Import/read → retain → select/reorder → filter ordinary fields → join reporting
metadata → bounded streaming and forced native spill → write → reopen in a
separate process → export must preserve complete selected values and metadata.
Compare independently produced/read values, not only row counts or a schema
label. Exercise SQL and DataFrame declarations against the same expected data.

Payload preservation does not require payload comparison or arithmetic: a join
on document ID must carry vectors, tensors and bytes without evaluating them.
Instrument unselected payload reads, selected range boundaries, byte completion,
copying, decoding and materialization at each execution and I/O boundary. Use
ordinary-column filtering and joins to establish whether untouched rich values
are carried efficiently before measuring vector arithmetic. Test slices/nonzero offsets, nested nulls, duplicate join matches,
empty datasets, non-finite and lower-precision representations, unfamiliar
provenance, malformed dimensions/metadata, changed sources, source/context drop,
concurrent owners, resource shrink, cancellation, denied allocations, partial
publication, cleanup failures and last-owner release. Native spill must retain
logical metadata and managed ownership; a descriptor alone is insufficient if
its referenced bytes can disappear before the result is consumed.

Vortex remains the highest-fidelity persistence target. Capable columnar adapters
must retain admitted logical identity and metadata deliberately. CSV/JSON and
other limited targets need explicit reversible representations/schema companions
or requested, reported metadata loss. Extend the preceding universal-I/O package
with mappings for newly admitted rich types; implement the necessary native and
Arrow round trips here instead of waiting for every format family. Read/write/spill
capabilities and arithmetic capabilities must be reported separately and only
after executable proof. A report-only checklist cannot close this package.

Use existing admission, cancellation, certificates and conformance tooling.
Freeze finite resource/performance controls before selected measurements. Mixed
payload and large-object workloads establish these claims; scalar ClickBench
results alone do not. Preserve engine clocks and the isolated Full43 procedure
for regression acceptance. No new runtime dependency, external effect, engine
fallback, package publication or CG completion follows from this queue addition.

## Design references and remaining decisions

[Vortex DTypes](https://docs.vortex.dev/concepts/dtypes) distinguish logical
types from physical encodings and define fixed-size lists and extensions with
identity, storage and metadata. Check the repository's pinned version rather
than assuming the latest documentation is its exact API.
[Arrow canonical extensions](https://arrow.apache.org/docs/format/CanonicalExtensions.html)
define fixed-shape and variable-shape tensor storage and shape/layout metadata.
These inform interoperability, without making Arrow the execution substrate.
[Lance's blob specification](https://lance.org/format/table/blob/) is a design
reference for descriptors, payload ranges, ownership and retained versions;
it does not authorize using Lance as ShardLoom storage or copying its code.

Before each implementation group, settle the smallest missing persistent-format
and type contract: extension/version identifiers and metadata propagation;
managed-object staging, recovery, reference lifetime and portable packaging;
then exact arithmetic/null/precision rules. Record those decisions in the
governing RFCs or a focused design amendment. Prefer extending existing native
providers over an independent rich-data subsystem. The principal risks are
metadata loss at a consumer boundary, premature payload deletion, eager payload
reads, uncredited retention and implicit lossy conversion; the lifecycle and
failure tests above are the acceptance checks for those risks.
