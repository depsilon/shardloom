# RFC 0011: Modular Extensibility for SQL, UDFs, Unstructured Data, LLM Calls, API Calls, and Embeddings

## Status

Accepted as modular extension contract; implementation deferred.

This RFC defines extension boundaries and evidence contracts. It does not
authorize SQL execution, UDF execution, OCR, media decoding, transcription,
embedding generation, LLM calls, model inference, API calls, vector indexes,
external-service dependencies, package publication, or fallback execution.

## Summary

This RFC defines ShardLoom's modular extensibility architecture for common and adjacent workloads.

ShardLoom's core execution engine is Vortex-native and encoded-columnar, but users and LLM agents
will expect it to gracefully support familiar and adjacent workflows:

- SQL.
- UDFs.
- Unstructured data.
- LLM calls.
- API calls.
- Embeddings.
- Vector search.
- Retrieval-augmented workflows.
- Agent-driven implementation in existing repositories.

These features may not all be in ShardLoom's initial scope, but the architecture should be designed
so that they can be added without violating ShardLoom's core principles.

## Context

Real-world data work rarely stays inside one clean analytical boundary.

Users often need to:

- Run SQL-like queries.
- Register scalar, aggregate, or table functions.
- Use custom business logic.
- Process text, documents, logs, media metadata, or nested JSON.
- Call external APIs.
- Generate embeddings.
- Query vector indexes.
- Use LLMs to summarize, classify, extract, or explain.
- Integrate an engine into a repo through a coding agent.

ShardLoom should not become an LLM framework, API gateway, vector database, or unstructured-data
platform. However, it should expose modular extension points so these workflows can be composed
cleanly around the native execution engine.

## Goals

- Support familiar SQL-like usage through a modular frontend design.
- Support UDFs through explicit function registries.
- Support unstructured data through typed object/chunk/metadata abstractions.
- Support LLM calls through explicit effectful operators.
- Support API calls through explicit external-call operators.
- Support embeddings through vector and embedding abstractions.
- Support agent-driven integration through capability discovery and deterministic diagnostics.
- Preserve native Vortex input/output.
- Preserve no-fallback execution.
- Preserve performance and correctness through explicit boundaries.
- Avoid turning hidden side effects into normal query execution.

## Non-goals

- Do not implement SQL in this RFC.
- Do not implement UDFs in this RFC.
- Do not implement LLM calls in this RFC.
- Do not implement API calls in this RFC.
- Do not implement embeddings in this RFC.
- Do not implement vector indexes in this RFC.
- Do not add dependencies in this RFC.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not make ShardLoom an LLM framework.
- Do not make ShardLoom a vector database.
- Do not make ShardLoom an API orchestration product.
- Do not hide external calls inside ordinary deterministic execution.

## Design principle

ShardLoom should support adjacent workloads through modular boundaries, not hidden coupling.

The core principle is:

> Keep deterministic encoded execution pure where possible, and model non-deterministic or external
  effects explicitly.

This enables ShardLoom to stay fast, explainable, safe, and agent-friendly.

## Core extension model

ShardLoom should eventually expose a modular extension architecture with these conceptual
registries:

- FrontendRegistry.
- FunctionRegistry.
- ExtensionTypeRegistry.
- ConnectorRegistry.
- EffectRegistry.
- EmbeddingRegistry.
- VectorIndexRegistry.
- CapabilityRegistry.

These registries define what ShardLoom can parse, plan, execute, call, translate, or expose.

## FrontendRegistry

The FrontendRegistry should allow ShardLoom to accept familiar user-facing syntaxes without changing
the core execution model.

Potential frontends:

- SQL.
- DataFrame-style API.
- Query builder API.
- Config-driven jobs.
- Agent-generated plan specs.

A frontend converts user input into ShardLoom logical plans.

A frontend must not execute work itself.

A frontend must not delegate unsupported execution to Spark, DataFusion, or another engine.

## SQL support

ShardLoom should eventually support common SQL in a familiar way.

Initial SQL design should prioritize:

- SELECT.
- FROM.
- WHERE.
- PROJECT.
- LIMIT.
- ORDER BY when supported.
- GROUP BY when supported.
- Basic aggregates.
- Basic joins when supported.
- Function calls.
- Explicit diagnostics for unsupported SQL.

SQL should be a frontend into ShardLoom's planner.

SQL support should not imply DataFusion execution.

Unsupported SQL should produce clear diagnostics.

Example unsupported diagnostic:

```json
{
  "code": "SL_SQL_UNSUPPORTED_WINDOW_FUNCTION",
  "category": "unsupported_sql",
  "feature": "window_function",
  "fallback_attempted": false,
  "message": "ShardLoom does not yet support SQL window functions in native execution.",
  "suggested_next_step": "Remove the window function, use a supported aggregate, or track native support."
}
```

## FunctionRegistry

The FunctionRegistry should manage UDFs and built-in functions.

Function categories may include:

- Scalar functions.
- Aggregate functions.
- Table functions.
- Predicate functions.
- Encoding-aware functions.
- Translation functions.
- Effectful functions.

Every function should declare:

- Name.
- Input types.
- Output type.
- Null behavior.
- Determinism.
- Purity/effect level.
- Batch behavior.
- Encoded execution capability.
- Materialization requirements.
- Cost hints.
- Unsupported diagnostics.

## UDF support

UDFs should be easy to register but explicit in behavior.

Potential future UDF kinds:

- Rust-native UDFs.
- WASM UDFs.
- Python UDFs.
- SQL-defined UDFs.
- External service UDFs.

Initial design should favor:

- Batch-oriented functions.
- Typed inputs and outputs.
- Explicit null semantics.
- Deterministic diagnostics.
- Clear encoded-vs-decoded capability.
- Explicit materialization requirements.

UDFs must not become a way to hide fallback execution.

## Purity and effect levels

ShardLoom should classify operations by effect level.

Suggested levels:

### PureDeterministic

The operation is deterministic and has no external effects.

Examples:

- Arithmetic.
- String normalization.
- Date extraction.
- Predicate evaluation.

### PureNondeterministic

The operation has no external side effects but may produce different results across runs.

Examples:

- Random values.
- Current timestamp.

### ExternalRead

The operation reads from an external system but does not mutate it.

Examples:

- API lookup.
- External metadata fetch.
- Vector index search.

### ExternalWrite

The operation writes to an external system.

Examples:

- API mutation.
- Ticket creation.
- External workflow trigger.

### ModelCall

The operation calls an LLM or ML model.

Examples:

- Summarization.
- Classification.
- Extraction.
- Embedding generation.

Every non-pure operation must be explicit in the plan.

## EffectRegistry

The EffectRegistry should manage external or non-deterministic operations.

Effectful operations must declare:

- Name.
- Input contract.
- Output contract.
- Effect level.
- Idempotency behavior.
- Retry policy.
- Timeout policy.
- Cost model.
- Rate limit behavior.
- Authentication requirements.
- Caching behavior.
- Redaction requirements.
- Failure behavior.
- Dry-run behavior.

Effectful operations must not run implicitly during explain, estimate, or dry run.

## LLM calls

ShardLoom should support LLM calls as explicit effectful operations, not hidden query magic.

Potential LLM operations:

- Summarize.
- Classify.
- Extract.
- Normalize.
- Explain.
- Generate embeddings.
- Validate text.
- Generate structured output.

LLM operations should declare:

- Model provider.
- Model name or alias.
- Prompt/template id.
- Input columns or chunks.
- Output schema.
- Token/cost budget.
- Timeout.
- Retry policy.
- Caching key.
- Safety/redaction policy.
- Determinism expectation.
- Evaluation requirements.

LLM calls should be visible in explain output.

LLM calls should be optional and modular. ShardLoom's core Vortex-native execution must not depend
on them.

## API calls

API calls should be explicit effectful operations.

API operations should declare:

- Endpoint or connector.
- Method.
- Input mapping.
- Output schema.
- Authentication boundary.
- Timeout.
- Retry policy.
- Rate limit.
- Idempotency key.
- Cache behavior.
- Error behavior.
- Dry-run behavior.
- Whether the operation mutates external state.

External writes should require stronger safety controls than external reads.

API calls must not be hidden inside ordinary filters or projections.

## Unstructured data

ShardLoom should support unstructured data through typed references and extracted representations.

Unstructured sources may include:

- Text files.
- JSON documents.
- Logs.
- PDFs.
- Images.
- Audio metadata.
- Video metadata.
- Web/API responses.
- Document chunks.
- Blob/object references.

ShardLoom does not need to process all media types directly at first.

Instead, it should design for these abstractions:

- ObjectRef.
- BlobRef.
- DocumentRef.
- ChunkRef.
- ExtractedField.
- ExtractionPlan.
- ChunkManifest.
- EmbeddingRef.
- VectorIndexRef.

Unstructured data should be handled as:

1. Metadata that can be queried.
2. References to payloads.
3. Chunks that can be embedded or processed.
4. Extracted structured fields.
5. Optional external/model processing steps.

ShardLoom should not be the media or model runtime by default. Pipeline code,
Foundry media transforms, AIP Logic, approved model services, or user-owned
orchestration should perform OCR, transcription, document conversion, image
tiling, embedding generation, LLM calls, model inference, retries, rate limits,
prompt handling, human review, and platform-specific credential behavior.

ShardLoom should own the contracts around those stages:

- Media references.
- Media and document manifests.
- Extracted text and chunk tables.
- Embedding/vector tables.
- Extraction provenance.
- Model-call boundary reports.
- Effect, cost, credential, redaction, and retention policy.
- Data-quality checks.
- Lineage and certificates.
- Downstream structured analytics over extracted outputs.

## Media references and manifests

`MediaRef` should represent media without forcing ShardLoom to load raw bytes:

```text
media_set_ref / dataset path / object URI
media_item_id / path
MIME type
size
checksum if available
source system
branch / transaction / version
access policy
extraction status
```

`MediaManifest` should represent a collection of media refs:

```text
manifest_id
media_refs
schema
source system
virtual/external status
update detection policy
known limitations
redaction policy
fallback_attempted=false
```

Virtual or external media handles remain governed references until staged or
natively accessed through certified source paths.

## Extraction and model-call boundaries

Media extraction, OCR, transcription, metadata parsing, chunking, embeddings,
LLM calls, and model inference must be explicit effect boundaries.

`ExtractionBoundaryReport` fields:

```text
boundary_type
input_kind
operation
executor
shardloom_native_execution=false
output_kind
deterministic
materialization_boundary
provenance_ref
confidence_policy
redaction_policy
fallback_attempted=false
```

`ModelCallBoundaryReport` fields:

```text
boundary_type
model_kind
provider
model_ref
model_version
input_artifact_ref
output_artifact_ref
prompt_template_hash
temperature/settings
token_budget
cost_accounting
deterministic=false
external_effect=true
human_review_policy
output_validation_schema
shardloom_native_execution=false
fallback_attempted=false
```

`EmbeddingBoundaryReport` is a model-call boundary specialized for vector
outputs and should record embedding model, dimension, normalization, input
hash, provider boundary, redaction, cost, and reproducibility status.

These reports allow ShardLoom to certify the data workflow without claiming it
performed the model/media work.

## Chunk and extraction model

For unstructured data, ShardLoom should eventually support:

- Chunk manifests.
- Chunk ids.
- Source object references.
- Byte ranges or offsets.
- Text spans.
- Extracted fields.
- Provenance.
- Embedding references.
- Model-call provenance.
- Output schema.

This enables retrieval and LLM workflows while preserving auditability.

`TextChunkTable` should be treated as a structured table contract:

```text
document_id
chunk_id
text
start_offset / end_offset
page_number
section
extraction_method
extraction_version
confidence
provenance_ref
redaction_status
```

ShardLoom can validate, filter, join, aggregate, write, certify, and benchmark
chunk tables as structured data once the table source path itself is certified.

## Embeddings

ShardLoom should support embeddings as modular typed data and effectful generation steps.

Embedding concepts may include:

- EmbeddingVector.
- EmbeddingModelRef.
- EmbeddingColumn.
- EmbeddingGenerationPlan.
- EmbeddingRef.
- VectorIndexRef.
- SimilaritySearchPlan.

Embedding generation is a ModelCall effect.

Similarity search is an ExternalRead or native vector operation depending on implementation.

Embedding operations should declare:

- Model.
- Dimensionality.
- Input source.
- Output type.
- Distance metric.
- Index type.
- Cost.
- Caching.
- Reproducibility expectations.

Embedding data may eventually be represented in Vortex-compatible forms when appropriate.

`EmbeddingTable` should be treated as structured model output:

```text
entity_id / document_id / chunk_id
embedding_model
model_version
vector
dimension
normalization
created_at
input_hash
redaction_policy
provider_boundary
```

Embedding generation remains an explicit model-call boundary. Vector similarity
scan, ANN/top-K, vector indexes, and native vector execution are separate
capability claims that require their own correctness, benchmark, Native I/O,
execution-certificate, and no-fallback evidence.

## Unstructured workflow certificate

`UnstructuredWorkflowCertificate` should summarize an end-to-end unstructured
workflow:

```text
input media refs
extraction boundaries
chunking boundaries
embedding/model boundaries
structured outputs
validation checks
redaction policy
cost/effect policy
lineage refs
downstream ShardLoom analytics refs
fallback_attempted=false
```

Maturity levels:

```text
U0 declared only
U1 media reference discovery
U2 extraction boundary recorded
U3 chunk table emitted
U4 embedding/model boundary recorded
U5 structured outputs validated
U6 downstream ShardLoom analytics certified
U7 Foundry workflow certified
```

## Vector search

Vector search should be modular.

Potential modes:

- Native vector scan.
- Native vector index.
- External vector index connector.
- Hybrid structured + vector filtering.

Vector search must be explicit in plans.

External vector search must be modeled as ExternalRead.

ShardLoom should make it easy to combine:

- Structured filters.
- Segment pruning.
- Metadata filters.
- Vector retrieval.
- LLM processing.
- Vortex output.

## Agent-facing capability discovery

LLM agents should be able to discover extension capabilities.

ShardLoom should eventually expose machine-readable capabilities such as:

```json
{
  "sql": {
    "supported": true,
    "window_functions": false,
    "joins": "partial"
  },
  "udfs": {
    "scalar": true,
    "aggregate": "planned",
    "python": "planned",
    "wasm": "planned"
  },
  "effects": {
    "llm_calls": "planned",
    "api_calls": "planned",
    "external_writes": "requires_explicit_enablement"
  },
  "embeddings": {
    "generation": "planned",
    "vector_search": "planned"
  },
  "fallback_execution": false
}
```

Capability discovery should distinguish:

- Supported.
- Planned.
- Unsupported.
- Requires explicit enablement.
- Requires dependency.
- Requires external credentials.
- Requires materialization.
- Requires effect permission.

## Plan boundaries

ShardLoom plans should eventually make boundaries explicit:

- Native encoded execution.
- Partial decode.
- Full materialization.
- Format translation.
- External read.
- External write.
- Model call.
- Vector search.
- Unsupported.

These boundaries should appear in explain output.

## Safety controls

Effectful operations require safety controls.

Potential controls:

- Dry run.
- Explicit enablement.
- Cost budget.
- Token budget.
- Timeout.
- Retry limit.
- Rate limit.
- Redaction policy.
- Idempotency key.
- Human approval flag.
- Credential scope.
- Output validation.
- Audit log.

ShardLoom should not surprise users with external calls.

## Configuration

ShardLoom may eventually support modular config.

Potential `shardloom.toml` sections:

```toml
[sql]
dialect = "shardloom"

[effects]
llm_calls = "disabled"
api_calls = "disabled"
external_writes = "disabled"

[budgets]
max_llm_cost_usd = 0.00
max_api_calls = 0

[embeddings]
default_model = "disabled"

[outputs]
default_format = "vortex"
```

Defaults should be safe.

## Familiar UX examples

### SQL-like

```sql
SELECT customer_id, COUNT(*)
FROM events
WHERE event_date >= DATE '2026-01-01'
GROUP BY customer_id
```

### DataFrame-like

```python
dataset = sl.dataset("events.vortex")

result = (
    dataset
    .filter("event_date >= '2026-01-01'")
    .group_by("customer_id")
    .count()
    .write_vortex("out/customer_counts.vortex")
)
```

### Agent-friendly dry run

```bash
shardloom explain query.sql --format json
shardloom estimate query.sql --format json
shardloom capabilities --format json
```

### Explicit LLM step

```python
workflow = (
    sl.dataset("support_tickets.vortex")
    .filter("created_at >= '2026-01-01'")
    .llm_extract(
        input="ticket_text",
        schema={"issue_type": "string", "severity": "string"},
        model="approved-support-extractor",
        budget_usd=5.00,
    )
    .write_vortex("out/ticket_extracts.vortex")
)
```

This example is conceptual. It should not imply implementation exists yet.

## Failure behavior

Unsupported extension behavior must fail explicitly.

Examples:

- Unsupported SQL construct.
- Unknown UDF.
- UDF requires materialization but materialization is disabled.
- LLM calls disabled.
- API calls disabled.
- External write requires explicit approval.
- Embedding model not configured.
- Vector index unavailable.
- Unstructured chunking unsupported.
- Unsupported output schema.
- Cost budget exceeded.
- Missing credentials.

Failures must not trigger fallback execution.

## Alternatives considered

### Ignore adjacent workflows

Rejected.

Users and LLM agents will naturally need SQL, UDFs, unstructured data, API calls, embeddings, and
LLM workflows around data execution.

### Build everything immediately

Rejected.

ShardLoom should design extension points first and implement incrementally.

### Hide external calls inside UDFs

Rejected.

External effects must be explicit for safety, cost control, reproducibility, and explainability.

### Use Spark or DataFusion for SQL support

Rejected.

SQL may be a frontend, but execution must remain ShardLoom-native.

### Make ShardLoom an LLM framework

Rejected.

ShardLoom should support LLM/model calls as modular effects, not become a general LLM orchestration
system.

## Risks

- Too much flexibility could complicate the core engine.
- Effectful operations could harm reproducibility if not modeled explicitly.
- SQL expectations may exceed native support early.
- UDFs may force materialization.
- Python or external UDFs may introduce performance and safety issues.
- LLM/API calls introduce cost, latency, auth, and reliability concerns.
- Embedding and vector search support may drift into a separate product area.
- Agent-facing APIs need stable schemas.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- SQL should be a frontend into ShardLoom plans, not external fallback execution.
- UDFs should be modular, typed, and explicit about determinism and materialization.
- Unstructured data should be represented through typed references, chunks, extracted fields, and
  manifests.
- LLM calls should be explicit effectful model calls.
- API calls should be explicit external effects.
- Embeddings should be modular and typed.
- Vector search should be explicit and capability-discovered.
- Agent-facing capability discovery is required.
- Hidden side effects are prohibited.
- No Spark or DataFusion fallback is allowed.

## Verification plan

Future implementation PRs should verify:

- Capabilities can be represented.
- Unsupported SQL can fail clearly.
- Function metadata can describe purity and materialization requirements.
- Effect metadata can describe cost, timeout, idempotency, and safety.
- LLM/API calls are not executed during explain or dry run.
- External effects require explicit enablement.
- Embedding/vector features can be marked supported, planned, or disabled.
- Native Vortex output remains available.
- No fallback execution is introduced.

## Open questions

- What SQL frontend should be implemented first?
- Should ShardLoom define its own SQL subset before adding a parser?
- What UDF runtime should come first: Rust, WASM, Python, or SQL-defined?
- Should effectful operations live in the core workspace or a separate crate?
- What diagnostic schema should capability discovery use?
- Should embeddings be stored directly in Vortex-native outputs?
- What is the first unstructured-data use case?
- What safety controls are required before enabling external writes?

## SQL frontend boundary vocabulary (R5.1)

Conceptual SQL frontend boundary:
- SQL text
- parse
- bind names/types/catalog refs
- validate ShardLoom-supported subset
- produce ShardLoom LogicalPlan
- capability check
- explain / estimate / execute later

Non-goals in this pass:
- no SQL execution delegation
- no Calcite dependency
- no DataFusion dependency
- no broad SQL dialect
- no parser dependency until explicitly approved

