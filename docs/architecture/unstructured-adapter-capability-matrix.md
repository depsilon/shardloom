# Unstructured Media And Universal Adapter Capability Matrix

`GAR-0032-D` records ShardLoom's current unstructured/media, vector, and universal-adapter posture.
The matrix is intentionally report-only: it gives users a stable way to inspect what is visible,
blocked, or metadata-only without implying runtime support.

## User Surfaces

- `capabilities unstructured-media --format json`
- `capabilities universal-adapters --format json`
- `capabilities event-api-saas-adapters --format json`
- `capabilities api-surfaces --format json`
- Python capability views that consume the CLI JSON field map

## Contract Fields

The matrix uses schema `shardloom.unstructured_adapter_capability_matrix.v1` and matrix id
`gar-0032-d.unstructured_media_universal_adapter_matrix`.

The summary keeps these fields explicit:

- `unstructured_adapter_capability_claim_gate_status=not_claim_grade`
- `unstructured_adapter_capability_runtime_execution=false`
- `unstructured_adapter_capability_source_io_performed=false`
- `unstructured_adapter_capability_sink_io_performed=false`
- `unstructured_adapter_capability_model_call_performed=false`
- `unstructured_adapter_capability_network_probe_performed=false`
- `unstructured_adapter_capability_fallback_attempted=false`
- `unstructured_adapter_capability_external_engine_invoked=false`

Each row exposes:

- `family`
- `surface`
- `support_status=report-only|blocked`
- `runtime_execution=false`
- `source_io_performed=false`
- `sink_io_performed=false`
- `metadata_only`
- `credential_required`
- `network_required`
- `model_call_required`
- `external_effect_blocker_id`
- `blocker_id`
- `required_evidence`
- `claim_boundary`

## Covered Families

- document references and metadata
- text extraction and chunking
- image/audio/video references and media decode boundaries
- embedding generation and vector values
- vector search
- Vortex TurboQuant vector encoding
- additional local file adapters
- database and warehouse adapters
- object-store and table/lakehouse adapters
- event/API/SaaS adapters
- source/sink metadata references

## Claim Boundary

This matrix is a diagnostic/report-only capability surface. It adds no document parser, text
extraction runtime, OCR, transcription, image/audio/video decode, embedding model, vector index,
vector search runtime, TurboQuant vector encoding/decoding runtime, database/warehouse driver,
object-store runtime, table/lakehouse runtime, event listener, API client, SaaS connector,
credential resolution, network probe, source read, sink write, model call, external engine
invocation, or fallback execution. In short, the slice preserves no fallback execution.

The external-effect blocker matrix remains the effect-policy authority for model calls, API calls,
media extraction, plugin execution, and network egress. A future row may only be promoted when it
attaches source/sink evidence, effect-budget evidence, redaction/audit policy, correctness,
execution-certificate, Native I/O where applicable, and no-fallback evidence.
