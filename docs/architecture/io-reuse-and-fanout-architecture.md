# I/O Reuse And Cross-Format Fanout Architecture

Status: partially implemented architecture reference for `GAR-IOREUSE-1`.

## Summary

`GAR-IOREUSE-1` defines the planned architecture and benchmark bundle for reusable input
discovery, source parsing, Vortex preparation, operator source-state, output planning, and
cross-format output fanout.

The core rule is that input type and output type are independent. Reuse must not depend on matching
input and output formats. The stable flow is:

```text
InputAdapter
-> SourceState
-> VortexPreparedState
-> ExecutionPlan
-> OutputPlan
-> SinkArtifact
```

`GAR-IOREUSE-1A` implements the first benchmark/report contract for local SourceState evidence.
`GAR-IOREUSE-1B` adds the companion VortexPreparedState benchmark/report contract for prepared
artifact identity, digest, preparation timing separation, source-state linkage, and scoped reuse
posture. `GAR-IOREUSE-1C` adds the OutputPlan benchmark/report contract for scoped local Vortex
result-sink planning, metadata preservation posture, write/replay refs, and sink artifact identity.
`GAR-IOREUSE-1D` adds the first-class fanout benchmark matrix for required cross-format output
cases as deterministic report-only rows. Fanout runtime, invalidation, reuse-level, and Foundry
generated-output slices are still planned. This document does not implement object-store I/O,
table/lakehouse commits, Foundry production support, performance claims, or a hidden fast mode.

## Goals

- Make source discovery, schema/dtype inference, parse/decode planning, Vortex preparation,
  operator source-state, and output write planning reusable across workloads.
- Keep source-side reuse, prepared Vortex reuse, operator-state reuse, and output-plan reuse
  independently visible.
- Support cross-format output fanout planning, where one prepared source can write multiple local
  sink artifacts without coupling to the input format.
- Keep `compatibility_import_certified`, `prepared_vortex`, `native_vortex`, and
  `direct_compatibility_transient` lanes distinct.
- Add benchmark vocabulary for `io_reuse_and_fanout`, `source_state_reuse`,
  `prepared_state_reuse`, `output_plan_reuse`, `cross_format_output`, and
  `generated_source_output`.

## Non-Goals

- No S3/GCS/ADLS runtime in this bundle.
- No object-store write, network probe, credential resolution, commit protocol, or lakehouse/table
  transaction claim.
- No performance, superiority, or Spark-replacement claim.
- No hidden fast mode or process-global cache.
- No external engine fallback.
- No broad SQL/DataFrame runtime claim.
- No Foundry production claim.

## State Model

### InputAdapter

`InputAdapter` is the format-specific admission and read-planning boundary for local and future
remote inputs. It owns source discovery, path/listing facts, schema/dtype inference, parse/decode
planning, source capability, and unsupported diagnostics.

### SourceState

`SourceState` is reusable source-side state. It may include discovered source metadata, inferred
schema, dtype mapping, parse/decode plan, source digest, source fingerprint, and adapter capability
results. It is not an execution result and it is not tied to an output format.

Implemented `GAR-IOREUSE-1A` benchmark/report fields:

```text
source_state_contract_schema_version
source_state_status_vocabulary
source_state_status
source_state_id
source_state_digest
source_format
source_location
source_fingerprint_kind
schema_digest
row_count_known
file_count
byte_size
partition_columns
compression
source_state_reuse_allowed
source_discovery_millis
schema_inference_millis
source_parse_millis
parse_decode_plan_digest
source_state_reuse_hit
source_state_reuse_reason
source_state_materialization_decode_boundary_ref
source_state_fallback_attempted=false
source_state_external_engine_invoked=false
source_state_claim_gate_status=not_claim_grade
source_state_claim_boundary
```

CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC rows can report a SourceState posture in the
traditional analytics benchmark artifact and Markdown report. `source_state_status` is one of
`source_state_reuse_supported`, `not_needed`, `blocked`, `unsupported`, `report_only`, or
`external_baseline_only`. Existing prepared/native batch source-state families keep their
family-specific fields while also mapping into the universal SourceState contract.

The current SourceState contract is local benchmark evidence only. It records source identity,
schema/fingerprint posture, parse/decode plan identity, reuse eligibility, reuse hit/miss, and
no-fallback policy fields. It does not claim output support, Vortex-native execution, performance,
production SQL/DataFrame runtime, object-store/lakehouse support, Foundry support, package release,
or Spark replacement.

### VortexPreparedState

`VortexPreparedState` is reusable preparation state for a source that has been converted or admitted
into a Vortex-prepared representation. It owns prepared artifact refs/digests, Vortex preparation
timing, Native I/O certificate refs, materialization/decode boundaries, source-backed scan refs, and
reuse counters. It may be created from compatibility import, native Vortex input, generated source,
or a future admitted source.

Implemented `GAR-IOREUSE-1B` benchmark/report fields:

```text
prepared_state_contract_schema_version
prepared_state_status_vocabulary
prepared_state_status
prepared_state_id
prepared_state_digest
prepared_state_source_state_id
vortex_artifact_ref
vortex_artifact_digest
layout_summary
encoding_summary
statistics_summary
prepared_state_reuse_allowed
prepared_state_reuse_hit
prepared_state_reuse_reason
preparation_included_in_timing
vortex_prepare_millis
prepared_state_materialization_decode_boundary_ref
prepared_state_native_io_certificate_status
prepared_state_fallback_attempted=false
prepared_state_external_engine_invoked=false
prepared_state_claim_gate_status=not_claim_grade
prepared_state_claim_boundary
```

The traditional analytics benchmark artifact and Markdown report now include a
`shardloom.traditional_analytics.vortex_prepared_state.v1` contract object and a
VortexPreparedState evidence matrix. Prepared/native rows can report scoped reusable prepared Vortex
artifact posture while compatibility-import-certified rows remain certification rows and
direct-transient rows remain `not_needed`. `prepared_state_status` is one of
`prepared_state_reuse_supported`, `not_needed`, `blocked`, `unsupported`, `report_only`, or
`external_baseline_only`.

Prepared state should be reusable across multiple queries and future output plans. Current rows
separate `vortex_prepare_millis` from query/runtime timing and preserve
`preparation_included_in_timing=false` where preparation is measured outside scenario runtime.
Prepared state reuse does not weaken no-fallback policy and does not authorize output support,
object-store Vortex artifact runtime, encoded-native operator coverage, or performance claims unless
those runtimes are separately admitted.

### ExecutionPlan

`ExecutionPlan` is the workload/operator plan. It consumes SourceState and/or VortexPreparedState
and may own optimizer trace refs, source-state refs, residual-native or encoded-provider admission,
and correctness/evidence requirements.

### OutputPlan

`OutputPlan` is output-side planning that is decoupled from input format. It owns sink target kind,
schema mapping, metadata preservation/degradation report, required materialization, layout/write
strategy, replay policy, and unsupported diagnostics.

Planned output formats:

```text
Vortex
CSV
JSONL
Parquet
Arrow IPC
Avro
ORC
Foundry output dataset, via transform wrapper
S3/object-store, blocked until runtime proof
```

Implemented `GAR-IOREUSE-1C` benchmark/report fields:

```text
output_plan_contract_schema_version
output_plan_status_vocabulary
output_plan_status
output_plan_id
output_plan_digest
output_format
output_location
output_schema_digest
output_partitioning
output_compression
output_encoding
output_write_mode
output_plan_reuse_allowed
output_metadata_preservation_status
output_materialization_required
output_plan_reuse_hit
output_plan_reuse_reason
output_plan_millis
output_write_millis
result_replay_verified
output_native_io_certificate_status
sink_artifact_ref
sink_artifact_digest
output_plan_fallback_attempted=false
output_plan_external_engine_invoked=false
output_plan_claim_gate_status=not_claim_grade
output_plan_claim_boundary
```

The traditional analytics benchmark artifact and Markdown report now include a
`shardloom.traditional_analytics.output_plan.v1` contract object and an OutputPlan evidence matrix.
Local Vortex result-sink rows with write/replay evidence can report `output_plan_supported`.
Rows without an output request report `not_needed`, unsupported rows stay explicit, and external
baselines are `external_baseline_only`.

Output planning is separate from input format. One prepared source may later fan out to multiple
local output formats when sink evidence exists, but cross-format fanout is still a future slice.
Local output, object-store write, and table/lakehouse commit semantics remain separate.

### SinkArtifact

`SinkArtifact` is the emitted local output artifact plus evidence. It owns target URI/ref, artifact
digest, output Native I/O certificate status, replay evidence when requested, write timing, metadata
loss report, and claim gate.

## Benchmark Bundle

The benchmark bundle tracks these scenario families:

```text
io_reuse_and_fanout
source_state_reuse
prepared_state_reuse
output_plan_reuse
cross_format_output
generated_source_output
```

Implemented `GAR-IOREUSE-1D` report-only fanout cases:

```text
CSV input -> Parquet + JSONL + Vortex outputs
Parquet input -> CSV + Vortex outputs
JSONL input -> Parquet + Vortex outputs
generated source -> CSV + Parquet + Vortex outputs
prepared Vortex -> multiple output formats
```

The traditional analytics benchmark artifact and Markdown report now include a
`shardloom.traditional_analytics.io_reuse_and_fanout.v1` contract object and a fanout benchmark
matrix. Current fanout rows expose:

```text
benchmark_family
fanout_case_id
source_format
requested_output_formats
currently_proven_output_formats
blocked_output_formats
fanout_status
fanout_blocker_id
fanout_blocker_reason
source_discovery_millis
schema_inference_millis
source_parse_millis
vortex_prepare_millis
operator_compute_millis
output_plan_millis
output_write_millis
output_replay_millis
total_runtime_millis
source_state_reuse_hit
prepared_state_reuse_hit
output_plan_reuse_hit
fanout_output_count
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
```

Current `GAR-IOREUSE-1A`, `GAR-IOREUSE-1B`, `GAR-IOREUSE-1C`, and `GAR-IOREUSE-1D` benchmark rows
expose the SourceState, VortexPreparedState, OutputPlan, and report-only fanout subsets listed
above. Future runtime fanout/reuse rows should replace report-only blockers with measured values
for:

```text
operator_compute_millis
output_replay_millis
total_runtime_millis
fanout_output_count > 1
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

Rows may also expose source/prepared/output fingerprints, invalidation reasons, sink artifact refs,
and per-output metadata preservation reports when implementation reaches those runtime slices.

The benchmark must demonstrate when source/prepared state is reused across outputs, separate raw
one-shot speed from reuse/fanout timing, and avoid marking any output sink as supported without
replay/evidence proof.

## Evidence Requirements

The bundle requires these evidence groups before any runtime support claim:

- Source discovery refs, schema inference refs, dtype inference refs, and source fingerprint refs.
- Parse/decode planning refs and materialization/decode boundary refs.
- Vortex preparation refs, prepared artifact refs, and prepared-state fingerprints.
- Execution plan refs, optimizer/source-state refs where applicable, correctness digest refs, and
  no-fallback refs.
- Output plan refs, target schema mapping refs, metadata preservation/degradation refs, local write
  refs, output replay refs when requested, and output Native I/O certificate refs.
- Invalidation refs for stale source, stale prepared state, stale output plan, or policy mismatch.

## Reuse Levels

The planned reuse evidence levels are:

```text
discovery_reuse
schema_reuse
parse_plan_reuse
prepared_vortex_reuse
operator_source_state_reuse
output_plan_reuse
result_replay_reuse
```

Reuse rows should classify each reusable layer independently:

```text
source_state_reuse_status
prepared_state_reuse_status
operator_source_state_reuse_status
output_plan_reuse_status
sink_artifact_reuse_status
```

Each level should be one of:

```text
reuse_hit
reuse_miss
not_needed
blocked
unsupported
invalidated
report_only
```

Every row must preserve `fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status`.

Required future reuse fields:

```text
reuse_level
reuse_hit
reuse_digest
reuse_allowed
reuse_blocker
evidence_level
claim_gate_status
```

Reuse never hides execution mode and never upgrades claim status by itself. Reuse evidence should be
visible in both `minimal_runtime` and `certified` modes.

## Cache Invalidation And Fingerprints

Required future invalidation fields:

```text
source_fingerprint_kind
source_content_digest
source_mtime
source_size
object_etag
manifest_version
schema_digest
plan_digest
output_plan_digest
cache_valid
invalidation_reason
```

Reuse is blocked when the source fingerprint, schema digest, plan digest, output plan digest, or
policy changes. Object-store ETag/version handling is planned but not runtime-claimed. Cache keys
and evidence must not contain secrets or credentials.

## Cross-Format Fanout Boundary

Cross-format fanout means one admitted source/prepared state can plan and write more than one local
output target, such as Vortex plus compatibility export formats. It does not mean output formats
share table commit semantics or object-store behavior.

Compatibility outputs are export targets, not execution fallbacks. Vortex remains the highest
fidelity output target. Metadata loss must be reported per output target.

## Foundry Generated-Output Boundary

Foundry no-input/generated-output fanout remains planned/report-only in this bundle. A future
Foundry-style smoke must use Foundry output APIs where applicable, not direct S3 writes, and must
emit generated-source and output evidence without claiming Foundry production support.

Required future Foundry generated-output fields:

```text
input_dataset_count=0
generated_source_created=true
generated_source_certificate_status
output_plan_id
output_native_io_certificate_status
foundry_runtime_invoked=false unless real Foundry runtime proof exists
foundry_spark_invoked=false
fallback_attempted=false
external_engine_invoked=false
```

No-input smoke and generated-output execution remain separate. Foundry output must go through
transform output APIs where applicable, not direct object-store/S3 writes.

## Claim Boundary

The bundle can eventually support claims about scoped local I/O reuse and local cross-format output
fanout only when the relevant evidence exists. It cannot authorize performance, superiority,
Spark-displacement, production, broad SQL/DataFrame, object-store/lakehouse, Foundry production,
package, or release claims.

Until implementation and evidence exist, rows should use:

```text
claim_gate_status=not_claim_grade
support_status=report_only|blocked|unsupported
fallback_attempted=false
external_engine_invoked=false
```

## Acceptance For The Planning Bundle

- The phase plan contains detailed remaining `GAR-IOREUSE-1E` through `GAR-IOREUSE-1G` slices, and
  the completed ledger records `GAR-IOREUSE-1A`, `GAR-IOREUSE-1B`, `GAR-IOREUSE-1C`, and
  `GAR-IOREUSE-1D` as completed SourceState, VortexPreparedState, OutputPlan, and fanout benchmark
  matrix evidence.
- Benchmark docs list the tracked benchmark families and metrics.
- Compute-flow docs show the decoupled path:
  `InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact`.
- The global architecture review mirrors unchecked follow-up items.
- No runtime code, package publication, object-store runtime, table commit, performance claim, or
  fallback engine is introduced by the planning slice.

## Verification Plan

Planning-only validation should include:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
python scripts/check_website_readiness.py
git diff --check
```
