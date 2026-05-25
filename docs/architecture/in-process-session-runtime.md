# In-Process ShardLoom Session Runtime

## Purpose

This document records the scoped runtime slice for `GAR-PERF-2F` and the follow-through completed
under `GAR-RUNTIME-IMPL-4L/5I`. It defines the path from scoped prepared/native batch execution and
report-only `ShardLoomSessionModelReport` vocabulary to explicit in-process `ShardLoomSession`
runtime and CLI-visible cache lifecycle evidence.

The target is not a daemon, service, scheduler, or remote server. The target is a caller-owned,
explicitly closed local session that can reuse prepared/native local artifacts across multiple
scenario executions without hidden global state or external-engine fallback.

## Current State

ShardLoom now has three relevant pieces:

- `ShardLoomSessionModelReport` in `shardloom-core/src/session.rs`, which records explicit
  session/registry posture and keeps runtime execution disabled.
- `traditional-analytics-vortex-batch-run`, which now opens a scoped in-process prepared/native
  local-artifact session, runs requested traditional analytics scenarios through that session, emits
  session/cache/lifecycle evidence, and closes the session before returning the typed envelope.
- `session-cache-smoke`, which exercises a CLI-visible scoped session cache lifecycle for
  `SourceState`, `VortexPreparedState`, `OutputPlan`, schema cache, dictionary cache, explicit
  fingerprint invalidation, scratch-buffer reuse accounting, optimizer-trace linkage, close, and
  cleanup.

The runtime support remains scoped and caller-owned. Python exposes the public ergonomic
`ShardLoomSession` for admitted local `vortex_ingest`, read/SQL collect, write, and fanout reuse;
the CLI smoke proves the same cache lifecycle contract without creating a daemon, hidden global
cache, persistent cross-process cache, object-store/table cache, or performance claim.

## Scoped `ShardLoomSession`

The scoped prepared/native session owns explicit local state:

```text
session_id
prepared_artifact_registry
source_metadata_cache
source_state_cache
schema_cache
dictionary_cache
buffer_pool
kernel_registry
evidence_recorder
```

The supported local slices externalize prepared-artifact, source-metadata, source-state,
OutputPlan, schema-cache, dictionary-cache, and scratch-buffer reuse evidence. All state is
caller-owned and explicitly closed. Session state must not become a hidden process global.

## Evidence Fields

Every session-backed run should expose:

```text
session_id
session_state_scope
session_runtime_status
session_open_status
session_close_status
session_drop_status
session_hidden_global_cache=false
session_daemon_or_service=false
prepared_artifact_cache_hit
prepared_artifact_cache_miss
prepared_artifact_reuse_count
source_metadata_cache_hit
source_metadata_cache_miss
source_state_cache_hit
source_state_cache_miss
source_state_reuse_count
schema_cache_hit
dictionary_cache_hit
buffer_pool_reuse_count
invalidation_reason_order
optimizer_trace_id
optimizer_rule_common_subplan_source_state_reuse_status
kernel_registry_ref
evidence_recorder_ref
session_fallback_attempted=false
session_external_engine_invoked=false
session_claim_gate_status
```

Unknown or not-applicable fields should be explicit instead of omitted.

## Runtime Rules

- Sessions are in-process and caller-scoped.
- Sessions may reuse local prepared Vortex artifacts, metadata, schemas, dictionaries, buffers,
  source-state, and evidence recorder handles only when their digests and policy refs match.
- Sessions must preserve typed envelopes, execution-mode fields, evidence-level fields, Native I/O
  refs, materialization/decode fields, result-sink evidence where requested, and deterministic
  unsupported diagnostics.
- Sessions must not silently change execution mode.
- Sessions must not invoke external engines.
- Sessions must not become remote daemons or long-lived services.

## Python And CLI Surface

The user-visible surface stays narrow:

- CLI batch command reports session fields for scoped prepared/native local artifacts.
- `session-cache-smoke --format json` reports the CLI-visible session/cache lifecycle contract with
  hit/miss counts, source/prepared/output IDs and digests, schema/dictionary cache reuse,
  invalidation reasons, scratch-buffer reuse count, optimizer trace linkage, explicit close, and
  cleanup.
- Python exposes `ctx.session(...)`, `sl.session(...)`, and
  `ShardLoomClient.session_cache_smoke()` typed accessors for the scoped local surfaces.

No broad DataFrame, SQL, REST, object-store, Foundry, live/hybrid, or package-release claim is
implied by a session API.

## Non-Goals

- No daemon or service runtime.
- No remote server claim.
- No REST listener.
- No hidden global cache.
- No object-store/lakehouse runtime.
- No persistent cross-process cache.
- No SQL/DataFrame runtime expansion.
- No performance or superiority claim.
- No Spark/DataFusion/DuckDB/Polars fallback.

## Acceptance

- The design distinguishes report-only `ShardLoomSessionModelReport`, scoped batch runner evidence,
  scoped CLI session-cache lifecycle evidence, and public Python `ShardLoomSession` API support.
- Session-backed rows expose `session_id`, cache hit/miss counts, source-state reuse counts,
  prepared-artifact reuse counts, OutputPlan reuse counts, schema/dictionary cache reuse status,
  buffer reuse counts, close/drop status, and no-fallback/no-external-engine evidence.
- Multiple scoped prepared/native scenarios execute without respawning the CLI or re-opening /
  re-preparing unnecessary session state inside the batch command.
- Session state is explicitly closed, scoped, and never hidden global state.

## Verification Plan

Planning-only updates should run:

```text
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python scripts/check_website_readiness.py
git diff --check
```

Implementation slices should add and maintain:

```text
batch smoke
session-cache-smoke snapshot
Python client smoke for surfaced session/cache views
benchmark row contract tests for session fields
session close/drop lifecycle tests
```
