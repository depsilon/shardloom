# In-Process ShardLoom Session Runtime

## Purpose

This document records the scoped runtime slice for `GAR-PERF-2F`. It defines the path from scoped
prepared/native batch execution and report-only `ShardLoomSessionModelReport` vocabulary to an
explicit in-process `ShardLoomSession` runtime.

The target is not a daemon, service, scheduler, or remote server. The target is a caller-owned,
explicitly closed local session that can reuse prepared/native local artifacts across multiple
scenario executions without hidden global state or external-engine fallback.

## Current State

ShardLoom already has two relevant pieces:

- `ShardLoomSessionModelReport` in `shardloom-core/src/session.rs`, which records explicit
  session/registry posture and keeps runtime execution disabled.
- `traditional-analytics-vortex-batch-run`, which now opens a scoped in-process prepared/native
  local-artifact session, runs requested traditional analytics scenarios through that session, emits
  session/cache/lifecycle evidence, and closes the session before returning the typed envelope.

This is not yet a general reusable public session API. Current runtime support remains scoped to the
prepared/native traditional analytics batch command.

## Planned `ShardLoomSession`

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

The first supported slice externalizes prepared-artifact, source-metadata, and source-state reuse
evidence. Schema and dictionary caches are reported as
`not_externalized_digest_policy_pending`; the buffer pool is reported as
`not_enabled_planned_under_GAR-PERF-2G`. All state is caller-owned and explicitly closed. Session
state must not become a hidden process global.

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

The first user-visible surface should stay narrow:

- CLI batch command reports session fields for scoped prepared/native local artifacts.
- Python client may expose a typed session/capability view only after the CLI evidence fields are
  stable.

No broad DataFrame, SQL, REST, object-store, Foundry, live/hybrid, or package-release claim is
implied by a session API.

## Non-Goals

- No daemon or service runtime.
- No remote server claim.
- No REST listener.
- No hidden global cache.
- No object-store/lakehouse runtime.
- No SQL/DataFrame runtime expansion.
- No performance or superiority claim.
- No Spark/DataFusion/DuckDB/Polars fallback.

## Acceptance

- The design distinguishes report-only `ShardLoomSessionModelReport`, scoped batch runner evidence,
  scoped session-backed prepared/native runtime support, and future public `ShardLoomSession` API
  support.
- Session-backed rows expose `session_id`, cache hit/miss counts, source-state reuse counts,
  prepared-artifact reuse counts, close/drop status, and no-fallback/no-external-engine evidence.
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

Future implementation slices should add:

```text
batch smoke
Python client smoke if surfaced
benchmark row contract tests for session fields
session close/drop lifecycle tests
```
