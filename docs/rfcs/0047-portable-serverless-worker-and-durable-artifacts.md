<!-- SPDX-License-Identifier: Apache-2.0 -->

# RFC 0047: Portable serverless native worker and durable artifacts

## Status

Proposed and locally verified for Deliveries 1 and 2 (November 21, 2026) under the
[portable serverless execution queue](../architecture/native-serverless-execution-2026-11-08.md) and
the [phased execution plan](../architecture/phased-execution-plan.md#current-execution). Covers the
headless Rust worker entrypoint, deterministic job/manifest/evidence schemas, durable prepared
source validation, isolated attempt cleanup, atomic host commit fencing, and retained-owner
protection. Deliveries 3–5 (real managed host deployment, live-host cold/warm routing and billed
resource measurements, and remote selective range access) remain explicitly deferred pending actual
host access; no HTTP listener, fallback engine, or competitive gate closes here.

## Summary and context

The November 8 [portable serverless queue addition](../architecture/native-serverless-execution-2026-11-08.md)
structures serverless execution around five ordered deliveries so compute instances can stay
short-lived while validated native data, manifests, and committed outputs survive worker loss.
Following the non-retention of RFC 0046 (`Resonance`) and local retention of RFC 0045 (`Chrysalis`),
Deliveries 1 and 2 freeze the smallest missing local wire, identity, publication, and fencing
contracts over existing resident native owners:

1. **Delivery 1 — Native worker (`portable_worker`)**: Headless Rust execution boundary
   (`PortableWorker::execute`) taking a bounded `PortableWorkerJob` (`shardloom.portable_worker_job.v1`),
   enforcing declared memory, parallelism, float policy, recursion limit, optional spill quota, and
   explicit deadline cancellation, executing via shared `native::prepare_sql_with_spill` +
   `PreparedNativeOperation::write_fields`, and returning complete outputs (`inline_payload_bytes`
   below the caller cutoff or a durable committed manifest handle) with stage-separated monotonic
   timings and `fallback_attempted=false` / `external_engine_invoked=false`.
2. **Delivery 2 — Durable preparation & atomic publication (`portable_worker_store`)**: Content- and
   policy-addressed `DurableArtifactStore` maintaining isolated per-attempt scratch workspaces
   (`attempts/<request_id>/<attempt_id>/`), self-validating prepared Vortex artifacts
   (`prepared/<artifact_id>/` matching `shardloom.durable_prepared_source.v1`), atomic first-writer
   host commit manifests (`committed/<request_id>/` matching `shardloom.durable_result_manifest.v1`),
   and retained-owner `.lease` protection preventing reclamation while downstream readers still hold
   an owner handle.

## Vortex-first provider review

Classification: **`use_vortex_native_provider`** for all native file reads, columnar normalization,
SQL relational execution, spillable intermediate buffers, and `.vortex` / compatibility artifact
publication via pinned Vortex **0.93.0** and Arrow/Parquet **59.3.2**; **`wrap_vortex_concept`** for
job admission, content/policy identity hashing, atomic `create_new(true)` commit fencing, attempt
workspace guards, and retained-owner lease files.

| Existing owner / provider | Role in RFC 0047 | Explicit boundary added |
| --- | --- | --- |
| `ResidentVortexSession` & `LiveMemoryPool` | Enforces memory cap, CPU lanes, `FloatPolicy`, `RecursionLimit`, and session optional-memory reclamation. | Fresh per-worker session dropped before completion; worker asserts zero residual leaked reservations. |
| `native::prepare_sql_with_spill` & `PreparedNativeOperation::write_fields` | Shared SQL binder and native operator/writer pipeline across Vortex, Parquet, Arrow IPC, JSON, JSONL, and CSV. | Job validation, secret rejection, live source SHA-256 verification before and after execution, and deadline cancellation token. |
| Local `DurableArtifactStore` filesystem boundary | Persists `.vortex` prepared sources and committed output files outside worker process memory. | Strict `[A-Za-z0-9._-]` ID validation (no path traversal), atomic temp+link/rename, `create_new(true)` manifest fencing, and owner `.lease` pinning. |

No new external crate dependency, external query engine fallback (`Spark`, `DataFusion`, `DuckDB`,
`Polars`, `Velox`), Flight fragment transport, or network listener (`shardloom-core/src/remote_api.rs`
unchanged) is introduced.

## Contract schemas and invariants

1. **`shardloom.portable_worker_job.v1` (`PortableWorkerJob`)**:
   - Binds safe identifiers `request_id`, `attempt_id`, `authorization_scope_id` (no secrets;
     secret-like markers in identifiers or scope names fail deterministically).
   - Declares input bindings (`reference`, local regular-file `path`, normalized `format`, optional
     text `schema`, optional `expected_sha256` and `expected_size_bytes`), explicit `ExecutionResources`
     + optional `ExecutionResourceLimits`, `deadline_ms`, `spill_buffer_bytes`, routing policy
     (`DirectOnly`, `ReuseIfCompatible`, `PrepareAndReuse`, `RequirePrepared`), output format, and
     `inline_max_bytes`.
   - Sets `allow_cross_request_result_reuse: false` (`true` fails deterministically in v1 per
     [v1-source-prepared-state-scope.md](../architecture/v1-source-prepared-state-scope.md)).
2. **`shardloom.durable_prepared_source.v1` (`DurablePreparedSourceManifest`)**:
   - Deterministic `artifact_id` = SHA-256 of canonical JSON binding `source_sha256`,
     `source_size_bytes`, normalized `source_format`, `float_policy`, `recursion_limit`,
     `runtime_version`, `provider_vortex_version` (`0.93.0`), `provider_arrow_version` (`59.3.2`),
     and `authorization_scope_id`.
   - Reopening validates every identity field, file existence, exact byte length, and exact file
     SHA-256; stale, corrupted, incompatible-version, or wrong-scope artifacts fail deterministically.
3. **`shardloom.durable_result_manifest.v1` (`DurableResultManifest`)**:
   - Atomic `create_new(true)` publication under `committed/<request_id>/manifest.json`.
   - Competing or duplicate attempts write staged artifacts under isolated attempt directories and
     publish immutable attempt files before attempting manifest creation; losing attempts observe
     `CommitOutcome::FencedAlreadyCommitted` and remove only their own loser artifact without
     mutating the winning manifest.
4. **Retained-owner protection (`DurableArtifactLease`)**:
   - `reclaim_prepared` and `reclaim_committed` refuse deletion (`ProtectedByRetainedOwners`) while
     any owner `.lease` remains active, allowing reclamation once all retained leases drop.
5. **`shardloom.portable_worker_evidence.v1` (`PortableWorkerReport`)**:
   - Emits distinct non-overlapping monotonic nanosecond stage fields (`worker_startup_nanos`,
     `input_validation_nanos`, `preparation_or_artifact_open_nanos`, `binding_planning_nanos`,
     `engine_execution_nanos`, `output_publication_nanos`) alongside enclosing
     `complete_request_nanos`, plus `fallback_attempted: false`, `external_engine_invoked: false`,
     and `claim_gate_status: "not_claim_grade"`.

## Deferred scope (Deliveries 3–5)

- **Delivery 3**: Selecting and deploying to one real managed serverless host (`Foundry` batch
  transforms remain package/transform-first; Compute Modules remain optional without preference).
- **Delivery 4**: Cold/warm/scale-to-zero routing policy comparisons and host billing attribution on
  a live deployment.
- **Delivery 5**: Live remote selective range reader and partition/column preparation over object
  storage or Foundry datasets beyond host-staged local files.
