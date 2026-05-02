# RFC 0019: Security, Secrets, Governance, and Agent Safety

## Status

Draft

## Summary

This RFC defines ShardLoom's security, secrets, governance, and agent safety design.

ShardLoom is expected to support object-store access, UDFs, API calls, LLM calls, embeddings, vector search, unstructured data, agents, and compatibility exports. These features introduce security and governance risks.

ShardLoom must model credentials, permissions, external effects, redaction, sandboxing, dry runs, auditability, and agent safety explicitly.

## Context

ShardLoom's core engine is Vortex-native encoded execution, but real usage will involve sensitive systems:

- Object stores.
- Local files.
- External APIs.
- LLM providers.
- Embedding providers.
- Vector indexes.
- UDF runtimes.
- Internal datasets.
- Unstructured documents.
- PII or confidential fields.
- Agent-driven repository integration.
- External write operations.

Without explicit security and governance design, flexible extension points become dangerous.

## Goals

- Define secret and credential handling principles.
- Define permission and capability boundaries.
- Define governance for external effects.
- Define safety requirements for LLM/coding agents.
- Define redaction and PII handling concepts.
- Define audit and policy concepts.
- Define UDF and plugin sandboxing concerns.
- Define dry-run and approval boundaries.
- Preserve no-fallback execution.

## Non-goals

- Do not implement security mechanisms in this RFC.
- Do not add dependencies in this RFC.
- Do not define a full RBAC/ABAC system.
- Do not define a full secrets manager.
- Do not implement UDF sandboxing.
- Do not implement LLM/API calls.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not guarantee compliance with any regulatory framework in this RFC.

## Core principle

ShardLoom should make side effects, credentials, and permissions explicit.

No operation should surprise the user by:

- Reading unauthorized data.
- Writing external state.
- Calling an LLM.
- Calling an API.
- Generating embeddings.
- Mutating a vector index.
- Leaking secrets.
- Logging sensitive values.
- Using fallback engines.

## Secret handling

ShardLoom should never store raw secrets in plans, diagnostics, logs, traces, or reports.

Rules:

- Secrets must be referenced via handles, aliases, environment indirection, or secure runtime providers.
- Machine-readable outputs should redact credential-bearing fields.
- Explain/estimate/doctor/capabilities outputs must not trigger secret resolution side effects.
- Error paths must never echo bearer tokens, key material, signed URLs, passwords, or private keys.

## Credential scope and lifecycle

Credentials should be scoped to least privilege and shortest practical lifetime:

- Read-only credentials for read-only operations.
- Write credentials only for explicit write/commit nodes.
- Effect credentials isolated by effect type (API, LLM, embedding, vector index).
- Optional per-job credential namespaces for multi-tenant runs.
- Expiration and refresh behavior must be explicit in runtime policy.

## Permissions and capability boundaries

Operations should require explicit capability gates:

- External read.
- External write.
- API call.
- LLM call.
- Embedding generation.
- Vector index mutation.
- Side-effecting UDF.

Default policy should deny effectful capabilities unless explicitly enabled.

## Dry-run, approval, and execution modes

ShardLoom should distinguish planning from effectful execution:

- Explain/estimate/doctor/capabilities: non-effectful only.
- Dry-run: validates permissions/capabilities without performing external writes or calls.
- Apply/execute: effectful operations allowed only when approval gates pass.

Approval policy should be explicit, auditable, and machine-readable.

## Auditability and provenance

Sensitive operations should emit structured audit events that include:

- Request id / job id.
- Principal identity.
- Capability used.
- Target system classification.
- Time and outcome.
- Diagnostic codes for denial/failure.

Audit logs should avoid raw payloads and secrets.

## Agent safety requirements

Agent-driven planning and automation should obey deterministic safety contracts:

- Capability discovery must be machine-readable.
- Disallowed operations must fail with stable diagnostic codes.
- Agents must not infer implicit permission from imported plans.
- Prompt/tooling flows must keep effect intent explicit.

## UDF and plugin safety

UDF/plugin contracts should declare:

- Determinism.
- Null behavior.
- Effect level.
- Network/file/process access requirements.
- Resource limits.
- Provenance/license metadata.

Unsupported sandbox requirements must fail explicitly before execution.

## Failure behavior

Unsupported or unauthorized behavior must fail explicitly with deterministic diagnostics and `fallback_attempted=false`. No Spark, DataFusion, DuckDB, Polars, Velox, or other fallback engines may be used.
