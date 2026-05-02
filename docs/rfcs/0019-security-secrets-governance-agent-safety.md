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
