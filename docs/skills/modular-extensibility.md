# Modular Extensibility Skill

## Purpose

Use this skill when designing or implementing SQL support, UDFs, unstructured data support, LLM
calls, API calls, embeddings, vector search, connector interfaces, or agent-facing capability
discovery.

ShardLoom should remain a high-performance Vortex-native execution engine while being flexible and
frictionless for common and adjacent workflows.

## When to use

Use this skill for tasks involving:

- SQL frontend design.
- DataFrame/query builder APIs.
- UDF registration.
- Function metadata.
- Custom scalar functions.
- Custom aggregate functions.
- Table functions.
- Unstructured data.
- Document chunks.
- Extraction.
- Embeddings.
- Vector search.
- LLM calls.
- API calls.
- External effects.
- Capability discovery.
- Agent workflows.

## Rules

- Keep deterministic encoded execution pure where possible.
- Model external, non-deterministic, and side-effecting operations explicitly.
- Do not hide external calls inside ordinary filters or projections.
- Do not execute LLM calls, API calls, or external writes during explain, estimate, or dry run.
- SQL is a frontend into ShardLoom planning, not a fallback engine.
- UDFs must declare types, null behavior, determinism, effect level, encoded capability, and
  materialization requirements.
- Unstructured data should use typed references, chunks, extracted fields, and manifests.
- LLM calls should be explicit ModelCall effects.
- API calls should be explicit ExternalRead or ExternalWrite effects.
- Embeddings should be typed and should declare model, dimensionality, metric, and generation
  behavior.
- Vector search should be explicit and capability-discovered.
- Agent-facing capability discovery should expose supported, planned, disabled, and unsupported
  features.
- Do not add Spark or DataFusion fallback for convenience.
- Do not add dependencies without license and architecture review.

## Required checks

For SQL work:

- Is SQL acting only as a frontend?
- Does unsupported SQL fail clearly?
- Is execution still ShardLoom-native?
- Are supported constructs documented?
- Are unsupported constructs diagnosed?

For UDF work:

- Are input and output types declared?
- Is null behavior declared?
- Is determinism declared?
- Is effect level declared?
- Is encoded execution capability declared?
- Is materialization requirement declared?
- Are unsupported cases deterministic?

For unstructured data work:

- Are payload references separate from extracted structured fields?
- Is chunk provenance tracked?
- Are embeddings and extracted fields traceable?
- Are model/API effects explicit?
- Are unsupported media or extraction cases diagnosed?

For LLM/API effect work:

- Is the operation explicit in the plan?
- Is dry run safe?
- Are cost and timeout modeled?
- Are retries and idempotency modeled?
- Are credentials outside ordinary plan data?
- Are external writes explicitly enabled?
- Are diagnostics machine-readable?

For embeddings/vector work:

- Is dimensionality declared?
- Is model identity declared?
- Is metric declared?
- Is generation separate from search?
- Is vector search native or external?
- Is capability discovery updated?
- Is output storage behavior clear?

## Red flags

- Adding DataFusion to get SQL support.
- Adding Spark to handle scale or unsupported plans.
- Hiding API calls inside UDFs.
- Running LLM calls during explain.
- Treating embeddings as untyped blobs.
- Treating vector search as invisible magic.
- Letting UDFs force full materialization silently.
- Making external writes possible without explicit enablement.
- Making agent workflows guess capabilities.
- Treating planned features as implemented.

## Example Codex prompt fragment

"Use the Modular Extensibility skill. SQL must be a frontend only. UDFs must declare types, null
behavior, determinism, effect level, encoded capability, and materialization requirements.
LLM/API/embedding operations must be explicit effects with safe dry-run behavior. Do not add Spark,
DataFusion, or fallback execution."
