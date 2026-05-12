# Diagnostics and Capabilities Skill

## Purpose

Use this skill when designing or implementing diagnostics, errors, explain output, estimate output,
doctor checks, capability discovery, or machine-readable agent-facing output.

ShardLoom should be easy for humans to understand and easy for LLM agents to integrate without
guessing.

## When to use

Use this skill for tasks involving:

- Error types.
- Diagnostic records.
- Diagnostic codes.
- Unsupported-feature reporting.
- Explain output.
- Estimate output.
- Doctor commands.
- Capability discovery.
- CLI machine-readable output.
- JSON output.
- Agent-facing APIs.
- Translation reports.
- Effectful operation reporting.

## Rules

- Diagnostics must be deterministic.
- Diagnostics must be specific and actionable.
- Unsupported behavior must fail explicitly.
- Fallback status must be explicit.
- Fallback execution must not occur.
- Diagnostic codes should be stable enough for agents to consume.
- Human-readable messages should be clear.
- Machine-readable fields should be structured.
- Explain output should expose execution boundaries.
- Estimate output should describe uncertainty.
- Doctor checks should be safe and avoid side effects.
- Capability discovery should distinguish supported, partially supported, planned, disabled,
  requires configuration, requires explicit enablement, and unsupported.
- Effectful operations must not run during explain, estimate, doctor, or capabilities.
- Vortex-native input/output status should be visible where relevant.
- Metadata loss should be explicit in translation diagnostics.

## Required checks

For diagnostics:

- Is there a stable code?
- Is there a severity?
- Is there a category?
- Is the message clear?
- Is the reason clear?
- Is the next step actionable?
- Is fallback status explicit?
- Is the diagnostic machine-readable?

For explain output:

- Are plan boundaries visible?
- Are metadata-only decisions visible?
- Are pruning decisions visible?
- Are encoded operations visible?
- Are materialization boundaries visible?
- Are translation boundaries visible?
- Are unsupported features visible?
- Are effectful operations visible but not executed?

For estimate output:

- Are estimates labeled as estimates?
- Is uncertainty represented?
- Are missing statistics represented?
- Are object-store and materialization costs considered where relevant?
- Are model/API costs represented where relevant?

For capabilities:

- Are features marked with clear statuses?
- Are planned features not represented as supported?
- Is fallback execution clearly disabled?
- Is native Vortex input/output visible?
- Are external effects marked disabled or requiring explicit enablement by default?

## Red flags

- Returning only a vague string error.
- Saying "unsupported" without explaining which feature.
- Hiding fallback status.
- Suggesting Spark or DataFusion as internal fallback.
- Running LLM/API calls during explain or estimate.
- Claiming a planned capability is supported.
- Emitting JSON with unstable or undocumented fields.
- Making text output the only agent-consumable interface.
- Hiding metadata loss during translation.

## Example Codex prompt fragment

"Use the Diagnostics and Capabilities skill. Add deterministic structured diagnostics with stable
codes, clear categories, fallback status, and actionable next steps.
Explain/estimate/doctor/capabilities must be machine-readable where relevant and must not trigger
effectful operations."
