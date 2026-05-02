# Documentation and RFC Skill

## Purpose

Use this skill when writing RFCs, architecture documents, project policy, design notes, README updates, or contributor guidance.

The goal is to keep ShardLoom's architecture explicit so future implementation does not drift.

## When to use

Use this skill for tasks involving:

- RFCs.
- Architecture docs.
- README changes.
- CONTRIBUTING changes.
- AGENTS.md changes.
- Skills changes.
- Project policy.
- Public design rationale.
- Non-goals.
- Acceptance criteria.

## Rules

- Document decisions before large implementation work.
- Keep architecture language precise.
- Distinguish goals from non-goals.
- Distinguish native execution from compatibility export.
- Distinguish unsupported diagnostics from fallback execution.
- State tradeoffs honestly.
- Include acceptance criteria for future implementation.
- Include risks and alternatives.
- Avoid marketing language without supporting evidence.
- Do not claim performance benefits without benchmark references.
- Keep docs aligned with ShardLoom's standalone, Vortex-native identity.

## Required RFC structure

A substantial RFC should include:

- Title.
- Status.
- Summary.
- Context.
- Goals.
- Non-goals.
- Decision.
- Detailed design.
- Alternatives considered.
- Risks.
- Compatibility impact.
- Acceptance criteria.
- Verification plan.
- Open questions.

Shorter docs may use a lighter structure, but should still be clear about decision, scope, and verification.

## Red flags

- Vague architecture words without concrete implications.
- Claiming Spark displacement without defining the workload.
- Claiming performance without benchmark evidence.
- Omitting non-goals.
- Omitting failure behavior.
- Blurring translation/export with fallback execution.
- Allowing docs to contradict AGENTS.md or RFCs.

## Example Codex prompt fragment

When writing architecture docs, include this instruction:

"Use the Documentation and RFC skill. Include decision, goals, non-goals, alternatives, risks, acceptance criteria, and verification plan. Preserve ShardLoom's standalone Vortex-native no-fallback architecture."
