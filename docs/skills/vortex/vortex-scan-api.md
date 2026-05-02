# Vortex Scan API Skill

## Purpose

Use this skill when reasoning about Vortex scan/source/sink concepts, projection pushdown, filter pushdown, split planning, or query-engine integration boundaries.

The goal is to learn from the Vortex Scan API model without over-coupling ShardLoom to unstable or incomplete API details.

## When to use

Use this skill for tasks involving:

- Scan requests.
- Projection pushdown.
- Filter pushdown.
- Source traits.
- Sink traits.
- Split planning.
- Concurrent scan execution.
- Storage/query-engine boundaries.
- Vortex API integration strategy.

## Rules

- Treat the Vortex Scan API as an important design reference.
- Verify current upstream API behavior before implementing against it.
- Do not assume the full Scan API surface is stable.
- Prefer small adapters that can evolve with upstream Vortex.
- Keep ShardLoom's internal execution model independent.
- A Vortex source/sink integration is allowed.
- Execution fallback to another engine through Scan API is not allowed.
- Push projection and filters as close to storage as possible.
- Preserve compressed/native data paths where possible.
- Use explicit diagnostics for unsupported scan pushdowns.

## Required checks

For scan design:

- Projection behavior.
- Filter behavior.
- Split boundaries.
- Concurrent execution assumptions.
- Metadata availability.
- Encoding preservation.
- Unsupported pushdown diagnostic.
- Fallback avoidance.

For source/sink integration:

- Input contract.
- Output contract.
- Error handling.
- Version compatibility.
- Metadata preservation.
- Test strategy.

## Red flags

- Depending on unstable upstream APIs without isolation.
- Treating Scan API integration as a reason to delegate execution.
- Losing compressed representation at the source boundary.
- Pulling all data into decoded Arrow arrays immediately.
- Failing silently when a filter/projection cannot be pushed down.
- Assuming scan splits are equivalent to ShardLoom execution tasks without design review.

## Example Codex prompt fragment

"Use the Vortex Scan API skill. Treat upstream Scan API as a design reference and possible adapter boundary, not as fallback execution. Keep ShardLoom's internal execution independent and preserve compressed paths where possible."
