# RFC 0010: Developer Experience, API Ergonomics, and Agent Usability

## Status

Draft

## Summary

This RFC defines ShardLoom's developer experience and agent usability principles.

ShardLoom may be technically complex internally, but it should feel simple, familiar, inspectable, and safe to use.

ShardLoom should be usable by:

- Human developers embedding it in applications.
- Data engineers migrating workloads.
- Platform engineers deploying it in infrastructure.
- LLM agents and coding agents integrating it into repositories.
- Future automation systems that need deterministic, machine-readable behavior.

Performance is not enough. ShardLoom should also be easy to adopt.

## Context

ShardLoom is designed as a standalone Vortex-native encoded-columnar execution engine.

The internal system may include:

- Encoded segments.
- Vortex layouts.
- Statistics and pruning.
- Metadata-only execution.
- Late materialization.
- Dataset manifests.
- Snapshots.
- Object-store byte ranges.
- Translation reports.
- Distributed segment tasks.

Those concepts are necessary internally, but most users should not need to understand all of them to get value.

ShardLoom should expose simple, familiar interfaces while preserving advanced controls for users who need them.

## Goals

- Make ShardLoom easy to adopt in existing repositories.
- Provide familiar APIs for human developers.
- Provide deterministic, structured APIs for LLM agents.
- Support progressive disclosure: simple defaults first, advanced controls later.
- Make performance behavior inspectable.
- Make unsupported behavior clear and actionable.
- Make native Vortex input/output easy.
- Make compatibility exports easy without confusing them with fallback execution.
- Preserve ShardLoom's standalone no-fallback architecture.

## Non-goals

- Do not implement APIs in this RFC.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not define final SQL grammar.
- Do not define final Python bindings.
- Do not require every advanced feature in the first release.
- Do not hide unsupported behavior behind magic.

## Design principle

ShardLoom should follow this principle:

> Internal complexity should produce external simplicity.

Users should not need to understand every encoded execution detail before they can:

- Read a Vortex dataset.
- Filter data.
- Project columns.
- Write Vortex output.
- Export compatible output.
- Explain a plan.
- Estimate cost.
- Understand why a query is unsupported.

## User surfaces

ShardLoom should eventually support these surfaces.

### Rust API

The Rust API is the primary systems API.

It should support:

- Embedding ShardLoom in services.
- Building datasets and plans.
- Running native execution.
- Inspecting plans and diagnostics.
- Writing Vortex output.
- Exporting compatibility outputs.

The Rust API should be explicit and strongly typed.

### Python API

The Python API should be familiar to data users.

It should feel closer to dataframe/query builder patterns than low-level systems programming.

Example direction:

```python
import shardloom as sl

dataset = sl.dataset("s3://bucket/events.vortex")

result = (
    dataset
    .filter("event_date >= '2026-01-01'")
    .select(["customer_id", "event_type"])
    .write_vortex("s3://bucket/out/events_filtered.vortex")
)
```

The Python API should preserve ShardLoom's no-fallback policy.

### CLI

The CLI should be simple, scriptable, and agent-friendly.

Potential commands:

- `shardloom status`
- `shardloom doctor`
- `shardloom scan`
- `shardloom explain`
- `shardloom estimate`
- `shardloom capabilities`
- `shardloom convert`
- `shardloom benchmark`
- `shardloom manifest`
- `shardloom write-vortex`

The CLI should support machine-readable output:

- `--format text`
- `--format json`
- `--format yaml` if later approved

### Configuration file

ShardLoom should eventually support a project-level config file.

Potential name:

- `shardloom.toml`

Potential purpose:

- Dataset aliases.
- Output defaults.
- Object-store settings.
- Benchmark profiles.
- Engine limits.
- Diagnostics preferences.
- Default output format.
- Safety settings for writes.

Config should be optional. Simple usage should not require a config file.

### Agent API

ShardLoom should be excellent for LLM and coding agents.

Agent-facing behavior should include:

- Capability discovery.
- Machine-readable diagnostics.
- Explain plans.
- Dry runs.
- Cost estimates.
- Fidelity reports.
- Unsupported-feature reports.
- Deterministic errors.
- Idempotent write planning.
- Clear next-step suggestions.

Agents should be able to ask:

- What can ShardLoom do?
- Can this query run natively?
- What output formats are supported?
- What will be materialized?
- What metadata will be preserved?
- What metadata will be lost?
- Why is this unsupported?
- What is the safest next step?

## Familiarity requirements

ShardLoom should feel familiar where possible.

Familiar patterns may include:

- SQL-like filtering.
- DataFrame-like chaining.
- CLI commands with common Unix-style behavior.
- Rust builder patterns.
- Python context managers for resources.
- JSON diagnostics for automation.
- Dry-run behavior for risky operations.
- Explicit output targets.

ShardLoom should avoid novelty for novelty's sake.

## Progressive disclosure

ShardLoom should support multiple levels of usage.

### Level 1: Simple use

A user can read, filter, project, and write without learning internal execution.

### Level 2: Inspectable use

A user can call explain, estimate, capabilities, or doctor to understand behavior.

### Level 3: Tuned use

A user can tune materialization, output fidelity, resource budgets, and object-store behavior.

### Level 4: Expert use

A user can inspect segments, statistics, manifests, snapshots, and execution states.

Advanced features should not make simple usage harder.

## Error and diagnostic requirements

Errors should be:

- Deterministic.
- Specific.
- Actionable.
- Machine-readable where possible.
- Safe for agents to interpret.
- Clear about unsupported behavior.
- Clear that no fallback execution occurred.

A useful diagnostic should include:

- Error code.
- Human-readable message.
- Category.
- Related feature.
- Reason.
- Suggested next step.
- Whether fallback was attempted. This should normally be false.
- Whether format translation is available.
- Whether native Vortex output is available.

Example diagnostic shape:

```json
{
  "code": "SL_UNSUPPORTED_ENCODING",
  "category": "unsupported_feature",
  "feature": "encoded_predicate_fsst_contains",
  "message": "ShardLoom does not yet support contains predicates over this encoded string layout.",
  "fallback_attempted": false,
  "suggested_next_step": "Use an equality predicate, request partial decode explicitly, or track support for this encoding."
}
```

## Explain and estimate requirements

ShardLoom should eventually support explain and estimate operations.

Explain should show:

- Input dataset.
- Snapshot or manifest if available.
- Segments considered.
- Segments pruned.
- Metadata-only decisions.
- Encoded operations.
- Partial decode operations.
- Materialization boundaries.
- Output target.
- Fidelity level.
- Unsupported features.

Estimate should show:

- Estimated bytes read.
- Estimated bytes decoded.
- Estimated rows scanned.
- Estimated rows materialized.
- Estimated output size.
- Estimated object-store requests.
- Estimated memory.
- Estimated runtime category.
- Known uncertainty.

Estimates should be honest about uncertainty.

## Agent usability requirements

Agent-facing commands and APIs should support:

- JSON output.
- Stable field names.
- Capability discovery.
- Dry-run mode.
- No hidden side effects.
- Clear write intent.
- Idempotency keys for writes where relevant.
- Explicit confirmation boundaries for destructive operations.
- Deterministic unsupported errors.
- No silent fallback execution.

Agents should be able to integrate ShardLoom into a repository without guessing internal architecture.

## Safety and write behavior

ShardLoom should avoid surprising writes.

Write operations should eventually support:

- Dry run.
- Planned output preview.
- Temporary path.
- Commit plan.
- Overwrite policy.
- Idempotency key.
- Translation report.
- Native Vortex output confirmation.
- Compatibility output confirmation.

Destructive operations should require explicit opt-in.

## Performance and usability balance

Performance optimizations must not make the user experience opaque.

If ShardLoom avoids reads, prunes segments, uses encoded execution, or materializes late, users should be able to inspect that behavior.

The system should expose performance wins through diagnostics and explain output, not through hidden magic.

## API design rules

Future APIs should prefer:

- Familiar method names.
- Small composable builders.
- Explicit output targets.
- Optional advanced tuning.
- Clear result objects.
- Structured diagnostics.
- Stable machine-readable fields.
- Dry-run support.
- Explain support.

Future APIs should avoid:

- Hidden engine delegation.
- Ambiguous execution behavior.
- Undocumented side effects.
- Silent lossy translation.
- Overly clever syntax.
- Requiring advanced Vortex knowledge for common tasks.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Developer experience is a first-class design constraint.
- Agent usability is a first-class design constraint.
- Human APIs should be familiar and progressively disclosed.
- LLM/agent APIs should be structured, deterministic, and safe.
- Explain, estimate, doctor, and capabilities commands are important future surfaces.
- Errors should be actionable and machine-readable.
- Simplicity must not violate ShardLoom's no-fallback architecture.
- Vortex-native input/output remains central.

## Verification plan

Future implementation PRs should verify:

- Public APIs are documented.
- Unsupported errors are deterministic.
- Diagnostics include clear reason and next step.
- CLI output can be machine-readable where relevant.
- Native Vortex output is easy to select.
- Compatibility exports do not hide metadata loss.
- No fallback execution is introduced.
- Simple usage remains simple.

## Open questions

- What should the first public Python API look like?
- Should `shardloom.toml` be introduced early or later?
- What JSON schema should diagnostics use?
- What should the first `explain` output include?
- What should the first `estimate` output include?
- What commands should the CLI support first after `status`?
- When should Python bindings be introduced?
- Should TypeScript bindings be considered for agent and tooling workflows later?
