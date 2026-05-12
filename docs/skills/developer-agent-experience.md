# Developer and Agent Experience Skill

## Purpose

Use this skill when designing or implementing public APIs, CLI commands, diagnostics, explain
output, estimate output, config files, docs, examples, or agent-facing behavior.

ShardLoom should be highly performant internally while remaining flexible, familiar, easy to use,
and excellent for human developers and LLM agents.

## When to use

Use this skill for tasks involving:

- Public API design.
- CLI commands.
- Python API design.
- Rust API design.
- Diagnostics.
- Error messages.
- Explain output.
- Estimate output.
- Capability discovery.
- Config files.
- Examples.
- Documentation.
- Agent workflows.
- Machine-readable output.

## Rules

- Internal complexity should produce external simplicity.
- Simple usage should not require advanced Vortex knowledge.
- Advanced controls should be available through progressive disclosure.
- APIs should feel familiar to data engineers and application developers.
- CLI commands should be scriptable.
- Agent-facing output should be deterministic and machine-readable where possible.
- Agents should use `agent-contract-pack --format json` to discover stable command surfaces and
  inspection order before relying on human text.
- Errors should be specific, actionable, and stable.
- Unsupported behavior must fail explicitly.
- Diagnostics should explain that no fallback execution occurred.
- Native Vortex output should be easy to select.
- Compatibility exports should clearly report metadata loss.
- Do not hide behavior behind magic.
- Do not add Spark or DataFusion fallback.
- Do not make performance claims without benchmarks.

## Required checks

For public API changes:

- Is the simple case simple?
- Is the advanced case possible?
- Is the behavior explicit?
- Is native Vortex output obvious?
- Are compatibility exports clearly labeled?
- Are unsupported cases clear?
- Are docs/examples updated?

For CLI changes:

- Is the command scriptable?
- Does it support or plan for `--format json` where relevant?
- Does it fail deterministically?
- Does it avoid hidden side effects?
- Does it distinguish dry-run from execution?
- Does it expose useful diagnostics?

For agent-facing behavior:

- Can an LLM agent discover capabilities?
- Can it inspect a plan before execution?
- Can it estimate cost before execution?
- Can it understand unsupported behavior?
- Can it avoid destructive operations?
- Are fields stable and machine-readable?
- Are next steps explicit?

## Red flags

- Simple tasks require deep internal knowledge.
- Errors say only "unsupported" without context.
- CLI output is impossible to parse.
- A write operation has surprising side effects.
- Compatibility export silently loses metadata.
- The API hides whether data was decoded or materialized.
- The implementation is fast but impossible to explain.
- Agent workflows require guessing.
- Spark/DataFusion is added to make UX easier.

## Example Codex prompt fragment

"Use the Developer and Agent Experience skill. Keep the external API familiar and simple, expose
structured diagnostics, support future machine-readable output, make native Vortex output obvious,
and do not introduce fallback execution."
