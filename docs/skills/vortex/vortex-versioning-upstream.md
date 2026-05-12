# Vortex Versioning and Upstream Skill

## Purpose

Use this skill when depending on Vortex crates, upstream APIs, release behavior, documentation, or
implementation details.

The goal is to keep ShardLoom compatible with Vortex without being fragile.

## When to use

Use this skill for tasks involving:

- Adding Vortex crate dependencies.
- Updating Vortex dependency versions.
- Using Vortex internals.
- Reading upstream Vortex docs.
- Relying on Scan API behavior.
- Contributing upstream to Vortex.
- Handling Vortex API changes.
- Pinning versions.
- Compatibility testing.

## Rules

- Prefer stable public Vortex APIs over internals.
- Isolate Vortex-specific code in `shardloom-vortex` where practical.
- Do not spread upstream Vortex details throughout the whole workspace.
- Pin or document Vortex dependency versions when added.
- Track upstream API volatility.
- Treat active-development APIs with caution.
- If ShardLoom needs an upstream Vortex capability, consider contributing upstream rather than
  forking behavior locally.
- Upstream Vortex array, compute, scan, source, and sink APIs may become
  ShardLoom-native providers only through isolated, feature-gated,
  version-recorded, certificate-backed boundaries.
- Do not vendor or copy upstream implementation code unless license and provenance are explicitly
  reviewed.
- Keep adapter boundaries small and testable.

## Required checks

Before adding or changing a Vortex dependency:

- Identify crate name and version.
- Identify license.
- Identify public APIs used.
- Identify any unstable or internal APIs used.
- Add tests for adapter behavior.
- Document assumptions.
- Confirm no fallback execution was introduced.
- Confirm Vortex-native input/output remains intact.

For upstream changes:

- Note what changed.
- Note compatibility impact.
- Update tests.
- Update docs if behavior changed.
- Avoid broad refactors unless necessary.

## Red flags

- Depending directly on unstable internals across multiple crates.
- Copying Vortex implementation code.
- Assuming active-development APIs are stable.
- Adding Vortex integrations that force ShardLoom into another engine's execution model.
- Treating Vortex query-engine integrations as ShardLoom-native execution.
- Making ShardLoom impossible to update when Vortex changes.
- Ignoring upstream changes to DTypes, encodings, layouts, or Scan API.

## Example Codex prompt fragment

"Use the Vortex Versioning and Upstream skill. Keep Vortex-specific code isolated, prefer public
APIs, document version assumptions, and do not copy upstream implementation code. Avoid fallback
execution."
