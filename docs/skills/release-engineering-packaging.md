# Release Engineering Packaging

## Purpose

Use this skill when planning release engineering, API compatibility, packaging policy, schema
stability, dependency hygiene, and benchmark accountability.

## When to use

Use this document for release process design, package publication readiness, compatibility
commitments, changelog/release note policy, and artifact governance.

## Rules

- Do not publish packages without explicit human approval.
- Do not create releases without explicit human approval.
- Use Apache-2.0 metadata consistently.
- Review dependency licenses and transitive dependency impact.
- Avoid forbidden fallback dependencies.
- Mark unstable APIs appropriately.
- Machine-readable schemas need versioning before stability promises.
- Performance claims require reproducible benchmark evidence.
- Release notes should not overclaim Spark displacement.
- No Spark or DataFusion fallback should enter release artifacts.

## Required checks

- Confirm release surface versioning is explicit (crates, CLI, package, docs, schemas).
- Confirm API stability tier labels are applied (internal/experimental/stable/deprecated/removed).
- Confirm license metadata and NOTICE expectations are satisfied.
- Confirm dependency review includes license, security, architecture, and fallback risk.
- Confirm schema changes include compatibility notes and schema version handling.
- Confirm benchmark claims include reproducible methodology and limitations.
- Confirm documentation and release notes preserve no-fallback architecture statements.

## Red flags

- Any publish/release action without explicit human approval.
- API stability claims without versioned contract boundaries.
- Schema compatibility promises without version fields.
- Performance claims without reproducible benchmark context.
- Dependencies that introduce hidden execution engines or licensing incompatibility.
- Release notes implying Spark/DataFusion fallback behavior or broad displacement without evidence.

## Example Codex prompt fragment

"Draft this release plan with explicit approval gates, API stability tiers, schema versioning
requirements, dependency/license checks, and benchmark evidence requirements. Preserve no-fallback
architecture and avoid overclaiming performance or Spark displacement."
