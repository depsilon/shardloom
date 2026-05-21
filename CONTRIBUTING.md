<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contributing to ShardLoom

ShardLoom is an Apache-2.0 licensed project focused on standalone,
Vortex-native, no-fallback execution. This document is the practical
contributor entry point; the detailed legal posture lives in
`docs/legal/contributor-policy.md` and `docs/legal/license-provenance.md`.

## Current Contribution State

ShardLoom is currently developed by the maintainer with Codex-assisted work.
Outside contributions are not automatically accepted until the maintainer has
approved the contribution intake path for that contribution. This contributor
entry point is checked by `scripts/check_contribution_governance.py`; the
machine-readable gate is documented in
`docs/legal/contribution-intake-readiness.md`.

Future outside contributions require one of these maintainer-approved routes:

- acceptance of the ShardLoom Individual Contributor License Agreement in
  `CLA.md`
- a future DCO policy if the maintainer activates DCO as a lightweight
  alternative

No external CLA Assistant is active. DCO remains inactive until the maintainer
adds the DCO text, sign-off rules, and consistent automation.

## Contribution Governance Controls

Current required signoff/CLA/DCO state:

- maintainer-authored and Codex-assisted changes may be reviewed and merged by
  the maintainer;
- outside contributions require maintainer approval of the contribution-rights
  route before acceptance;
- the draft `CLA.md` is the default future outside-contribution route;
- DCO remains inactive unless a future DCO policy is explicitly activated;
- no external CLA Assistant, DCO bot, package publication workflow, or release
  action is authorized by contribution governance alone.

Contributor review must keep these references aligned:

- `AGENTS.md` for no-fallback architecture and repository contribution rules
- `docs/legal/contributor-policy.md` for contributor-rights policy
- `docs/legal/license-provenance.md` for license and provenance rules
- `docs/skills/license-provenance.md` for dependency and copied-code review
- `docs/skills/release-engineering-packaging.md` for release/package impacts
- `docs/architecture/phased-execution-plan.md` for active planned work
- `SECURITY.md` for vulnerability reports

## Maintainer Roles And Review States

The current role model is intentionally simple:

- `maintainer`: owns merge decisions, contribution-rights acceptance, release
  approval, and dependency/license/provenance waivers;
- `reviewer`: may provide technical review, request changes, or identify
  policy blockers, but does not approve outside-contribution rights by default;
- `contributor`: proposes code, docs, tests, or issue content and confirms
  rights, provenance, no-fallback impact, and validation evidence.

Review-state reporting for pull requests should use the PR template sections:
`Contribution Route`, `No-Fallback And Dependency Check`,
`Security, Release, And RFC Impact`, `Claim Boundary`, `Reviewer State`, and
`Tests Run`.

## Decision Escalation

Escalate to the maintainer before merge when a change touches contribution
rights, CLA/DCO state, copied code, new dependencies, package/release posture,
security policy, public claims, RFC-level architecture, runtime fallback risk,
or external-engine boundaries. If the contribution route is unclear, the pull
request remains blocked until the maintainer records the accepted path.

## Contributor Checklist

Before opening a pull request, confirm:

- you have the right to submit the contribution
- the contribution is original work or is clearly derived from compatible,
  documented sources
- no implementation code was copied from GPL, AGPL, SSPL, BUSL, proprietary,
  source-available, or unknown-license projects
- AI/Codex-generated content was reviewed for originality, correctness,
  license compatibility, and tests
- any employer-owned or client-owned contribution rights have been cleared
- no Spark, DataFusion, DuckDB, Polars, Velox, or other external query engine
  dependency was added as a ShardLoom runtime fallback
- relevant tests and formatting checks were run

## Development Guardrails

Contributions must preserve these project constraints:

- ShardLoom remains Apache-2.0 licensed.
- External engines may be used as benchmark baselines or correctness oracles
  only, not as hidden execution fallback.
- New dependencies require license, provenance, architecture, and no-fallback
  review.
- Performance claims require reproducible benchmark evidence.
- Unsupported behavior should fail with deterministic diagnostics.

## How to Propose Work

For code changes, keep pull requests focused and include:

- the behavior or policy changed
- the relevant tests or validation commands
- any dependency, license, provenance, or fallback-risk impact
- links to related issues or RFCs when applicable

For substantial architecture or public API changes, update or add the relevant
RFC or architecture document before implementation.
