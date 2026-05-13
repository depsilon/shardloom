<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contributing to ShardLoom

ShardLoom is an Apache-2.0 licensed project focused on standalone,
Vortex-native, no-fallback execution. This document is the practical
contributor entry point; the detailed legal posture lives in
`docs/legal/contributor-policy.md` and `docs/legal/license-provenance.md`.

## Current Contribution State

ShardLoom is currently developed by the maintainer with Codex-assisted work.
Outside contributions are not automatically accepted until the maintainer has
approved the contribution intake path for that contribution.

Future outside contributions require one of these maintainer-approved routes:

- acceptance of the ShardLoom Individual Contributor License Agreement in
  `CLA.md`
- a future DCO policy if the maintainer activates DCO as a lightweight
  alternative

No external CLA Assistant, DCO bot, or contribution-gating automation is active
yet.

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
