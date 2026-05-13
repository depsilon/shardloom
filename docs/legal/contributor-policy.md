<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contributor Policy

## Status

Current policy for future contribution intake. ShardLoom is presently developed
by the maintainer with Codex-assisted implementation and documentation work.

## Summary

ShardLoom remains Apache-2.0 licensed. Future outside contributions require a
maintainer-approved provenance route before acceptance. The default future route
is the ShardLoom Individual Contributor License Agreement draft in `CLA.md`.
A Developer Certificate of Origin path may be added later, but it is not active
today.

## Current State

The repository is currently maintained by a sole maintainer using
Codex-assisted development. Codex-assisted changes are reviewed by the
maintainer for originality, correctness, license compatibility, no-fallback
architecture compliance, and appropriate tests before merge.

Outside pull requests may be discussed, but acceptance requires maintainer
approval of the contribution-rights path for that specific contribution.

## Future Outside Contributions

Before accepting future outside contributions, the maintainer must require one
of these routes:

- acceptance of the ShardLoom Individual Contributor License Agreement in
  `CLA.md`
- a maintainer-approved DCO policy if ShardLoom later chooses to support
  signed-off-by attestations as a lightweight alternative

ShardLoom does not currently ship `DCO-1.1.txt` and does not require
`Signed-off-by` lines. DCO remains a future optional policy choice. If activated,
the maintainer should add the DCO text, document the sign-off requirements, and
add CI or repository automation that checks the policy consistently.

## Bots And Dependency Updates

Bots, dependency update services, generated lockfile refreshes, or mechanical
metadata updates may be exempted only by explicit maintainer policy. Any
exemption must preserve:

- Apache-2.0 project licensing
- dependency license review
- no incompatible copied code
- no hidden fallback-engine dependency
- reproducible review of generated changes

No bot exemption is automatic merely because a change is small or mechanical.

## Incompatible Copied Code

Contributions must not include copied implementation code from GPL, AGPL, SSPL,
BUSL, proprietary, source-available, or unknown-license projects. Code copied
from blogs, forums, snippets, generated answers, or repositories is also
disallowed unless provenance and license compatibility are clear and the
maintainer approves the inclusion.

Contributors may independently implement ideas from papers, public standards,
specifications, and documentation. External ideas should be attributed where
appropriate, and behavior should be validated with ShardLoom-owned tests.

## AI-Assisted Contributions

AI-assisted contributions are allowed only after human review. Contributors are
responsible for checking generated content for originality, correctness,
license compatibility, provenance, no-fallback architecture compliance, and
adequate verification. AI assistance is not a substitute for contribution
rights.

## Future CLA Assistant Activation

No external CLA Assistant is active. If ShardLoom later activates one, the
maintainer should:

- choose the contribution route to enforce: CLA only, DCO only, or both
- publish the final accepted CLA or DCO policy before activation
- configure the service without changing ShardLoom's Apache-2.0 project license
- document bot and maintainer exemptions explicitly
- add CI or repository checks so the accepted route is visible and repeatable
- verify the service does not require package publication or runtime changes

## Non-Goals

This policy does not:

- change the project license away from Apache-2.0
- activate an external CLA Assistant
- add DCO as an accepted route today
- add runtime dependencies or runtime behavior
- permit fallback execution through external query engines

## Acceptance Criteria

The contribution policy is ready for future activation when:

- `CONTRIBUTING.md`, `CLA.md`, and this policy agree on contribution routes
- pull requests ask contributors to confirm rights and provenance
- incompatible copied code remains disallowed
- AI-assisted contribution review is explicit
- employer or client contribution-rights warnings are visible
- future CLA Assistant or DCO activation requires maintainer action
