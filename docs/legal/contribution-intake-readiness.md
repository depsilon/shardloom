<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contribution Intake Readiness

Status: contribution governance gate for future outside contribution intake. This document records
what is automated, what is documented, and what remains blocked before ShardLoom accepts broader
community contributions or uses contribution posture in release/package readiness claims.

This is not legal advice, a public release approval, a package publication approval, a governance
transfer, or runtime behavior. It does not activate a CLA Assistant, DCO bot, or fallback engine.

```text
shardloom.contribution_governance_report.v1
contribution_intake_status=documented_and_ci_checked
external_contribution_acceptance_status=maintainer_approval_required
cla_assistant_status=not_active
dco_policy_status=not_active
legal_claim_status=documented_policy_only
public_release_claim_allowed=false
public_package_claim_allowed=false
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

## Automated Controls

The automated gate is intentionally narrow. It checks that contributor guidance, contributor
policy, this readiness document, the pull request template, CI wiring, and the CI gate matrix stay
aligned.

```text
automated_control=ci_contribution_governance_validator
```

Run:

```powershell
python scripts\check_contribution_governance.py
```

The script writes:

```text
target/contribution-governance-report.json
```

The report is consumed by release-readiness automation and the final no-publication release
rehearsal. A passing report means the repository has inspectable contribution controls and CI drift
checks. It does not mean outside contributions are automatically accepted.

## Documented Controls

```text
documented_control=required_signoff_cla_dco_state
documented_control=reviewer_roles_and_decision_escalation
documented_control=dependency_license_provenance_checklist
documented_control=security_release_rfc_checklist
documented_control=claim_boundary_checklist
documented_control=no_fallback_dependency_policy
documented_control=reviewer_roles_and_decision_escalation
```

The current required signoff/CLA/DCO state is:

- maintainer-authored and Codex-assisted changes may be reviewed and merged by the maintainer;
- outside pull requests require maintainer approval of the contribution-rights path before
  acceptance;
- the draft `CLA.md` is the default future route for outside contribution intake;
- DCO remains inactive unless the maintainer adds the DCO text, sign-off policy, and consistent
  automation;
- no external CLA Assistant is active.

Reviewer roles and decision escalation are documented in `CONTRIBUTING.md` and
`docs/legal/contributor-policy.md`. Security reports go through `SECURITY.md`; dependency,
license, and provenance review follows `docs/legal/license-provenance.md` and
`docs/skills/license-provenance.md`; release-impacting changes follow
`docs/skills/release-engineering-packaging.md`.

## Blocked Controls

```text
blocked_control=external_cla_assistant
blocked_control=dco_signoff_route
blocked_control=broad_governance_transfer
blocked_control=package_publication_from_contribution_gate
```

These controls remain blocked until the maintainer explicitly activates them:

- an external CLA Assistant or equivalent contributor-management service;
- a DCO route with `Signed-off-by` requirements and CI enforcement;
- broad maintainer-role delegation or governance transfer;
- package publication, release tags, signing, or package-channel submission based only on
  contribution governance posture.

## Claim Boundary

This gate supports only the claim that ShardLoom has contribution intake guidance and CI drift
checks for that guidance. It does not support claims of legal sufficiency, production readiness,
package availability, performance superiority, Spark replacement, broad SQL/DataFrame support,
object-store/lakehouse support, Foundry/platform support, or release readiness.

External engines remain benchmark baselines or correctness oracles only. Contribution automation
must preserve no-fallback dependency and claim policies.
