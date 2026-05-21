<!-- SPDX-License-Identifier: Apache-2.0 -->

# Final Release Rehearsal

Status: GAR-0043-B local no-publication rehearsal. This workflow does not publish packages, create
tags, sign artifacts, upload SBOMs, submit feedstocks, submit marketplace listings, add secrets, or
authorize fallback execution.

## Command

```powershell
python scripts\final_release_rehearsal.py
```

For local inspection while release evidence and human approval are incomplete:

```powershell
python scripts\final_release_rehearsal.py --allow-blocked
```

The script writes:

```text
target/final-release-rehearsal/final-release-rehearsal-report.json
target/final-release-rehearsal/local-publication-attestation-plan.json
```

The report uses schema:

```text
shardloom.final_release_rehearsal_report.v1
```

The local attestation plan uses schema:

```text
shardloom.local_publication_attestation_plan.v1
```

## What The Rehearsal Proves

The rehearsal aggregates local release evidence refs:

- release provenance manifest
- `SupplyChainReleaseEvidence`
- checksum manifest
- SBOM refs
- release security gate report
- contribution governance report
- release architecture tracker report
- package-channel readiness report
- known unsupported paths
- per-claim evidence matrix
- publication/API/schema stability gate

It records:

```text
rehearsal_status=passed
claim_gate_status=not_claim_grade
public_release_claim_allowed=false
public_package_claim_allowed=false
publication_authorization_status=human_approval_required
publication_human_approved=false
local_artifacts_only=true
package_artifact_ref_count
sbom_ref_count
checksum_ref_count
attestation_ref_count
final_attestation_status=not_signed_local_rehearsal
contribution_governance_report_ref
publication_attempted=false
tag_created=false
secrets_required=false
package_upload_attempted=false
feedstock_submission_attempted=false
marketplace_submission_attempted=false
signing_key_used=false
fallback_attempted=false
external_engine_invoked=false
```

The local attestation plan records `attestation_generation_status=not_signed_local_rehearsal` and
`slsa_attestation_status=not_generated_until_publication_approval`. Real signing and artifact
attestations remain maintainer-approved release actions, not autonomous Codex actions.

## Current Expected State

The current expected state for the local no-publication rehearsal is passed when all local
artifact, SBOM, checksum, provenance, security, architecture, package-channel, unsupported-path,
contribution-governance, per-claim, and publication/API/schema refs are present and internally
consistent:

```text
status=passed
rehearsal_status=passed
publication_human_approved=false
public_release_claim_allowed=false
public_package_claim_allowed=false
```

That local pass is not a public release pass. The surrounding hard release-readiness gate remains
blocked while package-channel evidence, publication/API/schema approval, architecture tracker
closeout, per-claim evidence, and maintainer approval remain incomplete.

## Claim Rule

This rehearsal may support only the claim that ShardLoom has a local no-publication release
rehearsal. It does not allow a public release claim, public package claim, production claim,
performance claim, Spark-replacement claim, Foundry/platform claim, object-store/lakehouse claim,
or SQL/DataFrame production claim.

## Non-Goals

The rehearsal does not publish to PyPI, TestPyPI, crates.io, conda-forge, Homebrew, Scoop, winget,
GHCR, Foundry Marketplace, or GitHub Releases. It does not create tags, upload release assets,
resolve credentials, sign artifacts, generate public attestations, invoke network publication APIs,
or run unsupported runtime paths.
