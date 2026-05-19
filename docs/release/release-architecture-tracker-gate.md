<!-- SPDX-License-Identifier: Apache-2.0 -->

# Release Architecture Tracker Gate

Status: GAR-0043-A fail-closed release architecture tracker. This gate does not publish packages,
create tags, add secrets, or authorize fallback execution.

## Command

```powershell
python scripts\check_release_architecture_tracker.py
```

For local inspection while release evidence is still incomplete:

```powershell
python scripts\check_release_architecture_tracker.py --allow-blocked
```

The script writes:

```text
target/release-architecture-tracker-report.json
```

The report uses schema:

```text
shardloom.release_architecture_tracker_report.v1
```

## Gate Coverage

The tracker checks that release claims are blocked when architecture evidence is still open across:

- `docs/architecture/global-architecture-review.md`
- `docs/architecture/phased-execution-plan.md`
- `docs/architecture/rfc-phase-traceability.md`
- `docs/release/known-unsupported-paths.md`
- `docs/security/release-security-gate.md`
- `docs/release/release-provenance-dry-run.md`
- `docs/release/per-claim-evidence-attachment-matrix.md`
- `docs/architecture/phased-execution-completed-ledger.md`

It records:

```text
architecture_tracker_status=blocked
claim_gate_status=not_claim_grade
public_release_claim_allowed=false
public_package_claim_allowed=false
unchecked_global_architecture_review_count
unchecked_phase_plan_count
unchecked_global_architecture_review_items
unchecked_phase_plan_items
traceability_matrix_present
known_unsupported_paths_present
release_security_refs_present
release_provenance_refs_present
per_claim_evidence_matrix_present
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

## Blocking Rules

The tracker blocks public release/package claims when any unchecked Global Architecture Review item
remains, any unchecked phased-plan item remains, required RFC traceability markers are missing,
unsupported-path evidence is missing, security/provenance evidence refs are missing, or the per-claim
evidence matrix is absent or incomplete.

Unchecked GAR IDs must be visible either in the active phase plan or in the completed ledger. This
keeps broad review findings from disappearing into prose without an implementation slice or
completed evidence block.

## Current Expected State

The current expected state is blocked:

```text
status=blocked
architecture_tracker_status=blocked
claim_gate_status=not_claim_grade
public_release_claim_allowed=false
public_package_claim_allowed=false
```

That blocked state is correct while runtime, publication, package-channel, unsupported-path, and
final attestation work remains open. The tracker is evidence for release discipline, not release
approval.

## Non-Goals

This gate does not publish packages, create release tags, sign artifacts, upload SBOMs, resolve
credentials, invoke network services, run runtime workloads, or close unchecked architecture work.
It only makes the remaining architecture/release state machine-readable for the hard release gate.
