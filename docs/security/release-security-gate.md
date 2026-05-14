<!-- SPDX-License-Identifier: Apache-2.0 -->

# Release Security Gate

Status: P8.0G security evidence integration for P8.4. This gate does not publish packages, create
tags, add secrets, or authorize runtime fallback.

## Command

```powershell
python scripts\check_release_security_gate.py
```

For local inspection while evidence is still incomplete:

```powershell
python scripts\check_release_security_gate.py --allow-blocked
```

The script writes:

```text
target/release-security-gate-report.json
```

## Required Evidence

The gate requires refs for:

- `SecurityThreatModelReport`
- `VulnerabilityResponseReport`
- `DependencyAuditReport`
- `SupplyChainReleaseEvidence`
- `RuntimeInputSafetyReport`
- `OpenSourceSecurityPostureReport`
- `KnownUnsupportedPathsReport`

## Blocking Rules

The gate blocks public release claims when any of these are missing or incomplete:

- threat model
- `SECURITY.md`
- dependency audit report
- SBOM refs
- checksum refs
- supply-chain provenance report
- runtime malicious-input/path-safety/redaction tests
- workflow-hardening posture report
- known unsupported paths

The gate also blocks if evidence shows:

- `publication_attempted=true`
- `tag_created=true`
- `secrets_required=true`
- `fallback_attempted=true`
- `external_engine_invoked=true`
- fallback-engine runtime dependency present

## Current Expected State

Before P8.4 is complete, this gate may legitimately produce `status=blocked` when external audit
tools, full generated evidence, clean Conda proof, or later release-readiness artifacts are absent.
That blocked status is intentional: public release claims cannot pass until P8.4 has all required
proof artifacts.

## Non-Goals

This gate does not run the whole release process by itself. P8.4 remains responsible for full
workspace validation, feature/build matrix validation, clean install proof, benchmark smoke,
package metadata checks, and final claim generation from evidence artifacts.
