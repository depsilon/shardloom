<!-- SPDX-License-Identifier: Apache-2.0 -->

# OpenSSF Scorecard Alert Triage

Status: June 17, 2026 review of 82 open GitHub code-scanning alerts emitted by OpenSSF
Scorecard. This document records closure/remediation disposition only; it does not publish
packages, create releases, enable repository settings, or authorize fallback execution.

## Alert Inventory

Source command:

```powershell
gh api --paginate -H "Accept: application/vnd.github+json" `
  "/repos/depsilon/shardloom/code-scanning/alerts?state=open&per_page=100"
```

| Rule | Count | Disposition |
| --- | ---: | --- |
| `PinnedDependenciesID` | 73 | Remediated in repository. Workflow actions are pinned to immutable commit SHAs and Python build/audit installs now use hash-locked requirements with `--require-hashes`. |
| `TokenPermissionsID` | 2 | Remediated in repository. CodeQL and Scorecard keep top-level `contents: read`; `security-events: write` is scoped to the upload/analyze jobs that need it. |
| `SecurityPolicyID` | 1 | Remediated in repository. `SECURITY.md` now links the GitHub private-advisory reporting path and related response docs. |
| `VulnerabilitiesID` | 1 | Remediated in repository where concrete source issues were found. Website dependencies audit clean after lockfile refresh and overrides; benchmark Python profile requirements are pinned and audit clean; release runtime dependency audit remains clean with the documented `RUSTSEC-2024-0436` upstream-transitive waiver. |
| `BranchProtectionID` | 1 | Repository-setting remediation required. Enable branch protection on `main`; do not dismiss as fixed from code changes. |
| `CodeReviewID` | 1 | Repository-setting remediation required. Require pull request review before merge through branch protection/rulesets; do not dismiss as fixed from code changes. |
| `FuzzingID` | 1 | Valid hardening gap. Add an admitted fuzzing integration/harness before closing, or explicitly accept the gap for the current release posture. |
| `CIIBestPracticesID` | 1 | Governance/process gap. Start and link the OpenSSF Best Practices badge process, or accept/dismiss as non-code governance scope. |
| `MaintainedID` | 1 | Time-based repository-age signal. The repository was created within 90 days at alert creation; this is not code-remediable and should auto-resolve or be dismissed as informational once maintainers agree. |

## Repository Remediation Applied

- `.github/workflows/ci.yml` pins GitHub Actions to immutable SHAs and preserves source tags only
  as comments for review/update clarity.
- `.github/workflows/ci.yml` and `.github/workflows/pypi-publish-draft.yml` install Python build
  tooling from `.github/requirements/ci-python-build.txt` using `--require-hashes`.
- `.github/workflows/ci.yml` installs dependency-audit tooling from
  `.github/requirements/ci-dependency-security.txt` using `--require-hashes`.
- `.github/workflows/codeql-analysis.yml` and `.github/workflows/scorecard.yml` scope
  `security-events: write` to the only jobs that need SARIF/security-event upload.
- `website-src/package-lock.json` has no `npm audit --audit-level=low` findings after refreshing
  Astro/Starlight tooling and overriding vulnerable transitive `esbuild` and
  `yaml-language-server` versions.
- `benchmarks/traditional_analytics/requirements*.txt` direct dependencies are pinned to audited
  versions so repository-level advisory scanners do not interpret unbounded benchmark profiles as
  vulnerable package ranges.
- `scripts/check_dependency_audit.py` now reports every benchmark profile separately instead of
  only the base profile.
- The website/docs CI lane now runs `npm audit --audit-level=low` after `npm ci`.

## Remaining Maintainer Actions

These are not safe to silently change in a code PR because they alter repository governance:

1. Enable protection/ruleset enforcement for `main`.
2. Require pull request reviews before merge.
3. Select required status checks after this PR's final workflow names are known.
4. Decide whether to start the OpenSSF Best Practices badge process now or dismiss that alert as
   governance scope for the current release.
5. Decide whether to add a fuzzing integration in the next release-hardening workstream or accept
   the current absence as an explicitly tracked gap.

## Verification

Expected local evidence for this remediation:

```powershell
npm audit --audit-level=low --json
python -m pip_audit --requirement benchmarks/traditional_analytics/requirements.txt --progress-spinner off
python -m pip_audit --requirement benchmarks/traditional_analytics/requirements-extended-local.txt --progress-spinner off
python -m pip_audit --requirement benchmarks/traditional_analytics/requirements-spark.txt --progress-spinner off
python scripts/check_dependency_audit.py --release-gate --json-output target/dependency-audit-report.json
python scripts/check_security_posture.py
python scripts/check_release_security_gate.py
python scripts/check_v1_security_ci_hardening.py
python scripts/check_ci_gate_matrix.py
```

After merge, run the OpenSSF Scorecard workflow again and re-query open code-scanning alerts. Alerts
listed as remediated above should close or receive a fresh, narrower instance. Repository-setting
and governance alerts should remain open until maintainer settings/process work is completed or
explicitly dismissed.
