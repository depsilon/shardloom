<!-- SPDX-License-Identifier: Apache-2.0 -->

# Open-Source Security Posture

Status: P8.0F release-readiness posture. This document and its workflows do not publish packages,
create tags, add secrets, add runtime dependencies, or authorize fallback execution.

## Scope

ShardLoom's public-release posture combines repository configuration, scheduled/manual security
workflows, dependency update automation, maintainer-only GitHub settings, and release gate checks.

Primary references:

- GitHub CodeQL code scanning: <https://docs.github.com/en/code-security/concepts/code-scanning/codeql/about-code-scanning-with-codeql>
- CodeQL supported languages, including Rust and Python: <https://codeql.github.com/docs/codeql-overview/supported-languages-and-frameworks/>
- OpenSSF Scorecard action: <https://github.com/ossf/scorecard-action>
- Current OpenSSF Scorecard alert disposition:
  [`docs/security/scorecard-alert-triage.md`](scorecard-alert-triage.md)

## Configured Checks

### CodeQL

`.github/workflows/codeql-analysis.yml` runs CodeQL for Rust and Python on manual dispatch, pull
requests to `main`, and a weekly schedule.

Release posture:

- `contents: read`
- `security-events: write`
- Rust and Python language matrix
- `build-mode: none` for the current repository shape
- privileged workflow actions pinned to immutable commit SHAs, with source tags retained only as
  comments
- no publishing secrets
- no runtime behavior changes

### OpenSSF Scorecard

`.github/workflows/scorecard.yml` runs OpenSSF Scorecard on manual dispatch and a weekly schedule.
It uploads SARIF to GitHub code scanning and keeps public result publication disabled until a
maintainer explicitly approves it.

Release posture:

- `contents: read`
- `security-events: write`
- `publish_results: false`
- `persist-credentials: false`
- privileged workflow actions pinned to immutable commit SHAs, with source tags retained only as
  comments

### PyPI Trusted Publisher Draft

`.github/workflows/pypi-publish-draft.yml` is manual-dispatch only and requires the
`publish-approved` input plus the protected `pypi` environment before any upload can run.

Release posture:

- repository-level `contents: read`
- build job has no `id-token: write` permission
- publish job depends on the build artifact and is the only job with `id-token: write`
- checkout, Python setup, artifact transfer, and publish actions are pinned to immutable commit SHAs,
  with source tags retained only as comments
- no package publication is authorized without maintainer approval

### Dependabot

`.github/dependabot.yml` enables weekly update checks for:

- Cargo
- Python packaging metadata under `python/`
- GitHub Actions

Dependabot pull requests still require the normal dependency audit, license policy, CI, and
no-fallback checks before merge.

Cargo updates for `vortex` and `vortex-*` are grouped as `vortex-upstream` and must follow
`docs/dependencies/vortex-upstream-release-intake-runbook.md`. Dependabot may propose those updates,
but it must not auto-merge them or replace the ShardLoom-specific release-note/API inventory,
feature-gated compile proof, dependency-footprint review, and no-fallback evidence checks.

### Website Dependency Advisory Gate

The website/docs CI lane runs `npm audit --audit-level=low` after `npm ci`. The website source is
not a ShardLoom runtime dependency, but the committed `website-src/package-lock.json` is still a
repository manifest that Scorecard and GitHub advisory scanners inspect. Website dependency updates
must keep the lockfile audit clean or carry an explicit maintainer-approved advisory disposition.

## Maintainer Settings

The following repository settings cannot be fully represented in code and must be verified by a
maintainer before public release:

- GitHub secret scanning enabled
- push protection enabled
- branch protection on `main`
- required checks for CI, CodeQL, dependency audit, release provenance dry run, and benchmark smoke
  where practical
- required status checks for CI, CodeQL, dependency audit, release provenance dry run, and benchmark
  smoke where practical
- required pull request review before merge
- protected `pypi` environment with human approval
- protected release tags

## Local Verification

Run:

```powershell
python scripts\check_security_posture.py
```

The script emits:

```text
target/security-posture-report.json
```

The report uses `schema_version=shardloom.open_source_security_posture_report.v1` and verifies that
the CodeQL workflow, Scorecard workflow, PyPI Trusted Publisher workflow, Dependabot config, and
this posture document remain present and aligned. Privileged workflows with `security-events: write`
or `id-token: write` must keep action `uses:` refs pinned to immutable commit SHAs.

## No-Fallback Rule

Security posture tools may report issues, block releases, and generate evidence. They must not make
unsupported ShardLoom work succeed by invoking Spark, DataFusion, DuckDB, Polars, or any other
external engine as runtime fallback.
