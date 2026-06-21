<!-- SPDX-License-Identifier: Apache-2.0 -->

# Production Usability Gate

Status: executable local no-publication usability gate.

`shardloom.production_usability_gate.v1` aggregates the local first-10-minutes, package-smoke,
website learning, benchmark-artifact, security/legal, and release-rehearsal evidence that a
non-expert reviewer needs before interpreting ShardLoom's current runtime state.

It is deliberately not the hard public-release gate. The report must keep:

```text
claim_gate_status=not_claim_grade
production_claim_allowed=false
performance_claim_allowed=false
public_release_claim_allowed=false
public_package_claim_allowed=false
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

## Command

Generate the upstream local evidence first:

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
python scripts\check_release_security_gate.py
python scripts\check_contribution_governance.py
python scripts\check_package_channel_readiness.py --require-local-evidence
python scripts\check_golden_workflows.py
python scripts\check_admitted_semantics_matrix.py
python scripts\check_release_architecture_tracker.py --allow-blocked
python scripts\final_release_rehearsal.py --allow-blocked
python scripts\check_website_readiness.py
```

Then run:

```powershell
python scripts\check_production_usability_gate.py
```

The validator writes:

```text
target/production-usability-gate.json
```

## What It Proves

The gate requires:

- `target/release-dry-run-proof/transcript.json` with a clean venv local-wheel install,
  installed-wheel Python client smoke, CLI status/capabilities smokes, local Python example smoke,
  first user-surface result/evidence markers, deterministic unsupported-path evidence,
  generated-source local output smokes, `benchmark_smoke_required_for_package_release=false`,
  SBOM/checksum/provenance dry run, and no-publication/no-fallback fields.
  In CI, the transcript may retain the original local package-stage paths while the compact
  evidence artifact restores the provenance-referenced wheel/sdist under `python/dist` and the CLI
  under `target/debug`; the gate resolves only those declared compact locations.
- `target/package-channel-readiness-report.json` generated with `--require-local-evidence`, while
  every real package channel remains blocked until channel-specific proof exists.
- release security and contribution-governance reports with fallback and external-engine fields
  false.
- final release rehearsal evidence that may still be blocked for hard public-release reasons, but
  keeps local artifacts only, unsigned attestation status, publication approval recorded, and public
  release/package claims false.
- website readiness evidence and checked start/status/docs assets.
- current benchmark publication evidence remains separated from product usability. Use the explicit
  benchmark publication validators and runbook when promoting benchmark data; default release
  readiness does not rerun or reinterpret benchmark artifacts.
- the generated `runs-today` support matrix with production and package-publication claim rows
  blocked and `claim_gate_status=not_claim_grade`.
- README, getting-started, release, SECURITY, LICENSE, NOTICE, package metadata, and website start
  page references that let a user run the local flow without reading the phase plan.

The website readiness input is `target/website-readiness-report.json`.

## Relationship To Public Release Readiness

This gate can pass while the hard release-readiness gate remains blocked. That is expected when
local usability is coherent but public publication, clean Conda release proof, package-channel
submissions, signing, tags, or full runtime/claim-grade evidence are not authorized.

Use `scripts\check_release_readiness.py` for the hard public-release aggregation. The hard gate
consumes this report, but a passing production-usability report never publishes packages, creates a
tag, uploads artifacts, signs attestations, adds secrets, or authorizes production/performance
claims.
