<!-- SPDX-License-Identifier: Apache-2.0 -->

# Hard Release Readiness Gate

Status: P8.4 release gate command. This gate is fail-closed and does not publish packages, create
tags, add secrets, or authorize fallback execution.

## Command

```powershell
python scripts\check_release_readiness.py
```

For local inspection while evidence is still incomplete:

```powershell
python scripts\check_release_readiness.py --allow-blocked
```

The script writes:

```text
target/hard-release-readiness-gate.json
```

## Gate Coverage

The gate aggregates:

- clean install, first-10-minutes, and local benchmark smoke transcript
- clean Conda environment install proof
- release security gate report
- package metadata, license, repository, and homepage metadata
- feature/build matrix execution evidence
- typed-envelope compatibility posture
- required validation command evidence
- global architecture runtime-claim gate evidence for distributed, object-store, and lakehouse
  public-claim boundaries

Required validation commands before public release:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
python -m unittest discover python/tests
python -m build python
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
cargo run -q -p shardloom-cli -- global-architecture-gate --format json
python scripts\check_release_security_gate.py
```

The local evidence runner records the feature/build matrix and required validation command status:

```powershell
python scripts\run_release_validation_evidence.py
```

It writes:

```text
target/release-validation-evidence.json
```

That report uses schema `shardloom.release_validation_evidence.v1` and contains:

```text
feature_build_matrix_status
required_validation_status
supporting_security_dependency_status
feature_build_matrix_rows
required_validation_commands
command_results
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

The global architecture gate uses schema
`shardloom.global_architecture_runtime_claim_gate.v1` and must keep
`runtime_claim_allowed=false`, `public_claim_allowed=false`, `fallback_attempted=false`, and
`external_engine_invoked=false` unless distributed, object-store, and lakehouse claims have their
own workload-scoped evidence.

The broader release process must also attach clean Conda proof, benchmark smoke evidence,
package metadata/license proof, SBOM/checksum/provenance evidence, runtime no-fallback dependency
audit, and release notes or known-unsupported-path evidence before public claims are allowed.

`GAR-PERF-2H` adds the future optimized build-profile and PGO benchmark lane. Until that gate is
implemented, portable release artifacts remain the normal release-profile artifacts. Any
`release-native-benchmark` or `target-cpu=native` build is benchmark-only and cannot satisfy public
release/package evidence. PGO artifacts must record training workload refs, profile artifact refs,
and claim gates before they can appear in benchmark evidence.

`clean_conda_env_install_status=passed` is required for a public-release pass. A source-local clean
venv install is useful P8.2 evidence, but it is not a substitute for the clean Conda proof required
before public package/release claims.

`scripts\release_dry_run_proof.py` records the clean Conda status as part of
`target/release-dry-run-proof/transcript.json`. When `mamba`, `conda`, or `micromamba` is not
available locally, the transcript records `clean_conda_env_install_status=skipped_tool_missing` and
the hard gate remains blocked. Maintainers can make missing or failing Conda proof fail the dry run
directly with:

```powershell
python scripts\release_dry_run_proof.py --require-clean-conda
```

## Claim Rule

`public_release_claim_allowed` and `public_package_claim_allowed` must remain false unless every gate
passes. Public claims must be generated from evidence artifacts, not prose.

## Current Expected State

When proof artifacts are missing, stale, or lack clean Conda evidence, the gate is expected to emit:

```text
status=blocked
public_release_claim_allowed=false
```

That blocked result is correct release behavior. It prevents accidental publication when any runtime,
protocol, packaging, benchmark, provenance, security, or unsupported-path proof is missing.

With current validation evidence, release security evidence, and a dry-run transcript containing
`clean_conda_env_install_status=passed`, the gate emits:

```text
status=passed
public_release_claim_allowed=true
public_package_claim_allowed=true
```

That pass is still local release-readiness evidence only. It does not publish packages, create tags,
upload artifacts, add secrets, or authorize unsupported-path claims.
