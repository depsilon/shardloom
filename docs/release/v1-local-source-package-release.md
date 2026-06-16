<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Local Source And Package Release Track

Status: selected local/source/package v1 release track with v0.1.1 GitHub pre-release, TestPyPI,
PyPI, and Homebrew channel proof complete.

Schema marker: `shardloom.v1_local_source_package_release.v1`.

Validate with:

```powershell
python scripts\check_v1_local_source_package_release.py
```

This page narrows the feasible v1 release after excluding real production environments. Maintainer
approval and channel proof now exist for the v0.1.1 GitHub pre-release, TestPyPI, PyPI, and
Homebrew sequence. This page does not itself publish additional packages, create new tags, create
new GitHub releases, upload new artifacts, sign artifacts, add secrets, run production services, or
authorize fallback execution.

## Selected V1 Track

| Workstream | V1 decision | Evidence owner |
| --- | --- | --- |
| Source checkout install | Supported local path. | `docs/getting-started/source-checkout-install.md`, `scripts/release_dry_run_proof.py` |
| Local wheel/sdist proof | Required before publication. | `target/release-dry-run-proof/transcript.json`, `python/dist/*` |
| Python user-surface proof | Required local smoke and scenario proof. | `examples/local-python-smoke/run.py`, `examples/local-python-benchmark-scenarios/run.py`, `examples/local-python-benchmark-scenarios/timing_review.py` |
| API/schema stability | Stable local v1 machine-readable contract. | `docs/release/v1-api-schema-stability.md`, `docs/release/schemas/v1/*`, `scripts/check_v1_api_schema_stability.py` |
| Local benchmark publication | Scoped full-local evidence only. | `website/assets/benchmarks/latest/manifest.json` |
| Docs/website/readme | Claim-safe public interpretation layer. | `README.md`, `docs/release/public-status-matrix.md`, `website-src/` |
| GitHub pre-release | Published v0.1.1 release assets with channel proof. | `docs/release/channel-proofs/github-prerelease-v0.1.1-transcript.json` |
| TestPyPI | Published v0.1.1 rehearsal package with Trusted Publisher proof. | `docs/release/channel-proofs/testpypi-v0.1.1-transcript.json` |
| PyPI | Published v0.1.1 public Python package with prior TestPyPI proof. | `docs/release/channel-proofs/pypi-v0.1.1-transcript.json` |
| Homebrew tap | Published v0.1.1 public CLI formula against the GitHub source archive. | `docs/release/channel-proofs/homebrew-v0.1.1-transcript.json` |

## Publication Sequence Completed For Selected Channels

The selected v0.1.1 channel order was:

1. Merge the v0.1.1 release-prep source revision.
2. Create the GitHub v0.1.1 release and attach source, wheel, sdist, CLI, checksums, SBOM, and
   provenance assets.
3. Publish TestPyPI through Trusted Publisher/OIDC and run the clean registry install/uninstall
   smoke transcript.
4. Commit or otherwise attach the TestPyPI proof reference required by the PyPI workflow.
5. Publish PyPI through Trusted Publisher/OIDC and run the clean registry install/uninstall smoke
   transcript.
6. Publish the Homebrew tap formula against the immutable GitHub v0.1.1 source archive and run
   `brew install`, `shardloom status --format json`, and `brew uninstall` proof.

The completed publication proof records:

- release version and tag: `v0.1.1`
- selected channels: GitHub pre-release, TestPyPI, PyPI, Homebrew
- exact source revision: `99093904d923d275072456512627110b4c0862d2`
- release notes
- checksum, SBOM, provenance, and signing/attestation policy
- rollback, yank, delete, or advisory plan per channel
- clean release gate evidence at the selected revision

The package publication state is:

```text
package_channel_status=published_v0.1.1_selected_channels
package_install_commands_visible=true
public_release_claim_allowed=false
public_package_claim_allowed=false
```

## Runtime Feature-Gate Packaging Note

The selected GitHub, PyPI, and Homebrew channels expose the default v0.1.1 package/CLI posture.
Feature gates remain runtime/build-scope qualifiers and do not become package-channel readiness
claims by being named in the public surface:

| Feature gate | Package/Homebrew posture | Claim boundary |
| --- | --- | --- |
| `universal-format-io` | Feature-gated local flat-scalar structured input/output support; not a default broad-format production claim. | Parquet, Arrow IPC, Avro, and ORC remain scoped local adapter/sink evidence surfaces until their feature-gated build and tests are explicitly selected. |
| `vortex-write` | Feature-gated local Vortex output support; default builds must fail closed with a deterministic sink blocker when disabled. | Vortex output is the highest-fidelity target, but broad native write/commit/object-store behavior requires separate write-intent, recovery, and Native I/O evidence. |
| `vortex-traditional-analytics-benchmark` | Feature-gated benchmark/runtime evidence path; not part of package installation proof by itself. | Benchmark-family prepared/native routes can support local evidence rows, but package availability does not imply performance superiority or production runtime scope. |

Package-channel proofs must state which binary/build profile was smoked. If a future channel ships
with any of these gates enabled by default, the channel proof must include the matching route,
fallback-disabled, Native I/O, and rollback evidence before the channel can be marked ready.

## Bundled CLI Wheel Strategy

Managed Python environments should not require user code to pass `repo_root`, `profile_order`, or
`SHARDLOOM_BIN` when a supported platform wheel includes ShardLoom's own CLI binary. The selected
strategy is bundled platform wheels in the `shardloom` package, with binary resources staged under
`shardloom/bin/<platform-tag>/shardloom` or `shardloom/bin/<platform-tag>/shardloom.exe`.

Python binary resolution precedence is:

```text
explicit binary argument
-> SHARDLOOM_BIN
-> SHARDLOOM_REPO_ROOT target/<profile>/shardloom
-> bundled wheel CLI resource
-> shardloom on PATH
-> deterministic binary-resolution error
```

Runtime binary download is rejected for this release track. Any wheel that includes a bundled CLI
must carry checksum, SBOM/provenance, clean install/uninstall, and no-fallback smoke evidence for
that exact platform artifact before publication. On POSIX platforms, bundled CLI resources must
preserve the executable bit; non-executable packaged binaries are ignored and normal resolver
fallbacks continue in order.

The PyPI Trusted Publisher draft workflow must build publishable wheel/sdist artifacts from the
same staged package tree used by the release dry-run proof: build `shardloom-cli`, copy the CLI into
`shardloom/bin/<platform-tag>/`, build artifacts from the staged package directory, and upload only
that staged `dist` directory. Direct `python -m build python` publication is not sufficient for
bundled-CLI releases because it omits the managed-environment binary resource.

## Deferred Environment Gates

These gates are intentionally not part of the v1 public package release because the real service
environments are unavailable:

- production object-store claim
- production table/lakehouse claim
- production distributed claim
- production live/hybrid claim
- real Foundry integration claim

They remain explicit fail-closed claim gates. Local fixtures and reports may stay in the product as
evidence of scoped behavior, but they must not become production/platform claims.

## Claim Boundary

The release may be described as a source-checkout and package-installable technical preview only
after the selected package channels are actually published and verified. Local benchmark artifacts
may be published as scoped local evidence, but `performance_claim_allowed=false` remains in force
unless a separate benchmark claim gate is approved.

No selected channel may introduce Spark, DataFusion, DuckDB, Polars, pandas, Dask, Velox, Trino, or
another external query engine as a ShardLoom runtime fallback.
