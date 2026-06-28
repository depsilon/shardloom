<!-- SPDX-License-Identifier: Apache-2.0 -->

# Release Dry-Run Proof

Status: executable local proof. This workflow builds and inspects local
artifacts only; it does not publish packages, create tags, add secrets, submit
feedstocks, push images, or add runtime fallback dependencies.

## Command

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

The script writes:

```text
target/release-dry-run-proof/transcript.json
```

## What It Proves

The dry run performs these checks in order:

- builds the local `shardloom` CLI binary with Cargo
- stages the Python package under `target/` with the built CLI in
  `shardloom/bin/<system-arch>/`
- builds the Python wheel and sdist from the staged package tree
- creates a clean virtual environment under `target/`
- installs the exact local wheel artifact with `pip --no-index <wheel>`
- resolves the bundled CLI from the installed wheel without `SHARDLOOM_BIN` or
  `SHARDLOOM_REPO_ROOT`
- imports `shardloom` from the installed wheel
- runs `ShardLoomClient().smoke_check()`
- attempts a clean Conda-style proof with `mamba`, `conda`, or `micromamba` when one is available
- runs `client.capabilities()`
- runs `shardloom status --format json`
- runs `shardloom capabilities --format json`
- runs `examples/local-python-smoke/run.py`
- runs a scoped `ctx.from_rows([...]).write(local_jsonl)` generated-source output smoke from the
  clean installed wheel
- runs a scoped `ctx.range(...).write(local_jsonl)` engine-native generated-source output smoke from
  the clean installed wheel
- records that benchmark smoke is not required for package-channel proof
- runs `scripts/release_provenance_dry_run.py --skip-build`

The transcript records command return codes, bounded stdout/stderr excerpts, local wheel path, CLI
binary path, bundled CLI resource path, clean venv path, `wheel_client_resolved_bundled_cli`, and
release-safety booleans.

## Required Safety Fields

The transcript must keep these fields false:

```text
publication_attempted
tag_created
secrets_required
external_runtime_dependencies_added
fallback_engine_dependency_added
```

The transcript must record these evidence fields:

```text
clean_venv_install_status=passed
wheel_import_and_client_smoke_performed=true
cli_status_smoke_performed=true
cli_capabilities_smoke_performed=true
local_python_example_smoke_performed=true
local_python_user_surface_quickstart_performed=true
local_python_result_and_evidence_printed=true
local_python_unsupported_path_evidence_printed=true
provenance_dry_run_performed=true
sbom_checksum_manifest_generated=true
clean_conda_env_install_status=passed | skipped_tool_missing | skipped_by_request | failed
clean_conda_env_install_required=true | false
generated_output_proof_distinct_from_no_dataset_smoke=true
generated_source_user_rows_runtime_performed=true
generated_source_range_runtime_performed=true
benchmark_smoke_required_for_package_release=false
benchmark_smoke_status=skipped_not_required_for_package_release | passed
```

`clean_conda_env_install_status=passed` is required by the hard release-readiness gate before public
release/package claims are allowed. In local source checkouts without `mamba`, `conda`, or
`micromamba` on `PATH`, the dry run records `skipped_tool_missing`; that is useful local evidence,
but it intentionally keeps the release gate blocked. Maintainers can force the dry run to fail when
Conda proof is missing or broken with:

```powershell
python scripts\release_dry_run_proof.py --require-clean-conda
```

When using a local, non-`PATH` Conda-compatible executable, pass it explicitly:

```powershell
python scripts\release_dry_run_proof.py --conda-executable target\release-tools\miniforge3\_conda.exe --require-clean-conda
```

By default, the clean Conda proof requests the same Python major/minor version used to build the
local wheel, because the bundled-CLI wheel is CPython- and platform-specific. Override
`--conda-python-version` only when deliberately testing another compatible artifact.

The clean venv proof installs only the exact ShardLoom wheel built during the current dry run. It
removes `SHARDLOOM_BIN` and `SHARDLOOM_REPO_ROOT` before client smoke so the installed package must
resolve its bundled CLI resource. Release package proof builds the bundled CLI with
`--features release-user-surfaces` so promoted local adapters, Vortex writes, local primitives, and
provider-backed native Vortex user routes are present in release artifacts. Benchmark comparison
engines remain optional benchmark/dev dependencies and are not installed by this proof. Run
`python scripts\release_dry_run_proof.py --include-benchmark-smoke --rows 64 --iterations 1` only
when you intentionally want the optional local benchmark smoke in the same transcript.

The local Python smoke is no longer only import/status evidence. It creates a tiny local CSV,
proves the user-facing `ctx.read(...).filter(...).select(...).write_jsonl(...)` route blocks
before execution until Vortex preparation/native routing is available, runs a scoped
generated-source write, prints generated-output evidence/claim fields, and prints deterministic
blockers for both the local-file route and unsupported materialization with `fallback_attempted=false`
and `external_engine_invoked=false`.

The generated-source output smokes are deliberately distinct from no-dataset smoke. They write
local JSONL files under `target/release-dry-run-proof/`, emit
`generated_source_certificate_status=present`,
`output_native_io_certificate_status=certified_local_file_sink`,
`fallback_attempted=False`, `external_engine_invoked=False`, and
`claim_gate_status=fixture_smoke_only`. They do not claim SQL `VALUES`, broad DataFrame runtime,
object-store/lakehouse output, Foundry output, production support, or performance.

Benchmark smoke is local pre-release evidence owned by the benchmark/feature lanes, not
package-channel proof. Its default lanes separate `compatibility_import_certified` from
`prepared_vortex`; the page and transcript must not read those rows as public speed rankings, Spark
replacement evidence, or production readiness.

The provenance step writes SBOM, checksum, workflow policy, and local
`SupplyChainReleaseEvidence` dry-run artifacts under
`target/release-provenance-dry-run/`.

The package-channel validator's strict mode consumes this transcript:

```powershell
python scripts\check_package_channel_readiness.py --require-local-evidence
```

That strict mode requires the generated-source smokes, package/CLI/Python smokes, provenance dry
run, SBOM/checksum manifest generation, `benchmark_smoke_required_for_package_release=false`, and
no-publication safety fields before the package-gate report can pass. Channel-specific package rows
still remain blocked until real channel evidence is attached.

The production-usability gate also consumes this transcript:

```powershell
python scripts\check_production_usability_gate.py
```

That gate requires the clean venv install, installed-wheel client smoke, CLI status/capabilities
smokes, local Python example smoke, generated-source output smokes, provenance dry run,
`benchmark_smoke_required_for_package_release=false`, and no-publication/no-fallback fields before
it can pass. It still keeps
`public_release_claim_allowed=false` and `public_package_claim_allowed=false`.

## Relationship To First 10 Minutes

The dry run is the source-mode version of the public first-10-minutes path. It
uses local build artifacts until channel publication and install proof exist.
Once release artifacts exist, the same proof should install the tagged wheel,
CLI binary, or channel package instead of source-built artifacts.
