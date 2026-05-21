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
- builds the Python wheel and sdist from `python/`
- creates a clean virtual environment under `target/`
- installs the exact local wheel artifact with `pip --no-index <wheel>`
- resolves the built CLI through `SHARDLOOM_BIN`
- imports `shardloom` from the installed wheel
- runs `ShardLoomClient.from_env().smoke_check()`
- attempts a clean Conda-style proof with `mamba`, `conda`, or `micromamba` when one is available
- runs `client.capabilities()`
- runs `shardloom status --format json`
- runs `shardloom capabilities --format json`
- runs `examples/local-python-smoke/run.py`
- runs a scoped `ctx.from_rows([...]).write(local_jsonl)` generated-source output smoke from the
  clean installed wheel
- runs a scoped `ctx.range(...).write(local_jsonl)` engine-native generated-source output smoke from
  the clean installed wheel
- runs `examples/local-vortex-benchmark/run.py` with the default compatibility-import plus
  `shardloom-prepared-vortex` lanes
- runs `scripts/release_provenance_dry_run.py --skip-build`

The transcript records command return codes, bounded stdout/stderr excerpts,
local wheel path, CLI binary path, clean venv path, and release-safety booleans.

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
provenance_dry_run_performed=true
sbom_checksum_manifest_generated=true
clean_conda_env_install_status=passed | skipped_tool_missing | skipped_by_request | failed
clean_conda_env_install_required=true | false
generated_output_proof_distinct_from_no_dataset_smoke=true
generated_source_user_rows_smoke_performed=true
generated_source_range_smoke_performed=true
prepared_native_benchmark_smoke_performed=true
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

The clean venv proof installs only the exact ShardLoom wheel built during the current dry run.
Benchmark comparison engines remain optional benchmark/dev dependencies and are not installed by
this proof. The local benchmark smoke is launched through the clean venv interpreter so wrapper
import behavior is checked from the installed artifact, not the host Python environment.

The generated-source output smokes are deliberately distinct from no-dataset smoke. They write
local JSONL files under `target/release-dry-run-proof/`, emit
`generated_source_certificate_status=present`,
`output_native_io_certificate_status=certified_local_file_sink`,
`fallback_attempted=False`, `external_engine_invoked=False`, and
`claim_gate_status=fixture_smoke_only`. They do not claim SQL `VALUES`, broad DataFrame runtime,
object-store/lakehouse output, Foundry output, production support, or performance.

The benchmark smoke is local pre-release evidence. Its default lanes separate
`compatibility_import_certified` from `prepared_vortex`; the page and transcript must not read those
rows as public speed rankings, Spark replacement evidence, or production readiness.

The provenance step writes SBOM, checksum, workflow policy, and local
`SupplyChainReleaseEvidence` dry-run artifacts under
`target/release-provenance-dry-run/`.

The package-channel validator's strict mode consumes this transcript:

```powershell
python scripts\check_package_channel_readiness.py --require-local-evidence
```

That strict mode requires the generated-source smokes, prepared/native benchmark smoke,
provenance dry run, SBOM/checksum manifest generation, and no-publication safety fields before the
package-gate report can pass. Channel-specific package rows still remain blocked until real
channel evidence is attached.

## Relationship To First 10 Minutes

The dry run is the source-mode version of the public first-10-minutes path. It
uses local build artifacts because no public package publication is authorized
yet. Once release artifacts exist, the same proof should install the tagged
wheel, CLI binary, or Conda packages instead of source-built artifacts.
