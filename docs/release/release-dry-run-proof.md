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
- installs the local wheel with `pip --no-index --find-links python/dist`
- resolves the built CLI through `SHARDLOOM_BIN`
- imports `shardloom` from the installed wheel
- runs `ShardLoomClient.from_env().smoke_check()`
- runs `client.capabilities()`
- runs `shardloom status --format json`
- runs `shardloom capabilities --format json`
- runs `examples/local-python-smoke/run.py`
- runs `examples/local-vortex-benchmark/run.py`

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

The clean venv proof installs only ShardLoom's local wheel. Benchmark comparison
engines remain optional benchmark/dev dependencies and are not installed by this
proof.

## Relationship To First 10 Minutes

The dry run is the source-mode version of the public first-10-minutes path. It
uses local build artifacts because no public package publication is authorized
yet. Once release artifacts exist, the same proof should install the tagged
wheel, CLI binary, or Conda packages instead of source-built artifacts.
