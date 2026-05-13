<!-- SPDX-License-Identifier: Apache-2.0 -->

# SBOM Generation Plan

Status: release-readiness scaffold. No packages, images, tags, or releases are
published by this plan.

## Rust Workspace SBOM

Generate a Rust SBOM from the locked workspace dependency graph before any
release candidate:

```powershell
cargo install cargo-cyclonedx --locked
cargo cyclonedx --workspace --all-features --format json --output-cdx target/sbom/shardloom-rust.cdx.json
```

The SBOM must be paired with `cargo deny check licenses advisories bans sources`
and, when enabled, `cargo audit`.

## Python Wheel And Sdist SBOM

Build Python artifacts first, then generate SBOMs from the built wheel and
source distribution:

```powershell
python -m build python
python -m pip install cyclonedx-bom
python -m cyclonedx_py environment --output-file target/sbom/shardloom-python-env.cdx.json
```

The Python package currently has no runtime dependencies. Any future Python
dependency must be reviewed before it appears in wheel/sdist SBOMs.

## Release Binary SBOM

For release binaries, generate a binary/package SBOM after building the CLI:

```powershell
cargo build --release -p shardloom-cli --bin shardloom
syft target/release/shardloom -o cyclonedx-json=target/sbom/shardloom-cli-binary.cdx.json
```

Windows builds should point Syft at `target/release/shardloom.exe`.

## Optional OCI Image SBOM

If ShardLoom later publishes an OCI image, generate an image SBOM before
publication:

```powershell
syft ghcr.io/depsilon/shardloom:<tag> -o cyclonedx-json=target/sbom/shardloom-oci.cdx.json
```

OCI image publication is not currently authorized. The image SBOM path is a
future gate only.

## Release Gate

A release candidate is SBOM-ready only when Rust workspace, Python artifact, and
release-binary SBOMs exist, are archived with checksums, and are referenced from
release notes. Optional OCI SBOMs are required only if an OCI image is published.
