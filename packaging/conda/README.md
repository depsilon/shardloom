# ShardLoom Conda Packaging Scaffolds

This directory contains local Conda recipe scaffolds for CG-20 distribution
readiness. They document the intended package split and can be used for future
local packaging experiments, but they do not publish packages or create a
conda-forge feedstock.

The October 25 [Foundry integration handoff](../../docs/architecture/native-foundry-integration-2026-10-25.md)
queues replacement of this older CLI-wrapper delivery with the current native
Python runtime and actual target-environment installation/execution proof.
These version-0.1.0 local recipes are not evidence that current native ShardLoom
is available through Conda or compatible with a Foundry Linux environment.

Package split:

- `shardloom-cli`: platform-specific Rust CLI binary package.
- `shardloom-python`: pure Python wrapper package with `noarch: python`.
- `shardloom`: optional metapackage that depends on both packages for a
  one-command install path.

The split keeps the Python wrapper importable without native bindings while
allowing the Rust CLI binary to be built per platform. External engines such as
Spark, DataFusion, Polars, DuckDB, pandas, and Dask remain optional benchmark
or correctness baselines only. They are not dependencies of these packages and
must not become runtime fallback engines.

These recipes intentionally use a local source path. A public feedstock pass
must replace that source with a tagged archive and verified hash, align Cargo,
Python, and Conda versions, review third-party license metadata, and receive
explicit human release approval.

`shardloom release-plan --format json` exposes the
`shardloom.conda_build_install_certification.v1` report so package consumers
can distinguish scaffolded local recipes from clean Conda build/install
certification. The certification remains closed until tagged archive, source
hash, version alignment, provenance attestation, and human approval gates pass.

Local packaging smoke, when Conda build tooling is available:

```powershell
conda build packaging/conda/shardloom-cli
conda build packaging/conda/shardloom-python
conda build packaging/conda/shardloom
```

No package publication is authorized by this directory.
