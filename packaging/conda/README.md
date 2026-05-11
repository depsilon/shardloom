# ShardLoom Conda Packaging Scaffolds

This directory contains local Conda recipe scaffolds for CG-20 distribution
readiness. They document the intended package split and can be used for future
local packaging experiments, but they do not publish packages or create a
conda-forge feedstock.

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

Local packaging smoke, when Conda build tooling is available:

```powershell
conda build packaging/conda/shardloom-cli
conda build packaging/conda/shardloom-python
conda build packaging/conda/shardloom
```

No package publication is authorized by this directory.
