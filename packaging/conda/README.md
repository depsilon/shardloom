# ShardLoom Conda Packaging Scaffolds

This directory contains local Conda recipe scaffolds for CG-20 distribution
readiness and the local packaging foundation for
[installed native ShardLoom in Foundry](../../docs/architecture/native-foundry-integration-2026-10-25.md).
They document the intended package split and native package recipe for future
local or private-artifact packaging builds, but they do not publish packages,
create a conda-forge feedstock, or establish compatibility with an external
Foundry Linux environment.

Package recipes:

- `shardloom-native`: platform-specific (`cp310-abi3`, non-`noarch`) Python
  runtime package bundling `shardloom._native`, the native release CLI, and the
  optional `shardloom.foundry` transform adapter, verified via import and native
  execution outside the source checkout.
- `shardloom-cli`: platform-specific Rust CLI binary package.
- `shardloom-python`: pure Python wrapper package with `noarch: python` for
  CLI/wrapper environments without bundled native compilation.
- `shardloom`: optional metapackage that depends on `shardloom-cli` and
  `shardloom-python` for the split wrapper install path.

External engines such as Spark, DataFusion, Polars, DuckDB, pandas, and Dask
remain optional benchmark or correctness baselines only. They are not runtime
dependencies of any package here and must not become runtime fallback engines.

These recipes are aligned to the current workspace version (`1.1.0`) and
intentionally use a local source path. A public feedstock or live Foundry
Artifact Repository pass must replace that source with an approved tagged
archive/private artifact and verified hash, verify target Linux libc/Python/CPU
compatibility and PyArrow Arrow interchange directly in the target build and
execution environments, review third-party license metadata, and receive explicit
human release approval.

`shardloom release-plan --format json` exposes the
`shardloom.conda_build_install_certification.v1` report so package consumers
can distinguish scaffolded local recipes from clean Conda build/install
certification. The certification remains closed until tagged archive, source
hash, version alignment, provenance attestation, and human approval gates pass.

Local packaging smoke, when Conda build tooling is available:

```powershell
conda build packaging/conda/shardloom-native
conda build packaging/conda/shardloom-cli
conda build packaging/conda/shardloom-python
conda build packaging/conda/shardloom
```

No package publication is authorized by this directory.
