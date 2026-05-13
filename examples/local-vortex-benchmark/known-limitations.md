<!-- SPDX-License-Identifier: Apache-2.0 -->

# Known Limitations

- The default example is a small local CSV smoke, not a public benchmark claim.
- It does not install or run pandas, Polars, DuckDB, DataFusion, Spark, Dask, or Velox.
- It does not prove object-store, table/catalog, SQL, DataFrame, live, hybrid, or Foundry support.
- One-iteration smoke output is useful proof-of-use evidence but is not claim-grade benchmark
  evidence.
- Comparative baseline engines are benchmark-only optional dependencies and must never execute
  unsupported ShardLoom work as fallback.
