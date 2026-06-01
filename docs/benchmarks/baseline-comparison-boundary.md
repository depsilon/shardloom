<!-- SPDX-License-Identifier: Apache-2.0 -->

# Baseline Comparison Boundary

ShardLoom can compare against local engines in benchmark/dev environments, but
those engines are never ShardLoom runtime dependencies and never execute
unsupported ShardLoom work as fallback.

## Runtime Install Path

The core install path is ShardLoom CLI plus the pure Python wrapper. It must not
install Spark, DataFusion, DuckDB, Polars, pandas, Dask, Velox, or similar
systems as runtime dependencies.

## Benchmark Extras

Optional benchmark environments may install comparison engines to populate
external baseline rows. Those rows must be labeled:

```text
external_baseline_only
route_runtime_status=external_baseline_only
```

They may serve as timing comparisons or correctness oracles, but they are not
ShardLoom execution, fallback execution, or evidence that ShardLoom supports an
unsupported scenario.

## Coverage Rows

Unsupported ShardLoom scenarios should produce deterministic coverage rows:

```text
unsupported
blocked
fallback_attempted=false
external_engine_invoked=false
```

Successful ShardLoom rows still need correctness, benchmark, certificate,
Native I/O, materialization/decode, and no-fallback evidence before they can be
claim-grade.

Benchmark pages must not collapse external unsupported rows into ShardLoom
runtime gaps. A DataFusion, pandas, Polars, DuckDB, Dask, Spark, or other
external baseline row can be unsupported because the baseline engine cannot run
that scenario. That should be reported as an external baseline limitation, not
as ShardLoom unsupported runtime surface. Current route-aware benchmark output
uses separate counts such as:

```text
ShardLoom unsupported rows: 0
External baseline unsupported rows: 6
```
