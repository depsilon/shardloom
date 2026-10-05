<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local Vortex Benchmark Example

This example is a thin command-line wrapper around
[`benchmarks/traditional_analytics/run.py`](../../benchmarks/traditional_analytics/run.py).
It runs the `selective filter` workload with one ShardLoom candidate and pandas as an
independent correctness reference. It does not build ShardLoom or install dependencies.

```powershell
python examples\local-vortex-benchmark\run.py `
  --shardloom-binary target\debug\shardloom `
  --workspace "$HOME\LocalData\shardloom\traditional-benchmarks"
```

The workspace must be a local-only location outside synced folders. The wrapper defaults to 64
rows, 8 dimension rows, one repeat, CSV input, raw input state, and collected output. Use
`--help` for the supported options. Fixture generation, resource checks, isolation, execution,
and result handling belong to the shared harness.

This invocation is local comparison evidence. It is not a performance claim, public benchmark
publication, complete benchmark acceptance review, or production-support claim.
Unsupported ShardLoom work is reported and is never executed by pandas.

The JSON files beside this README describe the example request and the result/claim posture. They
are declarative metadata, not captured runtime output or certificates.
