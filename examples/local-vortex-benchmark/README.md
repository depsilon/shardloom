<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local Vortex Benchmark Smoke

Run a small ShardLoom-only local taxonomy benchmark:

```powershell
python examples\local-vortex-benchmark\run.py --repo-root .
```

The script writes into an isolated per-run directory under
`target/local-vortex-benchmark/<run-id>/`. Use `--run-id local-smoke` when you
want a stable local path such as
`target/local-vortex-benchmark/local-smoke/smoke.json`.
It does not install external baseline engines and does not publish results.
By default it runs the internal `shardloom` plus `shardloom-prepared-vortex`
engine IDs, presented publicly as ShardLoom Cold Certified Route and ShardLoom
Warm Prepared Query so their start states are visible separately. The rows are
local technical-preview evidence, not a performance, Spark-replacement, or
production claim.

The wrapper always passes a run-scoped `--data-dir` to the inner benchmark
harness and holds a per-run lock while the tiny dataset is regenerated. That
keeps overlapping local smoke or release dry-run attempts from deleting or
rewriting the same generated data path.

Files in this example:

- `environment.yml`: minimal local benchmark smoke environment shape.
- `fixtures/benchmark-request.json`: input fixture for the smoke parameters.
- `expected-output.json`: expected artifact fields.
- `expected-certificate-fields.json`: expected certificate/evidence fields.
- `known-limitations.md`: current boundaries and non-goals.
