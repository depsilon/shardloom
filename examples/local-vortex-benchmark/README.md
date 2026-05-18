<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local Vortex Benchmark Smoke

Run a small ShardLoom-only local taxonomy benchmark:

```powershell
python examples\local-vortex-benchmark\run.py --repo-root .
```

The script writes `target/shardloom-local-vortex-benchmark-smoke.json`.
It does not install external baseline engines and does not publish results.
By default it runs `shardloom` plus `shardloom-prepared-vortex` so the
compatibility-import certification lane and the current prepared/native
runtime-development lane are visible separately. The rows are local
technical-preview evidence, not a performance, Spark-replacement, or production
claim.

Files in this example:

- `environment.yml`: minimal local benchmark smoke environment shape.
- `fixtures/benchmark-request.json`: input fixture for the smoke parameters.
- `expected-output.json`: expected artifact fields.
- `expected-certificate-fields.json`: expected certificate/evidence fields.
- `known-limitations.md`: current boundaries and non-goals.
