<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local Vortex Benchmark Smoke

Run a small ShardLoom-only local taxonomy benchmark:

```powershell
python examples\local-vortex-benchmark\run.py --repo-root .
```

The script writes `target/shardloom-local-vortex-benchmark-smoke.json`.
It does not install external baseline engines and does not publish results.
