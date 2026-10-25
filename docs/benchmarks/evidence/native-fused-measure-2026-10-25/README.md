<!-- SPDX-License-Identifier: Apache-2.0 -->

# Portable computed-measure evidence

The [decision](../../native-fused-measure-2026-10-25.md) retains the finite
optimization while complete-package acceptance remains pending.

From the repository root:

```sh
python3 -B docs/benchmarks/evidence/native-fused-measure-2026-10-25/verify.py
```

Verification uses only `verify.py`, `manifest.json` and `packet.tar.gz`.
It starts no engine and never executes archived code. It validates archive
membership and hashes, complete input/result witnesses, ordered values, schemas,
resource/source closure, failed attempts, actual encoding observations and both
timing cohorts. Historical absolute paths are identity keys, not files to open.
The copied packet passes with access to original local files denied.

`portable-validation.json` records that isolation check and 32 rejected corruption
controls. `historical_validation_driver.py` is the exact completed validation
driver; its original paths must be rebound for a new validation run.
`cleanup.json` records retirement of completed executables and finite native
fixtures after proof. Original results, build/source identities, input-generation
harness and failure history remain in the archive. Historical execution drivers
must not be rerun against their completed output paths; a new engine experiment
requires a new identity and the original storage/concurrency guards.
