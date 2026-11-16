# Native optimization propagation evidence

Keep `selection/` and `propagation/` together. The latter is the unchanged
original propagation packet; the former contains the focused evidence delta and
its repaired verifier. Both may be relocated away from the original checkout.

With Python 3.13 or newer, run from this directory:

```sh
python3 -I -B selection/verify.py --baseline-packet propagation --output /path/to/new-result.json
```

The output path must not already exist. Extraction requires the unchanged
12-GiB free-space floor plus the archive contents. The verifier uses the saved
complete values, schemas, references, resource/work records, clocks, source and
check evidence. It executes no queries and refuses original path reads,
native-engine imports, subprocesses and network access.

It reconstructs all 23,158 original fresh observations and 1,080 prepared
operations, then reuses the exact selected baseline subset and verifies 2,190
new fresh observations and 540 new prepared operations. The original decision
is **not admitted**; the focused performance decision is **drop**. All focused
required work witnesses pass. See [portable-result.json](portable-result.json).

[integration-disposition.json](integration-disposition.json) separately records
the agent's integration judgment within the maintainer's explicit instruction
to retain useful behavior, fix the demonstrated propagation gap and prepare a
coherent PR. It does not turn either failed performance gate into a pass.
[cost-inventory.md](cost-inventory.md) preserves complete partitions and the
largest individual changes; its independent reconstruction is recorded in
[cost-primary-verification.json](cost-primary-verification.json).

The original failed verifier, metadata-only hardlink relocation, corrected
identity checks, all failed tests and storage-admission observations remain
archived. The final packet relocation uses byte-verified copies. Native
executables, physical input payloads and execution environments are identity-only
exclusions; candidate source containers are excluded only when all source
members are already archived. No release or whole-engine speedup is claimed.

[copy-verification.json](copy-verification.json) binds the exact packet and
closeout files copied into this checkout. The [report](../../native-optimization-propagation-2026-11-15.md)
describes source scope, results, useful behavior and remaining work.
