<!-- SPDX-License-Identifier: Apache-2.0 -->

# Float16 and vector-collection lifecycle evidence

Local acceptance for the [November 7 contract](../../../architecture/native-float16-lifecycle-2026-11-07.md).
The packet binds the tested source, runtime and wheel to complete native workflows.
It contains no published package or benchmark claim. Hosted integration and
subsequent artifact retention have separate receipts; this frozen local packet
does not retroactively claim those later outcomes.

| Check | Accepted observation |
| --- | --- |
| Default Rust workspace | 3,321 passed; 0 failed; 0 ignored |
| Native Rust workspace | 6,976 passed; 0 failed; 37 existing opt-in tests ignored |
| Formatting and strict lint | Default and native configurations passed |
| Feature configurations | All seven saved configurations passed |
| Python core | 1229 discovered; 850 run; 54 skip records; zero failures/errors |
| Clean installed native wheel | 375 passed; zero failures, errors or skips; isolated load and uninstall passed |
| Float16 integration | All 14 tests passed against the frozen runtime and again in the installed wheel |
| Runnable example | Both complete rows reopened; negative-zero representation preserved |
| Portable evidence | Original and relocated verification passed; all 9 corruption controls rejected |

The representation checks exhaust all 65,536 binary16 patterns, including NaN
sign, signaling state and payload, through core conversion and native
Vortex/Arrow IPC/Parquet persistence. The lifecycle checks carry mixed scalar,
nested, embedding, vector-collection and tensor columns through all five public
interfaces, duplicate joins, selection, forced native spill, bounded streaming,
retained results and separate-process reopen after source removal. Selected
strict-policy validation, exact thresholds, memory denial/shrink, cancellation,
source changes and failed publication have explicit controls.

The archive preserves 20 original failed or interrupted
top-level observations, including sequence wrappers, with their available logs and
dispositions. It also binds the preceding variable-tensor closeout to its exact
merged commit, all 58 PR checks, all 55 required main checks and verified cleanup.
Dependency versions and provider patches are unchanged by this unit.

## Offline replay

Run from this directory after relocation:

```sh
python3 -I verify.py
```

From a matching source checkout, also compare the saved code and dependency
contracts with that checkout:

```sh
python3 -I docs/release/evidence/native-float16-lifecycle-2026-11-07/verify.py --repo-root .
```

Replay checks saved evidence without rebuilding, installing a package or
executing queries. The verifier checks archive membership and identities,
commands and source snapshots, complete test-method coverage, logs, native
artifact identities, every original failure, and preceding integration receipts.
`corruption-controls.json` preserves exact mutation verification and control
outputs, including a removed exhaustive test with adjusted counts and log hashes.

## Boundaries

Vortex is the native persistence target. Avro and ORC refuse scalar or nested
Float16 before opening a streaming producer. Optional legacy scalar casts and
encoded reductions retain their prior admission limits. PyArrow 25.0.1 cannot
independently read fixed-size lists beneath absent parents in Parquet; the
capable present-parent subset and all scalar patterns are independently checked,
alongside complete native reopened results. Sparse/quantized interpretation and
exact vector expressions remain separate work. This packet does not close item
19, item 20, RFC 0044 or CG-1 through CG-23.
