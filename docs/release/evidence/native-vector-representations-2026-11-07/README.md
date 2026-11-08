<!-- SPDX-License-Identifier: Apache-2.0 -->

# Sparse and quantized vector lifecycle evidence

Local acceptance for the [November 7 contract](../../../architecture/native-vector-representations-2026-11-07.md).
The packet binds tested source, runtime and wheel identities to native workflows.
It contains no package publication or benchmark claim. Hosted integration and
subsequent artifact retention have separate receipts; this frozen local packet
does not retroactively claim those later outcomes.

| Check | Accepted observation |
| --- | --- |
| Default Rust workspace | 3,321 passed; 0 failed; 0 ignored |
| Native Rust workspace | 6,988 passed; 0 failed; 37 existing opt-in tests ignored |
| Formatting and strict lint | Default and native configurations passed |
| Feature configurations | All seven saved configurations passed |
| Python core | 1247 discovered; 857 run; 56 skip records; zero failures/errors |
| Clean installed native wheel | 386 passed; zero failures, errors or skips; isolated load and uninstall passed |
| Vector integration | All 11 tests passed against the frozen runtime and again in the installed wheel |
| Existing rich-type regressions | All 37 fixed/variable tensor and Float16 Python methods passed in the installed wheel |
| Runnable example | Both complete rows reopened with sparse coordinate order/duplicates, signed zero and integer extremes preserved |
| Portable evidence | Original and relocated verification passed; all 9 corruption controls rejected |

The frozen reference contains six complete rows and eleven mixed scalar, dense,
tensor, sparse and quantized fields, including nested containers and sliced
inputs. Carriage is checked across all eight integer coordinate/code widths and
all three floating value/scale widths. Both sparse index policies and both
quantized parameter modes retain exact metadata, nullability and values.

Lifecycle checks cover all five public interfaces, duplicate scalar joins,
selection, forced native spill, bounded streaming, retained results and
separate-process reopen after source removal. Independent Arrow IPC and Parquet
readback checks complete values and logical schemas. Controls distinguish absent
parents from visible malformed children, reject invalid scales and metadata,
and exercise memory denial/shrink, cancellation, source changes and failed
publication. Prior tensor and exhaustive Float16 regression tests remain present.

The archive preserves 10 original failed observations,
including two independent preparation attempts and stopped sequence wrappers,
with their available logs and dispositions. Preparation includes 192 independent
format observations and the frozen mixed reference; these are explicitly
distinguished from native acceptance. The preceding Float16 closeout binds its
exact merge, 58 PR checks, 55 required main checks and verified cleanup. Five
independent dependency proposals remained pending at that historical closeout.
Dependency versions and provider patches are unchanged by this unit.

## Offline replay

Run from this directory after relocation:

```sh
python3 -I verify.py
```

From a matching source checkout, also compare source and dependency contracts:

```sh
python3 -I docs/release/evidence/native-vector-representations-2026-11-07/verify.py --repo-root .
```

Replay checks saved evidence without rebuilding, installing or executing queries.
The verifier checks archive membership and identities, commands and source
snapshots, complete test-method coverage, logs, runtime identities, every original
failure and preceding integration receipts. `corruption-controls.json` preserves
exact mutation verification and outputs, including an omitted width-coverage
test with adjusted counts and log hashes.

## Boundaries

Vortex remains native persistence. Text, Avro and ORC refuse these representations
before producer or publication work. Rich dictionary intake remains unadmitted.
List and LargeList input map to native List; export keeps the existing checked
Int32 offset limit. Generated list-child names are not native type identity.

These are payload representations. Vector arithmetic, comparison keys,
quantization/dequantization, ANN, packed sub-byte schemes and quantized tensors
remain separate work. Selected grants do not account for every upstream provider
allocation and do not establish an RSS bound or zero-decode claim. There is no
external-engine fallback. This packet does not close all of item 19, item 20,
RFC 0044 or CG-1 through CG-23.
