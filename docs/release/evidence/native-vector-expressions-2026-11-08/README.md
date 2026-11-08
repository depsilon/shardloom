<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared exact vector expression evidence

Local acceptance for the [November 8 contract](../../../architecture/native-vector-expressions-2026-11-08.md).
SQL and Python share ten native functions for dense vector arithmetic, selection
and inspection, with sparse/quantized dimension and tensor shape inspection.
The [user guide](../../../reference/vector-expressions.md) documents the supported
types, nulls, embedding identities, floating policies and runnable examples.

| Check | Accepted observation |
| --- | --- |
| Default Rust workspace | 3,322 passed; zero failures or ignores |
| Native Rust workspace | 7,021 passed; zero failures; 37 opt-in ignores |
| Formatting and strict lint | Default and native checks passed |
| Features and supported Rust | Seven feature checks and both Rust 1.96.0 checks passed |
| Selected public Python | 21 passed; zero failures, errors or skips |
| Python core | 1,263 discovered cases; 862 reported runs; 59 optional/environment skip records; zero failures/errors |
| Isolated installed wheel | 397 passed; zero failures, errors or skips; isolated load and uninstall passed |
| Guide examples | Two Python examples and one SQL example passed |
| Portable verification | Original and relocated replay; 15 rejected corruption controls |

The packet binds fifteen validation roles to their complete original source
inventories, exact commands, logs and process-cleanup receipts. Final document
checks and contract tests see the final source. Earlier native checks may differ
only in the named later Python tests, which subsequently pass through public and
installed boundaries, and the three named status documents. Runtime, fixture,
Python package and other compiled inputs must match. The installed wheel contains
the final Python source and the same frozen native library and CLI.

The frozen independent reference includes 270 finite cases with a separate
6,144-digit Decimal crosscheck, 4,918 rounding cases, 85 semantic cases and 19
binding cases. The public numeric matrix checks 6,480 observations. Retrieval
uses an actual 1,536-component constant query, all admitted floating widths,
fixed/variable storage and deterministic score-tie ordering. The archive retains
the test sources and every named public/native vector regression result.

Tests cover null and identity refusals, original floating bits, empty arrays,
resource denial, cancellation, final-owner release, streamed delivery, actual
spill, native write and separate-process reopen. Complete native Parquet and
independent Arrow readbacks are checked. PyArrow 25.0.1's nullable fixed-list
Parquet limitation remains explicit; present-parent Parquet subsets receive
independent PyArrow readback.

All original failed observations are retained with dispositions. They include
the demonstrated flat literal credit-lifetime defect and public raw-value copy
defect, their fixes, and their passing regressions. No failed or incomplete run
is counted as acceptance. The debug-retirement receipt preserves repeated
identities, closed-handle checks and unchanged retained inputs/runtime evidence.
Prior provider PR #1670, exact-main checks, dependency dispositions and retirement
remain bound to their original receipts.

## Offline replay

Use Python 3.11 or newer. After relocating this directory, run:

```sh
python3 -I verify.py --require-controls
```

From the matching checkout, also compare every final source input:

```sh
python3 -I docs/release/evidence/native-vector-expressions-2026-11-08/verify.py --require-controls --repo-root .
```

Replay verifies saved sources, commands, logs, identities and control outcomes.
It does not build, install, execute native queries, access the network or require
the original temporary directories. The controls used actual isolated subprocess
replay of the original and relocated packet. Negative cases reject omitted roles
or tests, failed native/installed acceptance, changed source or runtime, old
build evidence, replaced frozen expectations, omitted original failures,
resealed failed prior-main checks, unsafe archive paths and checksum corruption.
Temporary mutated payloads are removed after each observation; original evidence
remains unchanged. Saved control logs and mutation identities are in
`corruption-controls.json`.

## Boundaries

This is local macOS arm64 installed-package acceptance. Hosted review, PR checks,
merge, exact-main checks and subsequent retention have separate closeout receipts;
this frozen packet does not claim those later outcomes. These functions require
a matching native source build after 1.1.0 and are not in published 1.1.0 wheels.

Sparse/quantized arithmetic, densification, dequantization, ANN, model calls and
implicit multi-vector reduction remain unadmitted. Explicit vector-value,
element and slice results retain raw-value policy through the documented ordinary
projection/filter/range/order paths; arithmetic and sort keys still observe
values, and other operators retain their existing validation contracts. Selected
native-buffer accounting is not a whole-process RSS bound or a blanket
zero-copy, zero-decode or I/O-saving claim. Vortex remains native input and
highest-fidelity output, with no external-engine fallback. No benchmark,
performance superiority, package publication, whole item 19/item 20, RFC 0044
or CG-1 through CG-23 completion is claimed.
