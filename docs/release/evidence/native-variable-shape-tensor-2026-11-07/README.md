<!-- SPDX-License-Identifier: Apache-2.0 -->

# Variable-shape tensor lifecycle acceptance

This immutable local acceptance records checked row-specific tensor shapes and
native lifecycle correctness at source base `740f682eb89191bdd5b85caec96cbb2e2c9abea6`.
Hosted review and integration of this delivery are pending at this observation.
It does not complete item 19, change independent CG gates, claim a performance
gain or publish a package.

`acceptance.json` binds 162 changed source files,
13 accepted check stages, the final CLI/library/wheel and
all 497 members of `verification.tar.xz`. Every final
runtime source matches the tested build. The archive includes source snapshots,
all 23 original failed observations, their saved
logs or explicit missing-log statements, provider archives and provenance, and
the preceding fixed-tensor integration. PR #1661 passed all 58 checks; its exact
signed merge passed all 55 required main checks and completed safe retirement.

Default/native Rust suites pass 3,319/6,972 cases;
37 existing opt-in native tests remain ignored. All seven
feature configurations and strict lint checks pass. The five format providers
pass all ten test/lint commands and 1,606 tests,
with one archive-disabled upstream Map test. Its deliberate failing diagnostic
is retained separately. The root dependency versions are unchanged; two existing
Arrow packages now use narrowly patched, provenance-checked local sources.

The public Python shard reports 845
test runs and 51 optional/environment skip
records, separately from 1210
discovered cases. The clean installed package passes all
361 tests without skips, including all 11 new
variable-tensor and 19 context tests, then uninstalls. The published example
runs through native persistence and checks both complete reopened rows.
Its original colon-delimited predicate was corrected to the SQL expression
`id >= 2`; the exact one-expression change and both outcomes are preserved.
The engine and Python runtime sources are unchanged by that example correction.

Native, IPC and Parquet lifecycle checks preserve supported component bits,
metadata, nullability, shape/data alignment and ownership through ordinary
relational payloads, bounded streams, native spill and all five write interfaces.
Controls cover malformed visible values, hidden/discarded children, cancellation,
source mutation, denied grants, failed publication and last-owner release.

PyArrow 25.0.1 has two explicit Parquet limitations: its default reader drops the
tensor extension, and it rejects fixed-size lists beneath null parents. Typed
independent present-parent reads use the actual file's `ARROW:schema`; the
default reader's complete present-parent values are also checked. Complete native
Parquet equality passes separately. The original reader failure is preserved.

From the repository root, replay with Python 3.11 or later:

```sh
python3 docs/release/evidence/native-variable-shape-tensor-2026-11-07/verify.py --repo-root .
```

Copy this directory elsewhere and run `python3 verify.py` for standalone replay.
Verification hashes and inspects saved evidence without builds, installation,
query reruns or network access. Original and relocated replay pass; nine
controlled corruptions are rejected. `corruption-controls.json` preserves exact
mutation checks, driver source, observations and logs.

See the [implementation contract](../../../architecture/native-variable-tensor-lifecycle-2026-11-07.md)
and [user guide](../../../reference/variable-shape-tensors.md) for the supported
types and boundaries. Tensor arithmetic and tensor key roles remain unsupported;
text, Avro and ORC outputs refuse metadata loss. Sparse, quantized and lower-
precision representations and exact vector expressions retain their queue order.
