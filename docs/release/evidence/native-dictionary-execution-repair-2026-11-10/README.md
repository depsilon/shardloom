<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dictionary execution repair evidence

The repair passes all 96 requests that failed in the original current-runtime
observation and all 421 predefined passing controls. Independent verification
checks complete answers, native certificates and engine clocks. The original
10,000-case observation remains 9,904 passed, 76 crashed and 20 CLI harness
errors, with no valid complete engine-time total.

The packet binds the fresh release CLI/library to exact source inventories,
passing default/native checks and the original failing regression. Default
tests pass 3,323 cases; native tests pass 7,068 with 37 existing opt-in ignores.
The three new dictionary regressions cover actual suspended execution, native
values, allocation denial and last-buffer ownership.

The separate 517-case cohort was selected before candidate execution without
using timing results. Each request ran once in isolation with the original
worker, client, independent expectations and safety limits. Full pre/post-run
hashes and process cleanup passed. Original failed checks and all original
10,000 outcomes remain immutable evidence.

`acceptance.json` indexes bounded `verification-*.tar.xz` parts. The parts
contain source/receipt bytes, the original 10,000 saved attempts, the separate
517 attempts, the frozen 65-file comparator/client and independent expected
results. Three supplementary protocol imports use the exact unchanged source
bytes bound by both observation/build inventories. Large logical files are reassembled with their original byte counts
and SHA-256 values. Native binaries, input datasets and compiler output are
represented by identities. Historical earlier-performance receipts remain
historical; replay makes no new attribution claim about them.

Run source-bound verification from a checkout:

```bash
python3 docs/release/evidence/native-dictionary-execution-repair-2026-11-10/verify.py --repo-root . --require-controls
```

For source-only replay, copy this directory and run `python3 verify.py --require-controls` there.
The verifier executes no query, build or external engine. It uses the original
frozen comparator to check complete saved results and clocks for both
observations, recomputes the case selection from original failures and request
shapes, and verifies source/check/runtime bindings. The corruption controls
challenge source changes, missing tests, failed/undrained checks, runtime
mismatches, omitted failures and inappropriate full10k/performance claims.

The [repair contract](../../../architecture/native-dictionary-execution-repair-2026-11-10.md)
records the root cause, provider decision, scope and remaining obligations.
Hosted integration is pending. This packet does not claim a complete current
10k timing result, an optimization win, package publication, or completion of
item 19, item 20, RFC 0044 or CG-1 through CG-23.
