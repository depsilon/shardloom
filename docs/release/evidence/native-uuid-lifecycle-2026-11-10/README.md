<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native UUID lifecycle evidence

The [UUID contract](../../../architecture/native-uuid-lifecycle-2026-11-10.md)
covers canonical unrestricted UUID values through shared native keys and typed
payloads, nested/dictionary fields, actual native spill, bounded results and
fresh-process Vortex/Arrow IPC/Parquet reopening. The pinned Vortex UUID provider
owns representation conversion; no external query engine executes residual work.

Local source-bound acceptance passes. The [acceptance record](acceptance.json)
binds 15 verification roles and 5,845 archived members. The
[sealed controls](corruption-controls.json) record two successful original/
relocated replays and 21 rejected intentional corruptions. Hosted integration
and artifact retirement remain separate gates.

The observed test selection includes 3,322 default and 7,052 native Rust tests,
seven UUID regressions, 778 Arrow-provider tests, 19 selected public/schema tests
and 431 isolated installed-wheel tests. All executed tests pass; the native and
provider suites retain 37 and one existing opt-in ignores respectively. The
public and installed selections have zero skips. The separate Python core run
reports 868 runs and 62 optional/environment skip records from 1,291 declared
cases. Formatting, strict lint, seven feature configurations, both supported-
Rust checks, fresh release packaging and uninstall also pass.

Preparation observations retain their original labels, failures and before/after
source inventories. Final acceptance verifies exact relevant-source, lockfile,
tool, runtime and wheel identities, the completed preceding Variant closeout,
an unchanged-tree base adoption, and fresh documentation/contract checks. Only
named status prose may differ at reused engine gates; production source is not
exempt. The standalone Arrow provider uses its unchanged complete vendor/lock/
Cargo/CI graph. Complete replay and intentional corruption controls pass without
executing native queries again. All 15 original failed/partial records remain,
including the initial control driver's incorrect provider-module log matcher.
The correction, original partial controls and complete replacement run remain
distinct observations. The preceding Variant closeout includes its exact merge,
58 PR checks, 55 main checks and verified retirement with replay afterward.

Run complete source-bound replay from the repository root:

```bash
python3 -I -B docs/release/evidence/native-uuid-lifecycle-2026-11-10/verify.py --repo-root . --require-controls
```

For source-only replay, copy `acceptance.json`, `verification.tar.xz`, `verify.py`
and `corruption-controls.json` together and run `python3 -I -B verify.py
--require-controls`. It needs no installed native engine, build cache or original
checkout. The archive contains verification inputs and observations, not a
redistributable runtime or wheel.

UUID parsing, generation, arithmetic, cross-domain conversion and version-
restricted declarations remain unsupported. Logical depth 24 and wrapper depth
64 are unchanged. Variant field expressions, useful spatial support and deep
traversal remain required item-19 work. This packet does not establish a
performance result, package publication, production readiness, whole item 19,
item 20, RFC 0044 or CG-1 through CG-23 completion.
