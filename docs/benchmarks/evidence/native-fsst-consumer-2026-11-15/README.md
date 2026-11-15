<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native FSST prefilter evidence

The finite candidate is **retained locally** at
`45be8e0bd1d9cef920c88fa9bf91811deade41c4`. Run `python3 -I -B verify.py` with
Python 3.13 or newer. Complete portable replay checks all 18,080 saved fresh
results and 1,080 prepared operations, independent values/schemas, native
certificates, resource policy, exact source and the frozen decision. It executes
no engine or query. Native binaries, inputs and the original consumer environment
are identity-only exclusions; their payloads need not exist for replay.

The [comparison](../../native-fsst-consumer-2026-11-15.md) and
[architecture contract](../../../architecture/native-fsst-consumer-2026-11-15.md)
define the boundary. Complete fresh engine time is 682.167470629 -> 663.612327731
seconds; separate prepared engine time is 0.520543159 -> 0.511136028 seconds.
All 1,759 individual fresh regressions remain in the decision. A reused baseline
and uncontrolled host/cache state prevent causal attribution of the total gain.
This is the frozen 9,040-declaration population, not Full43 or a complete 10k pass.

Each candidate request executes once. All 9,040 baseline results and 540 baseline
prepared calls are reused with exact worker/request/input/reference/runtime
bindings. The candidate executes 270 prepared plans with two calls each. There
are no warmups, repeated successful observations, cache changes or timing-based
selection. All active query stages retain the original lightweight supervision.

The conservative necessary-byte filter stays inside the admitted positive FSST
source LIKE route. A rejection can skip matcher construction or exact row work;
every surviving candidate still uses the original exact matcher. Prefix, negated,
unsupported and shared projection routes preserve their existing behavior.
The separate untimed diagnostic replays 864 physical/shared declarations and
378 complete native-core requests against 288 nullable Boolean vectors. Its
session observer measures logical calls/rows, not decoded bytes, and compiles
out of release builds. Per-request release prefilter counters were not measured.
Existing provider scratch exclusions remain; full-provider allocation accounting
is not claimed. No writer default, new scalar bridge or fallback engine is added.

`packet/` preserves the nine original archive parts, all 6,363 archived members,
21 additive repair members, source/patch, validation and failed attempts. The
first relocated replay exposed two missing shared validators. Their exact bytes
are now bound through the original baseline protocol to the accepted dictionary
and source-domain closeouts, controls and manifests. All 62 local imports pass
with original paths inaccessible. Original archive bytes and measurements are
unchanged; the failed replay remains recorded.

`supplement/` binds the completed portable replay, 57 corruption controls and
eight additional repair-provenance controls. Twelve prospective synthetic
acceptance controls are in the original archive. Required checks passed: 3,344
default tests, 7,266 native tests, 113 provider tests, 455 installed-wheel tests,
strict lint, feature boundaries and supported-Rust checks. The 41 native and one
provider intentional ignores are explicit. The exact measured source was built
from parent `873e60bca` plus its recorded source changes; the later preservation
commit is not represented as the build revision.

The initial packaging storage refusal is preserved. Verified retirement removed
18,398 idle regenerable debug files after repeated identities and closed-handle
checks. Measured inputs, runtimes, tested wheel and release cache were unchanged.
The original inventory comparison stop is also retained. No storage limit changed.

Exact-table matcher lifetime, broader I attribution/batching, required spatial
execution, optional spatial summaries, codec/fusion work, item 20, RFC 0044 and
CG-1 through CG-23 remain open. This evidence implies no merge or release.
