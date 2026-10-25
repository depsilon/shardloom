<!-- SPDX-License-Identifier: Apache-2.0 -->

# Warm-source investigation evidence

The candidate was implemented, measured, rejected by its frozen gates, and
removed. See the [decision](../../../architecture/native-warm-source-reuse-2026-10-24.md).
The archive contains complete observations, not retained runtime payloads.

From the repository root, with Python 3.10 or newer:

```sh
python -B docs/benchmarks/evidence/native-warm-source-reuse-2026-10-25/verify_packet.py --self-test
```

At the recorded closeout revision, also verify the restored runtime and
reconstruct the rejected patch using its base commit and a private Git index:

```sh
python -B docs/benchmarks/evidence/native-warm-source-reuse-2026-10-25/verify_packet.py \
  --current-source --reconstruct-source --self-test
```

The second command requires Git history containing
`82d9732eb7652671a01b0f223cf62c63f01ffe47`. It leaves the real index, checkout and
branch unchanged. Replay uses the hash-bound result helpers in this checkout;
it does not execute archived helpers or native query code, load native libraries,
read the original dataset, contact a service, or rerun timing observations.

Coverage includes 93 baseline and 93 candidate calls, plus the earlier 129-call
accepted-runtime characterization: 315 complete executions and 3,285 result
rows in total. It checks exact call identities, independent values, native
schemas, fresh completion, source counters, final memory release, engine-clock
linkage, all frozen family/control calculations, 19 build/test receipts,
installed-wheel identity, exact patch reconstruction and source restoration.
Ten corruption controls exercise missing/duplicate calls, wrong values/schema,
missing execution, incorrect source counts/engine clocks, retained memory,
forged acceptance and changed frozen rules.

`manifest.json` binds every original path and hash to a content-addressed
member of `evidence.tar.xz`. The original selection inventory and explicit
additions prevent silent loss of earlier failures. Duplicate content is stored
once. Raw process observations, timing fields, helper versions and historical
absolute paths remain unchanged inside the archive. Paths are identifiers for
lookup, not extraction destinations.

The earlier adjacent-triple result belongs to the accepted provider runtime.
It is not evidence of a gain from the rejected candidate. The candidate fails
both the benefit and control requirements despite correct results and fewer
source opens. No candidate Full43 or broad final native acceptance is claimed.

The runtime sources can be regenerated from Git plus the preserved patch.
Executables, compiler caches, installation environments and source datasets
are intentionally excluded; build/protocol/install records retain their exact
identities. Cleanup records separately identify retired generated artifacts.
