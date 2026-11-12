<!-- SPDX-License-Identifier: Apache-2.0 -->

# Direct COUNT invalid-baseline checkpoint

This packet preserves investigation B's unadmitted prototype and failed fixed
baseline. Of 2,543 cases, 2,483 pass and 60 retain their original failed statuses.
All 2,258 original controls pass. No candidate benchmark query ran; complete
engine time and a candidate gain/loss comparison are unavailable. B remains
pending. Runtime, tests and CI registration were restored to the accepted source.

See the [architecture record](../../../architecture/native-direct-count-bridge-2026-11-12.md)
for the unsigned SQL spelling and mixed COUNT/SUM schema mismatches, prototype
validation and exact continuation contract. The prior A closeout, original failed
full10k observation and all earlier failures remain distinct evidence.

`acceptance.json` uses `shardloom.direct_count_checkpoint.v1` with disposition
`not_admitted_invalid_baseline`. Identity-checked `verification-*.tar.xz`
archives contain complete saved results, references, request/source proof,
restoration receipts and the reapplicable prototype patch. Archive members are
bounded to 24 MiB, each uncompressed archive to 48 MiB, each compressed archive
below 64 MiB and the logical packet below 512 MiB. Runtime, wheel and input
payloads are not duplicated.

From this directory, with standard-library Python 3.13 and Git available:

```bash
python3 -B verify.py --require-controls
```

For source-bound verification from the repository root:

```bash
python3 -B docs/benchmarks/evidence/native-direct-count-bridge-2026-11-12/verify.py --repo-root . --require-controls
```

The verifier replays all 2,543 saved results and checks all 60 failures without
executing a query, compiler or reference engine. Git applies the preserved patch
only inside a temporary directory to compare every reconstructed prototype byte.
Two positive controls cover the repository and a relocated packet; 33 corruption
controls challenge source, tests, CI, installed proof, diagnostics, complete
results, references, restoration, claims and archive identity. Their original
logs and receipts accompany the packet. Passing this verifier validates the
checkpoint's evidence; it does not turn the failed baseline into a valid result
or complete investigation B. Hosted integration and later retirement receipts
are separate from the immutable local checkpoint.
