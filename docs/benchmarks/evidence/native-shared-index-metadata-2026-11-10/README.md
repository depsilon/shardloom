<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared native index metadata evidence

The [decision](../../../architecture/native-shared-index-metadata-2026-11-10.md)
retains one frozen candidate after all 2,452 cases pass, with engine time
320.508980218 → 319.977114074 seconds. The baseline comprises 433 exact saved
repair observations and 2,019 newly measured missing cases. This is not a new
full10k result or a statistical confidence claim.

`acceptance.json` inventories the split verification archives, source/runtime
identities, tests, original failures and complete decision. `verify.py` uses
Python's standard library to validate the archived source and independently
replay all 4,904 saved complete results, schemas, no-fallback certificates and
engine clocks with the original frozen request builder and comparator. It
executes no query, compiler, native library or reference engine. Runtime and
input payloads are identified rather than copied into this packet.

From the repository root:

```sh
python3 -B docs/benchmarks/evidence/native-shared-index-metadata-2026-11-10/verify.py --repo-root . --require-controls
```

After copying this directory elsewhere, run `python3 -B verify.py --require-controls`
inside that directory for source-only replay. Source-bound replay additionally
checks the checkout against the exact validated candidate, allowing only the
four declared status documents to advance.

`corruption-controls.json` binds two positive replays and 17 rejected corruptions
to this exact manifest, verifier and archives. `control-observations/` preserves
their raw stdout/stderr. The archived diagnostic verifier separately checks all
22 complete diagnostic requests, eight cyclic calls and 14 counter corruptions.
The ordinary release runtime contains none of those debug counters.

The original invalid 10,000-case observation, the separate repair cohort,
baseline reuse map, all family regressions, tiny-owner cost and failed proof
attempts retain their individual provenance. Hosted integration and final
retirement are separate from this immutable local acceptance packet. No package
publication or installed-wheel claim is made.
