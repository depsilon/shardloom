<!-- SPDX-License-Identifier: Apache-2.0 -->

# Regional aggregate experiment evidence

The exact regional scalar aggregate candidate is **retained locally** at
`c9baefd00b20ef1ee1aec3cd49c72b07f762d77d`. Run `python3 -I -B verify.py`
with Python 3.13 or newer from this directory. The verifier checks the packet
and closeout supplement, replays all 1,974 complete results and 40 lifecycle
operations, and recomputes the frozen decision without executing a query or
loading an engine.

The [comparison](../../native-regional-aggregates-2026-11-14.md) and
[architecture disposition](../../../architecture/native-regional-aggregates-2026-11-14.md)
record the admission and remaining scope. Complete fresh engine time decreases
from 14.828472105 to 13.836163897 seconds; the separate prepared lifecycle
total decreases from 0.399843288 to 0.221834044 seconds. All 429 individual
fresh regressions remain. All ten strict requested-read witnesses and 350
regional coverage checks pass. This is neither a Full43 nor a full10k result.

`packet/` preserves the original eight archive parts, exact member manifest,
standalone verifier, complete raw ledgers, independent references, exact
candidate sources and patch, validation logs and failures, and 34 saved-evidence
negative controls. The executed build predates the source-preservation commit;
all 5,745 captured source files were checked byte-for-byte against that commit.
Large input, environment and runtime payloads remain identity-only external
artifacts; replay neither reopens them nor regenerates reference data.

`supplement/` binds completed packaging and all 36 packet controls, including
relocated replay with original source/runtime/evidence paths forbidden. The
initial packaging failure used the predecessor's outer manifest instead of its
inner portable manifest; that failed receipt and log remain. The earlier
derived-replay filename collision and correction are inside the original packet;
raw results, references and frozen requirements were unchanged, with no extra
query executions.

Constant-key grouping, the broader optimization/metadata queue, item 19/20 and
competitive gates remain open. This local acceptance does not merge a PR,
publish packages, authorize a release, or change writer defaults.
