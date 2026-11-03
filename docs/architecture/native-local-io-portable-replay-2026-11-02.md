<!-- SPDX-License-Identifier: Apache-2.0 -->

# Saved local I/O matrix replay

Status: saved-evidence acceptance passed November 3. The original r17 campaign
binds the r8 runtime. Its 9,633 completed observations and original timing
dispositions remain immutable. This work runs no engine query.

The existing collection comprises four bounded multipart bundles: metadata,
Arrow result/reference values, original binary outputs and original text
outputs. The maintained replay must verify the collection and declaration
identities, reuse the existing bundle limits, and reject missing, extra,
ambiguous or corrupted members. Original absolute paths are lookup keys inside
the bundles, never instructions to open local files or execute archived code.

Acceptance requires all of the following:

- Reconcile every declared job and ordinal in the five cohorts: 205 smoke,
  4,555 correctness, 400 fidelity, 18 lifecycle and 4,455 timing observations.
  Reconstruct the original combined JSONL streams byte-for-byte where declared.
- Compare complete saved Arrow results against their independent references,
  retaining logical types, metadata, nulls, values and order. Preserve explicit
  string/binary representation normalization and format-specific translations.
- Reopen retained compatibility outputs with independent format readers and
  compare all values. Validate the existing 348 Vortex readbacks against their
  33 saved independent decode exports, bound to original output/reference and
  provider identities. This replays those completed decodes; it does not execute
  an archived Vortex binary or claim a newly independent Vortex decode.
- Check output fanout count/commit evidence and every follow-on result, explicit
  preparation/reuse, separate-process reopening and changed-source refusal.
  Check no-fallback, caller-resource and direct-source evidence at each relevant
  execution. Rich text wire checks must not become typed-roundtrip claims.
- Verify source/runtime/input bindings and original process receipts/logs.
  Replay all three historical 347-case candidates with the maintained complete
  result comparator, keeping their original strict-gain failures and the later
  scoped maintainer tradeoff distinct. Recompute diagnostic timing summaries
  from the saved observations; unavailable clocks stay unavailable.
- Exercise corruption controls and replay from a relocated packet with original
  artifact paths unavailable. Record exact verifier/dependency identities and
  remaining proof boundaries. Original input payloads and runtime executables
  remain hash-bound external objects; they are unnecessary for saved-result replay.

The report establishes saved-evidence completeness for this historical matrix.
Later adaptive/table/preparation acceptance remains separate. It neither grants
performance superiority nor closes optional adapters, remote integration, rich
data lifecycle, competitive gates or package publication.

## Accepted replay

The [portable acceptance](../benchmarks/evidence/native-local-io-portable-2026-11-03/acceptance.json)
records all 9,633 observations, 10,878 complete saved-value comparisons covering
10,779,851 rows, and 2,648 output readbacks. Repeated observations count separately;
the verifier decodes 75 unique Arrow pairs and 398 unique compatibility outputs.
All 348 Vortex observations bind to their 33 previously completed independent
Vortex decodes. The three declared combined JSONL streams reconstruct exactly.
All three historical 347-case candidates replay their complete 12,565 result rows
and retain `drop_no_complete_engine_gain`. The original diagnostic screen still
contains 216 case gaps and ten family gaps. The November 1 scoped tradeoff and
diagnostic-only decision remain separate from those original outcomes.

Runtime/source binding checks cover 3,249 compiled sources, 141 external input
and executable identities, the 289-test installed package and 202 original
process receipts. All 65 failed and 137 passed receipts remain represented.
Sixteen corruption-control tests pass without skips. A relocated replay passes
after a Python audit guard rejects reads of the original evidence and source
trees and process/network effects; two deliberate original-read controls fail,
and the replay attempts neither original reads nor external effects. Independent
reader dependencies load before that guard. This is an evidence-isolation check,
not a general-purpose sandbox security claim.

The maintained entrypoint is `scripts/verify_local_io_packet.py`. It uses PyArrow
25.0.1, fastavro 1.13.1 and cramjam 2.11.0 as verification readers, outside the
engine runtime. From a checkout with those optional evidence dependencies:

```sh
python3 -B scripts/verify_local_io_packet.py \
  docs/benchmarks/evidence/native-local-io-portable-2026-11-03
python3 -B -m unittest discover -s scripts -p test_local_io_packet.py -v
```

The acceptance archive retains the three initial verifier/isolation failures,
their exact corrections, final commands, complete logs and verifier identities.
The collection is byte-identical to the original four bounded bundles; the
reader's existing record/part limits have not been enlarged.

## Gap disposition

The original r8 matrix, later shared adaptive owner/interface acceptance and
finite local table-reader acceptance now have their required local evidence.
The [documentation/site closeout](../release/v1.1.0-documentation-refresh.md)
covers the supported examples and user-facing boundaries. Hosted integration
of these final evidence/docs changes remains the milestone gate.

Necessary format limits remain explicit: IPC message granularity, controlled
ORC LZO rejection, admitted ORC/Avro logical-type translations and text wire
contracts. Local Iceberg/Delta/Hudi readers retain their finite visibility and
schema profiles; catalogs, remote table discovery and table commits are outside
them. Remote authenticated workflows remain deferred by the maintainer. Rich
data lifecycle belongs to item 19 after the source milestone. These boundaries
are not missing replay records or grounds to relabel timing screens as passing.
