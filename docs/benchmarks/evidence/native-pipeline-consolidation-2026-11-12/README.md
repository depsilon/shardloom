# Native pipeline consolidation evidence

This packet records the local implementation at
`3a861917ad6852e6a35ac8642d78c4d31451f29f` and the one authorized execution
of each original 10,000-case declaration. All 10,000 results pass. Engine time
is **1,514.235435577 seconds**, compared with **1,540.711839539 seconds**
for exactly the same IDs in the preceding observation. There are no newly
blocked or incomplete cases; their previous engine-time sum is zero.

See the [report](../../native-pipeline-consolidation-2026-11-12.md) and
[architecture contract](../../../architecture/native-pipeline-consolidation-2026-11-12.md).

## Verify without executing queries

```sh
python3 -B docs/benchmarks/evidence/native-pipeline-consolidation-2026-11-12/verify.py
```

The verifier uses Python's standard library. It verifies every packet and archive
member hash; the original order and 10,000 unique IDs; current/previous successful
statuses and engine sums; per-family totals; selected raw certificate fields;
and the recorded validation and retirement outcomes. It opens no native runtime,
uses no network and executes no query. The packet can be copied to another
directory and verified there.

The full result payloads and independent reference ledgers remain in local
evidence. They were replayed completely before this packet was created. This
compact verifier checks the saved summaries, clocks and selected certificates;
it does not independently repeat full answer correctness against those ledgers.
The selected certificate fields retain their exact raw response hashes and the
export receipt binds the original result ledger. Those hashes alone cannot
reconstruct the omitted payloads.

## Contents and limits

- `summary.json`, `cases.jsonl.gz`: all current outcomes and engine observations.
- `previous-summary.json`, `previous-cases.jsonl.gz`: the preceding complete
  observation, matched by original ID, order, family, profile and public surface.
- `certificates.jsonl.gz`, `pipeline-audit.json`: native pipeline, no catch-all,
  no external fallback, invocation and preparation-write evidence for every case.
- `validation-inventory.json`: all 51 guarded check receipts, including the
  original 18 failures; seven final accepted checks are identified separately.
- `observations.tar.gz`: byte-exact original receipts, logs, test repairs,
  installed-package reports, capture/replay helpers, declarations/input manifests,
  implementation patch and task-intermediate retirement records. No runtime,
  wheel, environment, dataset or rollback payload is archived.
- `manifest.json`: hashes and original locations. Recorded absolute paths are
  historical provenance, not paths the portable verifier opens.
- `verification.json`: result of running this verifier against this manifest.

The native-feature suite's original four stale assertion failures remain visible.
Focused repaired tests and all changed CLI/Python targets were rechecked; the
packet does not relabel that original run as a fresh passing full native suite.
The final installed wheel passes 441 tests with no failures, errors or skips.

This is one observation per runtime, without warmups or repeats. It supports
the observed same-ID comparison, not repeatability or attribution to an
individual change. The tested code and this evidence are local; there is no
PR, merge, package publication or release claim.
