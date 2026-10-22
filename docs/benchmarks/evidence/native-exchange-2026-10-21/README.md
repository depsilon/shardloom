<!-- SPDX-License-Identifier: Apache-2.0 -->

# Rejected finite native exchange evidence

`decision.json` records the rejection; `results.json.xz` contains the original
JSON and log members with their byte lengths and SHA-256 identities. This is a
result archive, not an executable, dataset or source-tree backup. It includes
both complete 325-workflow cohorts, the exact fixture manifest and protocol
freeze, final preflight/protocol results, all intermediate failed receipts and
the pre-scoring build acceptance. No scored query was rerun for packaging.

From a checkout containing candidate commit
`6e3419bc5f36d0ec41d381420d255c77ef533409`, run:

```sh
python3 -B docs/benchmarks/evidence/native-exchange-2026-10-21/verify.py
python3 -B docs/benchmarks/evidence/native-exchange-2026-10-21/verify_closeout.py
python3 -B -m unittest discover -s docs/benchmarks/evidence/native-exchange-2026-10-21 -p test_native_exchange_cost_gates.py -v
```

Only Python's standard library and Git are needed. Replay checks 2,003 frozen
repository inputs directly against the candidate Git commit, recomputes both
complete cost reports including the fixed-seed bootstrap, checks all 650 scored
and 67 preflight/client workflows, verifies actual protocol discovery/execution
and process-failure records, and reruns the 24 report-corruption refusals.
The four adjacent original verifier/test files retain their exact scored bytes.
The reported rejection is expected; a successful replay does not mean the cost
gates passed. Engine and other diagnostic clocks preserve their original meanings.

Each packet member has `text`, `bytes` and `sha256`. To inspect a result without
extracting payloads, load `json.loads(lzma.decompress(Path("results.json.xz").read_bytes()))`
and parse the selected member's `text`. The complete score is
`exchange-frozen-cost-report-846-a1.json`; the cohort reports are
`exchange-scored-report-one-846-a1.json` and
`exchange-scored-report-two-846-a1.json`. Each score contains all thirteen cases
against all four controls, with individual paired values and all gates.

For implementation archaeology, candidate source is in the commit above;
the accepted prerequisite baseline is
`02aa39f310322e019b4acc2cf631d88fe3c9edb2`. The original fixture generator,
reference recipes, schema, dependency lock and frozen plan are in that history.
The preceding [control packet](../native-exchange-control-2026-10-20/)
contains the exact generation/validation commands and both fixture identities.
`exchange-validated-build-freeze-846-a1.json` records the toolchain, features,
configuration, external runtime inputs and accepted build/test commands.
The cohort guard receipts record the exact invocations and host/process context.
These command records use historical absolute local paths; a future reproduction
must use fresh unsynced output paths, freshly generated test credentials and the
same storage/process safeguards. Do not rerun an unchanged rejected mechanism
as a new retention attempt.

`postchecks.json` and `postchecks.json.xz` separately preserve source retirement,
fresh restored-source validation and the public test-certificate material.
`cleanup.json` identifies the exact generated artifacts retired after validation.
`verify_closeout.py` checks these records without a native build or query run.
No private certificate keys are archived. Public protocol/client support,
package publication and CG completion are not claimed.
