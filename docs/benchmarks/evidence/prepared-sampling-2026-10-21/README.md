<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared weighted sampling evidence

Status: accepted October 21, 2026; complete portable replay and all 34 bounded
corruption checks pass.

This packet supports the
[finite prepared-sampling decision](../../../architecture/native-prepared-sampling-decision-2026-10-21.md).
It combines two separately frozen comparisons and corrected local acceptance;
their original source revisions, failures and claim boundaries remain distinct.
No benchmark or native engine is invoked by the replay commands below.

## Contents

- `acceptance.json` identifies the compressed packet, replay implementation,
  accepted source, scored source and full replay result.
- `acceptance.json.xz` contains complete values, independent reference outputs,
  source manifests, compiler-input records, plans, process receipts, compact
  logs, both comparison packets and the original failures.
- `verify_acceptance.py` verifies the complete packet against its recorded Git
  source revisions, rebuilds all result comparisons and checks ownership,
  no-fallback, clocks, test coverage and explicit scope.
- `check_acceptance.py` checks bounded evidence corruptions without rerunning
  native workloads. `corruption-check.json` records its results.

Compiled runtimes, wheel payloads, resident inputs, temporary reference
environments and generated datasets are omitted rather than retained as backup
copies. Their original identities remain in the evidence. Replay does not
revalidate live residency or execute archived code as an external engine.

## Replay

Run from a repository that contains the recorded source commits and unchanged
verification helpers. The branch preserves those commits through integration.
Pass an existing local scratch directory; the example uses `/tmp`.

```sh
python3 -B docs/benchmarks/evidence/prepared-sampling-2026-10-21/verify_acceptance.py \
  --repository . \
  --manifest docs/benchmarks/evidence/prepared-sampling-2026-10-21/acceptance.json \
  --scratch /tmp
python3 -B docs/benchmarks/evidence/prepared-sampling-2026-10-21/check_acceptance.py \
  --repository . \
  --manifest docs/benchmarks/evidence/prepared-sampling-2026-10-21/acceptance.json
```

The full replay checks all public cases, Full43 values and schemas, all eighteen
format cohorts, both comparisons and every required acceptance gate across
28,698 archived members. The
corruption check uses a complete first format cohort and the complete gate,
wheel and private-environment bindings. It is a bounded adversarial check, not
a substitute for the full replay.

## Scope and corrections

Each comparison has 768 measured workflows and 5,376 calls, plus 48 unscored
observed preflights and 336 calls. Final acceptance covers 5,930 public cases,
all 118 installed binding tests, Full43 once with 381 rows and 49.077810546
engine seconds, and 704 native plus 704 independent reference format workflows.
The matrix has exactly eighteen declared cohorts and does not claim all
input/output combinations.

The recorded corrections include the cyclic projection expectation, feature
gating and discovery imports, the installed output-window fixture, and the
format rerun after reference-package file changes. The original failed or
invalidated results are retained. The corrected wheel uses an explicitly frozen
complete test-directory overlay and the same native binaries. The format rerun
uses private package copies with unchanged pre/post identities; reference
engines remain correctness oracles only.

Two failed packaging attempts preserve the verifier's original inventory/count
comparison and receipt-field mistakes. Their corrected replay checks every
comparison member, count and recorded guard; no native workload is repeated for
those verifier corrections.

The protocol documents remain historical and byte-identical. The later
fixture/documentation commit and any subsequent dependency integration do not
become the source of the earlier timings. No release, package publication,
cross-platform certification, process-RSS limit or competitive superiority is
claimed by this packet.
