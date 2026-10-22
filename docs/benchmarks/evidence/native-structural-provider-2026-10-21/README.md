<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native structural provider evidence

This packet supports the
[finite ownership acceptance](../../../architecture/native-structural-provider-acceptance-2026-10-21.md)
at implementation commit `89da798f949aaa83705491e1cf711e3e0d6ae9c5`.
The [manifest](acceptance.json) identifies the compressed packet and verifier.
Its 29,659 members preserve original records, complete result envelopes,
references, test transcripts, source/compiled-input identities, guards and
failure history. No executable, installed environment or dataset backup is
included. Absolute paths inside records are historical identifiers, not replay
dependencies.

The [copied-packet replay](replay.json) passes. The
[helper correction record](helper-corrections.json) preserves initial assembly
failures on multi-object JSON stdout and the corruption checker's first
linked-reference failure. Assembly now reads that stdout as a JSON stream;
the negative checker rehashes each affected record once in dependency order
so shared ancestors cannot retain a stale intermediate identity. Its mutation
uses the reference result's actual named field. The interrupted intermediate
attempt and row-shape correction are also retained. These corrections
repeat no native workload and weaken no acceptance gate. The packet retains
the original negative checker; the manifest also identifies the corrected
standalone checker shipped beside it.

The replay checks 2,594 committed source inputs; 69 new native regressions;
workspace, native, CLI, provider and installed-package tests; all 5,930 public
cases; 18 format cohorts with 704 native and 704 independent reference
workflows; complete Full43 outputs; and all retained timing diagnosis. It
retains 27 failed and 74 passed guarded-run receipts with their original logs.
Original parser/fixture, cleanup-test, notice, compiler-binding and verifier
corrections remain visible. Passing the replay means those archived records
are internally consistent with the committed source. It does not run native
queries, recheck old payload residency or establish cross-platform acceptance.

## Replay

Run from a checkout containing the implementation commit and unchanged replay
dependencies. Python 3.10 or newer and Git are sufficient; native binaries and
the original local workspace are unnecessary.

```sh
python3 docs/benchmarks/evidence/native-structural-provider-2026-10-21/verify_acceptance.py \
  --repository . \
  --manifest docs/benchmarks/evidence/native-structural-provider-2026-10-21/acceptance.json

python3 docs/benchmarks/evidence/native-structural-provider-2026-10-21/check_corruptions.py \
  --repository . \
  --manifest docs/benchmarks/evidence/native-structural-provider-2026-10-21/acceptance.json \
  --verifier docs/benchmarks/evidence/native-structural-provider-2026-10-21/verify_acceptance.py \
  --output /tmp/shardloom-structural-corruption-checks.json
```

The corruption check requires a new output path. Its cases rehash altered
member references so they exercise semantic checks beyond simple checksum
failure. They challenge source/gate omission, changed package origins,
weakened installed coverage, lost cleanup, complete-value and engine-clock
substitution, query duplication, lost failure history and misleading timing
scope. The valid packet is replayed before and after the negative cases.
All [21 semantic corruption cases](corruption-checks.json) reject with their
expected diagnostics, and both positive replays pass.

## Timing identity

The final isolated Full43 totals **49.990920425 engine seconds**, one execution
of each of the 43 queries, on the unchanged candidate executable and input.
The manifest retains the earlier 112.675662535-second complete pass under its
original `full43_engine_execution_seconds` field. The final isolated result
has the separate `isolated_full43_engine_seconds` field. The twenty targeted
diagnostic calls and two intervening complete comparison passes remain
separate observations, with all complete values and schemas replayed.

This distinction preserves the history; it does not assert a causal diagnosis
or speed gain. Future routine runs follow the
[isolated single-pass procedure](../../../skills/benchmarking.md#isolated-full43-procedure).

## Scope records

The [scope validator](scope-validation.json),
[governance validator](governance-validation.json) and
[primary queue inventory](queue-scope-validation.json) accompany the revised
phase plan. All 58 child IDs and fourteen parents remain. Fifteen scoped
required contracts and five scheduled rows precede the 38 optional rows at the
bottom; optional breadth carries no implementation commitment. Existing valid
evidence may satisfy a required contract. This packet closes no entire PERF
owner, CG gate or release.

The [retirement receipt](cleanup.json) and compressed identity inventory record
39,022 removed generated file/symlink entries. Complete public/format outputs,
temporary reference/install environments, staged package copies and the older
comparison runtime are reproducible from their source/protocol records. Exact
archived records are checked against the packet before deletion. Closed-handle
checks pass. The latest accepted CLI, native library and installed-tested wheel
remain; original Parquet and current Vortex input are untouched. No payload
backup is created.
