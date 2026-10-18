<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native nullable structural-layout acceptance

Nullable native records now retain their root validity and logical DType through
default structural-layout scans. Field access applies parent validity before
null-sensitive expressions and filters. Whole-record null tests read only the
validity child; literal projections read no input columns. Local provider,
workspace, native, Python and format/workflow acceptance passes. Hosted checks
and merge remain pending.

The [implementation contract](../architecture/native-nullable-layout-2026-10-18.md)
records the design and accepted scope. This corrects the default reader inside
the pinned Vortex 0.85 provider. It preserves native execution, explicit resource
admission and no external-engine fallback. The on-disk format and public schemas
are unchanged. Broader PERF-06/07 and CG-1 through CG-23 obligations remain open.

## Correctness coverage

The original default-root scan fails with a nullable-record DType mismatch.
Changing only the final type would leave predicates incorrect: a null parent
can contain stored, non-null child values. The corrected reader carries a private
validity partition into native expression evaluation. It masks the original
record or accessed field before surrounding operators and retains conservative
pruning when child statistics cannot establish the nullable expression's answer.

Provider fixtures compare complete values and DTypes against direct native
expression evaluation across root, field, reordered/empty selection, new-record
and literal projections; null-sensitive filters; row ranges and incoming masks;
empty, all-valid, all-null and mixed validity; nested records and an encoded
child. Read tracking verifies that unreferenced fields remain unread. Persisted
file fixtures exercise the default structural reader. ShardLoom fixtures prove
owned native-result retention and a nullable-root private query-run round trip,
including source/type identity and cleanup while a returned array remains live.

| Local validation | Passed | Scope |
| --- | ---: | --- |
| Workspace and reduced writer Rust gates | 4,374 | 3,225 workspace; 1,149 reduced writer |
| Native Rust gates | 4,047 | 2,968 native provider-boundary; 1,079 native CLI |
| Vendored provider suites | 389 | 249 layout; 140 file |
| Direct Python binding | 100 | No failures, errors or skips |
| Public workflow cases | 4,827 | All 36 existing families |
| Format/workflow records | 1,408 | 704 native; 704 independent oracle |
| Full43 regression calls | 43 | All complete values and schemas match |

The Rust counts are test executions across feature configurations, not unique
tests. The 28 ignored native tests and their literal reasons exactly match the
preceding accepted suite; they remain manual benchmark/cost/regeneration checks.
The report does not claim they ran. Required workspace format, Clippy and tests,
native Clippy, provider format/lint/tests and the CI command contract pass.

The 18 format/workflow cohorts cover 22 workflows, all eight admitted inputs
with collection, and CSV input to all eight admitted outputs, using raw and
prepared input. This is a finite star of combinations, not the full input/output
Cartesian product. Independent oracle records are validation only. The public
evidence reopens 4,495 complete value files, 39,979 expected value rows and
6,330 result envelopes, plus 45 pressure fixtures containing 1,578,741 rows.

## Full43 observation

**Full43 engine time: 52.266268995 seconds (43 queries, one execution each).**

This is one retained-native regression pass and carries no comparative speed
claim. The ordinary native CLI reads the existing 99,997,497-row ClickBench
Vortex input, without a new ingest. Its 15,713,610,513-byte payload has SHA-256
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`.
The observation uses macOS 27 ARM64 with 10 logical CPUs, native memory admission
of 24 GiB and maximum parallelism 12. Admission is not a whole-process memory
ceiling. The file is resident and prehashed; OS cache state remains uncontrolled.

All 381 result rows, schemas, native certificates, resource declarations and
engine clocks are reopened. Source, executable and complete input identities
remain unchanged and native children are absent after execution. Every query's
timing companion and the complete timing CSV remain in the evidence. The engine
sum excludes UAT preparation and post-run verification.

## Source and reproduction evidence

The [local evidence index](evidence/native-nullable-layout-2026-10-18.json)
identifies the split archives and complete member manifest. They retain all
2,416 accepted source files; the original failure and intermediate attempts;
test, build and guard receipts; frozen acceptance protocol; complete public,
Full43 and modular results; and reproduction helpers with their local import
closure. Every archived member is reopened and verified against its original
size and SHA-256. Bulk inputs, executable binaries and redundant output payloads
have explicit exclusion identities; result values and validation evidence remain.

Source validation 649 precedes a single CI-validator change. The 650-a1 bridge
proves that only `scripts/check_ci_gate_matrix.py` changed, requires the complete
provider commands and rejects the earlier narrow command. It reruns CI contract,
release metadata and license checks. The CLI and direct binding are then built
from the 650-a1 source and frozen for all acceptance runs.

The main reproduction helpers are `run_nullable_layout_validation_649.py`,
`validate_nullable_ci_bridge_650.py`, `nullable_layout_acceptance_650.py`,
`run_nullable_layout_full43_650.py`, `run_nullable_modular_651.py` and
`package_nullable_layout_652.py`. The archived protocol binds their dependencies,
commands, source identities and reference values. Local absolute paths are
provenance; a different host must remap them explicitly and rebuild from the
recorded source before rerunning. Historical helper/source versions are retained
without relabeling them as the final runtime.

This acceptance does not add a public spill operator, external-memory membership
strategy, object-store execution or new output publication contract. Those queued
contracts remain separate. Installed-wheel and hosted integration evidence will
be recorded separately after the final PR head passes.
