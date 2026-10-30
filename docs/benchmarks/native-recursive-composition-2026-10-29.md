# Native recursive and materialized composition acceptance — October 29, 2026

Local public, default/native workspace, common-conformance and installed-package
acceptance pass. Hosted PR acceptance remains a separate gate. The
[implementation contract](../architecture/native-recursive-composition-2026-10-29.md)
and [public guide](../reference/connected-programming.md) define the supported
semantics and caller policy.

Explicit `AS MATERIALIZED` retains a complete native relation once per action.
Recursive CTEs evaluate an anchor and successive frontiers; `UNION ALL` keeps
duplicates, while `UNION` removes rows already present using native complete-row
identity. Immutable shared definitions connect SQL, DataFrame, typed relations,
workflows and CLI execution to the same native owners. The positive session/CLI
`recursion_limit` defaults to 1,000 recursive-term evaluations, independently of
plan nesting. A new action recomputes its relations.

Existing Vortex arrays, memory reservations, native replay, set operations and
spill implement these boundaries. No dependency or external executor is added.
Vortex remains native input and output; Arrow IPC and Parquet are explicit
compatibility boundaries. No new benchmark or performance claim is made.

## Complete-value and package acceptance

| Evidence | Result |
| --- | --- |
| Focused native checks | 19 tests pass, including binding, iteration, action reuse, zero-limit completion, real spill and failure cleanup. |
| Five public interfaces | Nine test methods pass without skips, with 60 equivalent positive executions across DataFrame, Python SQL, typed native relations, embedded workflows and CLI. |
| Independent comparisons | 69 complete comparisons cover 450 rows, including 420 rows from the five-interface cases. Schema, nullability, nested values, decimal values, temporal ticks and original floating bits are retained. |
| Native evidence | All 62 native certificates identify admitted composition work and explicit no-fallback execution. Public envelopes carry the same policy and counters. |
| Common conformance | All 153 stages pass: 126 executable semantic cases, 25 expected diagnostic cases and two support stages. Two new complete fixtures cover recursive graph/materialized edges and materialized grouping/window composition. |
| Default workspace | 3,289 tests pass, with no failures or ignores. Formatting and default/native strict lint pass. |
| Native workspace | 6,489 tests pass with no failures; 35 preexisting opt-in tests remain ignored. The minimal native build passes with its seven existing conditional warnings. |
| Installed package | The exact frozen CLI/library is packaged and installed in an isolated environment. All 259 integration tests pass without skips, including all new composition methods; verified uninstall passes. No publication occurs. |
| Source and documentation | Fifteen source/reference/conformance checks pass, including 136 tests without skips. The exact recursive example in the public guide executes successfully. |

The full native suite preceded only a conditional import correction for the
minimal native build. The packet preserves that earlier source and verifies the
exact import-only rewrite. The final source passes focused native tests, strict
lint, product builds, all public checks and installed-package acceptance; no
behavior change is omitted from the full-suite evidence.

The common-matrix consumer requires canonical artifact references below
`target/admitted-semantics-matrix/artifacts`. The local run writes its artifacts
under nonsynced task storage. Acceptance preserves the original path refusal,
verifies the exact path mapping, validates the portable view through the existing
consumer and independently captures all 126 complete expected/actual results.
It neither reruns the queries nor relaxes the production validator.

## Semantics, ownership and failure controls

Complete references cover a 38-step chain, branching and cyclic graphs,
duplicate paths, NULLs, empty anchors, rich retained payloads and floating
identity under both policies. Composed workflows include typed parameters,
named-plan snapshot replacement, multiple CTEs, self-joins, GROUPING SETS,
aggregate FILTER, windows and final ordering. Vortex, Arrow IPC and Parquet
inputs and independently reopened outputs exercise the same native operators.

Read-once sources are replayed without reopening their producer. Tests verify
one materialized producer evaluation per action, independent subsequent actions,
retained output lifetime, source-generation refusal, late producer failures,
cancellation, early consumer closure and successful fresh retries. Invalid
self/forward/mutual references, nonlinear recursion and schema drift fail
explicitly. CLI custom limits and native workflow inheritance use the same
immutable session policy.

Attempt `c016` exposed two completion bugs: outer `LIMIT 0` hid an iteration-limit
failure and a late arithmetic failure. The shared completion requirement now
propagates through retained relations and their parents, so both standalone
LIMIT and pipeline shortcuts finish required validation. The corrected focused
suite and final public/installed suites pass. Failed native writes publish no
partial fixpoint. All 11 unsuccessful guarded attempts remain in the evidence;
earlier fixture-alias and workflow-flag test mistakes are also preserved.

Every pressure case checks complete values against an ample-memory execution
and independent expected values. Resident-only execution at the pressure grant
fails; explicit native spill completes within that grant and the existing disk
quota. No storage limit is raised.

| Case | Input / complete output rows | Grant | Retained payload bytes | Spill runs | Peak reservation | Peak spill bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Materialized relation | 8,193 / 8,193 | 16 MiB | 16,913,490 | 37 | 3,230,566 | 33,972,564 |
| Recursive relation | 4,097 / 12,291 | 16 MiB | 25,373,290 | 57 | 3,412,153 | 33,980,604 |

Planned retry controls produce the same complete results and spill counts; the
recursive retry peaks at 3,412,205 reserved bytes. Quota denial, corrupt spill,
source/schema failures, cancellation and consumer failure release owned state
and preserve unrelated files. These are resource/correctness observations, not
timing measurements or general scalability claims.

## Portable evidence and retention

The [summary](evidence/native-recursive-composition-2026-10-29.json) identifies the
[portable packet](evidence/native-recursive-composition-2026-10-29.json.xz): 588
members preserving all 43 guarded attempts, source/product identities, complete
public values, matrix artifacts and references, pressure logs, helper sources
and installed-package evidence. The packet SHA-256 is
`fa0b79866a4a92305259912119bfbf6aa8c03bbd017b0132f7dfbe48d9e4eb3b`.

The [isolated replay](evidence/native-recursive-composition-2026-10-29-replay.json)
uses the archived verifier without importing the checkout or native engine.
Sixteen corruption controls reject changed members, source/product identities,
failed-attempt records, values/types/nullability/floating bits, counters,
certificates, policy, counts, pressure, matrix, wheel and uninstall evidence.
The [cleanup receipt](evidence/native-recursive-composition-2026-10-29-cleanup.json)
records exact retired paths and identities, closed-handle checks, retained
runtime/wheel identities and verification after retirement. Current inputs and
compact results remain; no rollback payload archive is created. It retires
10,536 regular files and three environment links, including 436 byte-verified
packet copies and 210 regenerable test fixtures. The
[post-cleanup replay](evidence/native-recursive-composition-2026-10-29-post-cleanup-replay.json)
passes all 16 controls again without the retired payloads.

## Remaining boundaries

Only the admitted linear recursive shapes are supported. Nonlinear, mutual,
outer-correlated and data-dependent-schema retained definitions remain explicit
errors. Anchor domains do not silently widen, rescale or reinterpret timezone
identity. UNION detects repeated complete rows, not every changing path cycle.
The iteration limit, memory grant, spill quota and cancellation remain separate
constraints; an outer LIMIT cannot replace fixpoint completion.

This closes local acceptance for the composition unit. Hosted review/CI,
broader combined workflows, batch functions, connectors/refresh and both item-20
tracks remain open. The seven performance candidates follow this unit before
batch-function work, per the October 29 maintainer order. No competitive gate or
release is declared complete.
