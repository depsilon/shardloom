<!-- SPDX-License-Identifier: Apache-2.0 -->

# Resolve native dictionary views before the fused group consumer

Status: historical revision-three correction, subsequently removed with the
maintainer-requested [whole-item drop](../benchmarks/native-workflow-drop-2026-10-25.md).
The following records its original premeasurement rationale.
The [original finite protocol](native-fused-measure-work-elimination-2026-10-25.md)
continues to own admission, correctness, six immutable inputs, resource grants,
paired schedule, timing thresholds and package acceptance. This note records
the exact change after its revision-two observation gate failed.

## Preserved failure and source diagnosis

All twelve revision-two debug calls completed with exact ordered values,
schemas, resource cleanup and expected computed-column elimination. The
independent verifier rejected the candidate because all three dictionary
targets observed zero dictionary-backed domains. No revision-two release
measurement ran, and no timing benefit is inferred from debug execution.
The first parsing failure remains separate and unchanged.

The immutable files retain their dictionaries. A small program linked against
the pinned Vortex 0.90.0 build reproduces the composed scan projection and
selection: the selected key is a deferred `get_item` over `select`, with the
dictionary below it. The existing variable-key owner checks the outer array
for `Dict` before requesting `VarBinViewArray`; that request expands the key.
Vortex's existing `execute_until::<Dict>` resolves the wrapper and stops at
the dictionary (or an ordinary canonical array). The reproduction checks all
2,048 selected values, dtype and row count. Its earlier no-projection probe
is preserved too: it disproved the initial assumption that every filtered
scan already has this wrapper.

## Candidate boundary and provider decision

Resolve only UTF8/Binary group columns inside the already admitted fused
consumer. Complete the existing projection preparations in their original
order first; then use the query's native execution context to resolve the
group views, check unchanged dtype/rows and pass them to the existing grouping
owner. Numeric mapping, arithmetic order, source checks, input files, ordinary
non-admitted execution and spill selection remain unchanged. No provider or
allocator failure triggers replay, source reopening or a strategy retry.

This is `use_vortex_native_provider`: pinned Vortex 0.90.0 array execution,
isolated in `shardloom-vortex`, behind the existing local-primitives feature
and query admission. Existing native I/O and fused-stage certificates report
the path with `fallback_attempted=false`. There is no new interpreter,
representation, dependency, worker pool or external engine. Group identity
uses actual dictionary values, with existing full-domain validation and
credited native backing owners.

## Revision-three proof, frozen before timing

Keep the identical corrected cost harness and original native inputs in both
roles. The unchanged baseline reuses its frozen revision-two debug and release
executables, source overlays and build receipts; verify their hashes before
and after every cohort. Build and freeze the revised candidate separately.
Run twelve debug observations before the original 72-call release schedule.
No failed cohort is replaced or given acceptance credit.

Require the original complete values, schemas, cleanup, absence of fallback,
CPU and memory controls, exact mapped rows and zero constructed expression
rows for eligible candidates. In addition, verify delivered scan rows equal
the independent selected-row count. Each dictionary target must observe one
dictionary-backed group domain per delivered scan batch and 65 dictionary
entries per such domain. Plain/high-cardinality and ineligible controls must
keep zero dictionary-backed domains; tiny uses its eight-entry domain.

The revision-two verifier also required equal variable-domain value counts
between roles. That auxiliary check measures prepared dictionary entries,
not source consumption, and is incompatible with the intended encoded
consumer: dense inputs can shrink that count, while sparse batches can refer
to fewer rows than the complete dictionary has entries. Revision three
replaces that equality with the exact dictionary-entry checks above and
independent delivered-row validation. Preserve equal domain-owner counts.
This is an explicit mechanism-check correction, not a timing-threshold change.
The 2% target improvement in both cohorts, 5% engine and first-execution
controls, and tiny's 50-microsecond allowance remain unchanged. A further
activation or timing failure ends this revision without silent retries.

Extend the existing dictionary/null/order/reexecution regression to assert
actual dictionary activation. Recheck native semantics, cancellation, denial,
cleanup and lint before the isolated comparison. The package still requires
public-surface proof, broad checks and one isolated standard Full43 before PR.
