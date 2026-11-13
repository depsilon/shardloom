<!-- SPDX-License-Identifier: Apache-2.0 -->

# One native execution pipeline

Status: implemented and validated locally under the November 12 maintainer
instruction. The [one-pass observation](../benchmarks/native-pipeline-consolidation-2026-11-12.md)
passes all 10,000 original cases; the report preserves the exact validation
scope, original failures and the later SQL-admission correction below. No hosted integration
or publication is claimed.
This work precedes the remaining item-20 performance investigations. It extends
the existing planner, native operator, resource and diagnostics owners; it adds
no execution engine or competitive gate.

## Decision

Every supported query executes one admitted native operator plan. Remove the
whole-query choice between an optional file provider and a general relational
executor. File-backed optimizations become operators inside that plan and can
compose with native joins, windows, expressions and other implemented operators.
An absent implementation is an explicit error. A declined optimization is not
permission to invoke a separate query executor, and an execution failure must
never retry the query through another implementation.

The existing native join, window, aggregation and source operators are real
implementations. Consolidation retains their supported semantics. It does not
manufacture unsupported cases from the old whole-query provider label. The old
193-of-10,000 count describes one optional file optimization; it is not native
execution coverage or proof that 9,807 operations were unimplemented.

Preparation remains an independent source investment. Original compatibility
files feed bounded native arrays into the same plan. Existing Vortex files are
opened directly. Explicit preparation can create reusable Vortex storage, but
query execution must not introduce a mandatory preparation write or silently
prepare a source merely to enter the engine.

## Implementation contract

- Validate the complete declaration and its types before admitting physical
  operators. Preserve nulls, exact numeric domains, order, errors and source
  generation checks when admitting an equivalent file kernel.
- Represent file kernels in the native operator tree. Remove the optional
  top-level provider and the provider-or-general execution branch. Execute
  through one exhaustive operator dispatcher; there is no catch-all executor.
- Admit file kernels at eligible subplans, including subplans of composed and
  multi-source queries. Preserve the original bound semantic description for
  demand, ownership and policy analysis; it is not an executable retry path.
- Apply the same compilation and execution contract to deferred declarations,
  retained relations, recursive work and explicit spill policy. Resource changes
  select a legal plan before execution, not after an operator failure.
- Preserve the existing source, session, cancellation, memory, spill, result and
  writer owners. A nested kernel contributes its actual execution evidence to
  the complete plan; a single primitive report must not stand in for a composed
  query's work.
- Preserve public source and preparation APIs. Keep legacy report fields where
  compatibility requires them, while identifying the unified pipeline and actual
  executed native operators. Unsupported diagnostics stay deterministic and
  identify the missing capability before payload or output effects when binding
  can determine it.
- Complete SQL and embedded workflow declarations compile to the same native
  plan. Remove the embedded primitive-versus-relational operation wrapper and
  the file-SQL shortcut around the common binder. Low-level typed primitive and
  source/sink APIs remain individual kernel APIs; they cannot interpret arbitrary
  unsupported query plans.

## Vortex-first provider check

Classification: `use_vortex_native_provider` within existing ShardLoom operator
owners. The pinned upstream `ArrayRef`, DType, array kernels, file/scan and native
source/sink surfaces already supply the data and compute primitives. ShardLoom
owns relational composition, complete-query semantics, admission and evidence.
No new source format, dependency, Arrow execution substrate or Vortex query-engine
integration is introduced. Required representation changes remain explicit;
external execution and fallback remain prohibited.

## Verification and observation

Regression coverage must demonstrate composed file kernels, direct Parquet/IPC
and native-memory inputs without prepared writes, supported joins/windows,
explicit unsupported operations, terminal kernel/consumer failures, generation
changes, bounded ownership, deferred binding and spill. Test reference execution
may remove an optimization for equivalence checks; production must expose no
switch that restores the deleted whole-query catch-all route.

Run formatting, strict workspace lint and workspace tests, then the relevant
native-feature and installed public-surface checks. Freeze the tested runtime,
source and unchanged original 10,000 declarations before the single authorized
observation. All other task work remains idle during that pass; existing storage,
deadline, complete-result and process-cleanup guards apply. No warmups or repeats.

Report successful complete results, unsupported operations and other failures
separately. Sum engine time for successfully validated cases, compare the same
case IDs with the preceding accepted observation, and report that preceding
observation's engine time for newly blocked cases. Publish a complete 10,000-case
total only if all 10,000 cases pass with valid clocks. The completed observation
is 1,514.235435577 engine seconds; the preceding exact same-ID observation is
1,540.711839539 seconds. All 10,000 cases pass with valid clocks and no missing
support group. The previous engine sum for newly blocked/incomplete cases is
zero. This is one observation per runtime, not repeatability or individual-change
attribution evidence.

This is a requested architectural consolidation, not acceptance of a previously
rejected performance candidate. Preserve all original retain/drop decisions,
the invalid B baseline, and CG-1 through CG-23 as independent gates.

## Initial integration findings

The public file-SQL and embedded-workflow shortcuts are removed from query
dispatch. Complete SQL, typed relations and embedded primitive declarations now
own a `PreparedVortexRelational` plan; file specializations are internal operators
in that plan. Direct low-level primitive commands retain their explicit kernel
contracts. They do not receive arbitrary unsupported relational plans.

The initial CLI unit/integration suite passed after updating route metadata
expectations. At that observation, the shared SQL frontend rejected the legacy
`EXTRACT(minute FROM ...)` and `DATE_TRUNC('minute', ...)` forms during route
admission. Their old primitive-summary recognition remains available as inert
metadata; it no longer routes complete SQL around the shared compiler. Those
were syntax-admission gaps; native calendar kernels already existed. The
correction below preserves the single executor contract.

### November 13 SQL minute correction

The maintainer requested both spellings before opening the consolidated PR.
The bounded shared scalar parser now normalizes `EXTRACT(minute FROM expression)`
to `extract_minute(expression)` and `DATE_TRUNC('minute', expression)` to
`date_trunc_minute(expression)`. Operands compose through the same parser,
including predicates, aggregates, lazy scalars and relational bindings. Repeated
minute expressions in GROUP BY and ORDER BY resolve by complete parsed identity
to their existing output alias; constants and operands cannot be discarded.

This remains `use_vortex_native_provider`: existing native expression binding,
minute kernels and Vortex arrays own values, nulls and output types. No new
kernel, dependency, executor or Arrow execution representation is added. The
existing integer-epoch-second and timestamp-text contracts are preserved;
signed truncation floors negative epochs and nulls propagate. Typed timestamps
retain their explicit native helpers (`timestamp_minute`, `temporal_extract`,
`temporal_trunc`); these epoch-second spellings cannot reinterpret timestamp
ticks. Other EXTRACT fields and DATE_TRUNC units remain explicitly unsupported.

Regression coverage includes complete results for negative/positive minute
boundaries, null and empty inputs, text and unsigned epochs, composed operands,
grouping, ordering, LIMIT/OFFSET, invalid syntax, parser depth and typed-unit
rejection. Installed-package coverage collects, streams and persists both SQL
forms through native Vortex. This correction postdates the frozen 10,000-case
observation; its original runtime, packet and measured values remain unchanged.

The [separate correctness packet](../benchmarks/evidence/native-pipeline-sql-minute-2026-11-13/README.md)
binds source `ab6dd9ed50c08030d0571254b40f068d03bd3fb2` to eight focused tests,
3,325 default tests, 1,567 CLI/Python native tests (three existing ignored),
strict default/native lint and 442 installed-package tests with zero skips.
The original fixture error and deliberately stopped build remain recorded.

Installed-surface validation exposed implicit aggregate output-name drift and
missing declaration-reuse report fields. The shared binder now preserves the
established file-SQL aliases using the admitted source schema; this naming step
does not choose an executor. The historical preparation-reuse field identifies
query-declaration reuse, independently of persisted source preparation.

File collections compose native scans and the shared aggregation/sort operators.
They no longer claim the removed whole-query partitioned row-reference sorting
optimization. Complete values, ordering, streaming and persistence remain the
validation contract; future optimization must become an operator in this same
pipeline. All query sources also use the same native writer policy; the retained
writer-coalescing report field is false for both native and compatibility sources.
Arrow export may separately refuse a memory grant after collection;
the wide-schema lifecycle check requires a precise memory diagnostic and release
of every owner in either phase.
