<!-- SPDX-License-Identifier: Apache-2.0 -->

# One native execution pipeline

Status: implemented locally under the November 12 maintainer instruction;
complete native validation and the one-pass 10,000-case observation are pending.
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
total only if all 10,000 cases pass with valid clocks. The preceding complete
observation is 1,540.711839539 engine seconds; no speed or coverage outcome is
claimed before the new observation.

This is a requested architectural consolidation, not acceptance of a previously
rejected performance candidate. Preserve all original retain/drop decisions,
the invalid B baseline, and CG-1 through CG-23 as independent gates.

## Initial integration findings

The public file-SQL and embedded-workflow shortcuts are removed from query
dispatch. Complete SQL, typed relations and embedded primitive declarations now
own a `PreparedVortexRelational` plan; file specializations are internal operators
in that plan. Direct low-level primitive commands retain their explicit kernel
contracts. They do not receive arbitrary unsupported relational plans.

The complete CLI unit/integration suite passes after updating route metadata
expectations. The shared SQL frontend currently rejects the legacy
`EXTRACT(minute FROM ...)` and `DATE_TRUNC('minute', ...)` forms during route
admission. Their old primitive-summary recognition remains available as inert
metadata; it no longer routes complete SQL around the shared compiler. These are
explicit syntax-admission gaps, not evidence that native calendar kernels are
absent. Do not restore the deleted executor split to admit them.
