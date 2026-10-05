<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native dynamic pivot composition

Status: implemented and merged in [PR #1507](https://github.com/depsilon/shardloom/pull/1507)
on October 4 after all 37 hosted checks passed. Exact head/merge identities and
review limitations are in the [completed ledger](phased-execution-completed-ledger.md).
This unit follows static
nested composition under PERF-02/03/07/10/11/12 and CG-3/5/19/20/21. It does not
close those entire owners or authorize a release.

## Contract

An admitted pivot consumes its immediately preceding native relation once,
discovers the output domain while building the existing sparse pivot state, and
uses that same state to deliver bounded native output. Downstream operators bind
against the actual resulting schema. They do not infer domains from a sample,
reopen an intermediate file, collect the prefix, or execute the prefix again.
SQL and Python/DataFrame declarations use the same native relational execution.

Source preparation retains authoritative schemas and generation-checked native
readers without reading query rows. A schema dependent on observed values is
explicitly unknown until execution. Each execution starts fresh under one CPU,
memory, cancellation and source-validation context. Schema discovery is included
in that execution's work and timing. Route inspection remains syntax-only.

The expert comparator is an encoded-columnar engine maintainer checking binding
order, source reuse, state ownership, empty schemas and atomic sink publication.
The implementation must preserve the existing static-schema fast path.

## Reuse ownership

| Existing component and callers | Gap and shared extension |
| --- | --- |
| `prepared_unary::BoundUnary`, `unary_pivot::Pivot` and the direct unary writer | Separate sparse-state completion and bounded emission from the direct-file report wrapper. Direct and composed callers use the same domain ordering, cell aggregation, margins, fill, collision and type rules. |
| Relational binder and synchronous traversal | Bind a dynamic pivot's input, consume it once into the existing state, then bind its parents with the completed native fields. Execution-owned state is consumed once and released when its downstream consumer finishes. |
| `VortexRelationalPreparation` and frontend schema lowering | Add execution-time schema resolution for declared dynamic plans. Retain frontend declarations and all prepared source leaves; resolve dependent names and wildcard expansion only when authoritative fields exist. Frontend lowering owns no row algorithm. |
| `NativeExecutionContext`, reserved containers and source generations | Schema discovery, subsequent binding, operators and sinks share the same grant and cancellation owner. Include discovery scans and state in existing execution evidence. |
| Native result producer and local sinks | Enter a writer only after the final schema is known, then use the existing admitted producer, source checks, cleanup and publication. No writer invents schema from its first batch. |
| Ordered Python renderer and SQL unary table parser | Expose pivot at its declared position, preserving source declarations, aliases, parameters and resource policy. Do not require users to enumerate observed pivot columns. |

Vortex-first provider decision: `implement_shardloom_kernel`. The pinned Vortex
0.85 native `DType`, `ArrayRef`, scalar/validity access, retained file readers,
ordered scans and allocator-backed typed output remain the providers. They do
not implement ShardLoom's pivot-domain naming, sparse aggregation or relational
schema binding. This unit connects the existing ShardLoom pivot kernel rather
than introducing a new execution provider. No dependency, Arrow execution
middle, external engine integration or serialized intermediate is added.

Metadata-first source preparation and existing scan pushdown remain applicable
before a pivot. Observed pivot domains cannot be answered from an ordinary source
schema. Capillary batches and synchronous consumers bound delivery; sparse state
remains charged to the one PulseWeave grant. Resource denial is explicit when
that state cannot fit. Existing ordering spill does not imply pivot-state spill.
Engine, preparation, transport and end-to-end clocks stay distinct.

## Schema and lifetime rules

- Known plans retain their prepared bound tree. Dynamic declarations retain
  source identities and bounded plan metadata, with a fresh bound tree for each
  execution. Preparing either kind must not execute rows.
- A frontend may defer its schema-dependent lowering until the admitted execution
  context exists. Its captured declaration and source mapping must be bounded and
  reserved. Source normalization happens during source preparation; a deferred
  lowerer cannot discover undeclared sources or invoke another executor.
- Completed pivot references are scoped to that execution and have one consumer.
  Reuse in another execution, duplicate consumption, missing references and
  unconsumed state are errors. They are in-memory ownership references, not file
  sources or a cache of answers between calls.
- The native schema is authoritative for wildcard expansion, aliases, projection,
  filtering, aggregation, ordering, joins, sets, windows and further unary stages.
  Unknown or ambiguous columns fail explicitly after the relevant domain has been
  discovered and before publication. Every stage preserves its declared position.
- Empty pivot input has its actual index-only schema, plus a declared margins
  column when applicable. A downstream named pivot-domain column that was not
  observed is absent; it must not be fabricated from a request or previous run.
- The existing scalar pivot admission, aggregate/null/fill/margins semantics,
  deterministic output names and 128-field result boundary remain in force.
  Richer pivot keys, mixed Variant outputs and larger domain support require
  their own type/ownership evidence.
- Correlated inner execution may discover different schemas for different outer
  rows. It cannot share a completed pivot across parameters. Its implementation
  must bind and consume within each parameter's scope, with explicit final
  projection/arity checks; support is not inferred from uncorrelated fixtures.
- The internal Rust preparation API's schema query must represent an unknown
  schema explicitly instead of returning an empty placeholder or reading rows.
  Update every repository caller and test together. CLI/Python request schemas
  and existing execution evidence fields remain compatible; delivered columns
  describe the actual execution.
- Retained arrays own their buffers and credits independently of pivot state.
  Consumer failure, cancellation, source replacement, schema failure and denied
  capacity release all execution-owned state and publish no successful output.

## Acceptance

Freeze independent expected results before the public matrix. Include pivot and
pivot-table aggregates, duplicate-cell rejection, null keys/values, sparse cells,
fill values, margins, empty input, renamed columns, Unicode/quoted domains and
output-name collisions. Check the direct provider alongside composed callers.

Exercise transformed inputs and downstream projection/filter/order/limit,
aggregation, joins, sets, windows, melt and successive pivots. Explicitly cover
alias/wildcard expansion, schema-changing repeated executions, correlated scope
and late binding failures. A source generation change must invalidate reuse.
Verify complete values, native dtype/nullability and actual scan counts.

Use native Vortex and declared compatibility inputs, SQL and DataFrame spelling,
bounded collection and all representable local writers. Reopen each complete
output. Cross batch and collection boundaries without changing collection limits.
Cover slow/failing consumers, cancellation before and during domain discovery,
narrow grants, state pressure and ownership after producer drop. Inspection must
remain inert even for missing sources and dynamic schemas.

Run focused tests first, then workspace formatting/Clippy/tests, native provider
and CLI suites, Python, lean/MSRV and affected documentation gates. Freeze the
runtime and run the complete public matrix and Full43 regression acceptance under
the existing serial storage/process guards. Availability is decided by complete
correctness and resource evidence; no speedup is claimed without paired evidence.

All accepted paths retain `fallback_attempted=false` and
`external_engine_invoked=false`. Real Vortex payload proof remains distinct from
placeholder artifacts. Wider types, adapters, pivot-state spill, paused large
format performance runs, native Python bindings and publication keep their
existing owners. CG-1 through CG-23 remain visible in the phase plan.

## Implemented ownership and public evidence

The direct provider and relational binder share `CompletedPivot`, including
sparse aggregation, domain names, scalar/null policy, margins and bounded batch
emission. An execution owns each completed stage until its consumer finishes.
No prefix is collected, reopened or executed a second time for schema discovery.

`prepare_relational_with_dynamic_schema` retains declared source handles and a
reserved lowering callback. `resolve_output` returns actual columns and an
opaque, one-use reference within that execution. `defer_subquery` retains an
inner declaration for the existing correlated executor; each outer singleton
receives its own binder, pivot state and schema. Invalid, foreign, repeated and
unconsumed references fail explicitly. Declaration and native-state reservations
share the existing execution grant and cancellation owner.

Static Rust plans keep metadata-time binding. `output_dtype()` now returns
`Some(dtype)` for those plans and `None` for a deferred declaration. Rust callers
that require data-dependent schema binding use the explicit dynamic preparation
API. SQL retains its parsed declaration and source mapping, then lowers against
actual columns during execution. Python renders the same ordered SQL stages.

The execution report distinguishes declaration reuse from bound-plan reuse:
`resident_relational_declaration_reused` reports the former;
`resident_relational_lowering_reused` remains false for dynamic plans.
`relational_schema_binding` reports `during_execution` or `during_preparation`,
and `relational_dynamic_schema_stages` counts completed discovery stages,
including separate correlated parameters. Discovery scans contribute to the
existing scan-row and reservation evidence.

Frozen runtime `50cc1e22c4df85883653ddba60783fa2e4108f3b` passes 3,305 public
checks and 7,687,525 complete row comparisons. The 846 pivot checks cover native
Vortex and declared CSV inputs, SQL/DataFrame spelling, fresh repeated calls,
all eight writers, nulls and name collisions, changing correlated domains,
the inclusive 128-field boundary and complete 65,541-row output. The separate
direct-unary matrix passes 202 checks. Thirteen native lifecycle tests also
prove dtype/nullability, cancellation, source invalidation, pressure, parameter
ownership and result lifetime after the prepared operation is dropped.

All 24 selected local gates pass, including the workspace, native provider/CLI,
Python, feature/MSRV and documentation checks. All 258 paired Full43 retained
results match; no predeclared timing or memory investigation threshold is crossed.
The [acceptance report](../benchmarks/native-dynamic-pivot-full43-2026-10-03.md)
records the unchanged aggregate timing, complete readbacks, certificate payloads
and portable proof. PR #1507 subsequently merged after all 37 hosted checks
passed. This evidence does not establish a performance improvement or
complete the broader PERF and competitive gates.
