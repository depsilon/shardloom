# Native recursive queries and explicit relation materialization

Status: implemented with local acceptance, October 29, 2026. Default/native
workspace, five-interface, common conformance, installed-wheel and portable
evidence checks pass; hosted PR acceptance remains a separate gate. The
[acceptance record](../benchmarks/native-recursive-composition-2026-10-29.md)
records complete results, failure controls and the admitted limits.
This is the composition continuation of
[item 20](phased-execution-plan.md#everyday-analytical-completion), under RFC 0044
and the existing expression, plan, resource, cancellation and workflow RFCs.
Item 20's performance track and CG-1 through CG-23 remain independent obligations.

## Existing owners and scope

Ordinary scoped CTEs, immutable named plans, typed parameters and long native
streaming pipelines already exist. GROUPING SETS, ROLLUP/CUBE, aggregate FILTER,
ordered selectors and ordered collections already lower into shared native
providers and have SQL, DataFrame and typed execution tests. Extend their
composition coverage instead of introducing another implementation.

The actual missing execution boundaries are recursive CTEs and explicit
`AS MATERIALIZED`. Implement both as native relation operators. SQL, DataFrame,
typed relations, workflows and the CLI must reach those same operators. Naming
or composing a declaration remains inert. Ordinary CTEs and `NOT MATERIALIZED`
continue to share declarations without implicitly retaining computed answers.

Vortex-first decision: `implement_shardloom_kernel` for relational iteration and
action-scoped reuse, while reusing the admitted Vortex 0.90 array providers,
native schemas and execution owners. The existing credited native replay in
`local_primitive_relational_batch_replay.rs`, native set membership/difference,
native spill store and cancellation owner supply data/state operations. Neither
Vortex's array API nor its scan API provides ShardLoom's recursive SQL binding
or action completion contract. No new dependency, external executor, Arrow
execution substrate or format-specific operator is introduced.

## Materialized relations

- An explicit materialized declaration has immutable shared identity. Each
  action evaluates its complete input once when demanded, then gives every
  reference the same completed native relation. A new action recomputes it.
  Unreferenced declarations do not execute.
- The native plan exposes the retention boundary. Consumer projection, filter
  or limit cannot be pushed through it if that would change the retained
  relation, input validation, effects, or another consumer's domain.
- The retained owner preserves complete schema, row multiplicity, NULL values,
  native rich values and carried floating bits. It uses credited native arrays
  and the existing optional native replay spill. It is not a public persistent
  table, catalog cache, temporary compatibility file or cross-action answer cache.
- Repeated references must identify the same bound definition. Reject
  inconsistent reuse of an identity, foreign ownership, and unsupported
  parameter-dependent retention before producing a successful result.
- Publish a completed entry only after its input finishes and validation passes.
  Failure, cancellation or early close discards provisional state. Release all
  replay owners before successful spill cleanup and certificate completion.
  Results already delivered to callers retain their existing independent owners.

## Recursive semantics

Admit a nonrecursive anchor followed by `UNION ALL` or `UNION` and one recursive
term. A recursive reference names the immediately preceding iteration's rows,
not all historical rows. The anchor must not reference itself; forward and
mutual recursive declarations are rejected. The recursive term has exactly one
self-reference in an admitted relational position. Unsupported nonlinear,
correlated or data-dependent-schema recursion fails explicitly.

`UNION ALL` keeps every row and duplicate. `UNION` removes duplicate anchor rows
and excludes any recursive row already present in the accumulated result, using
the existing native complete-row set identity, including NULL and the selected
floating policy. This can terminate a finite graph with cycles; it does not
detect every path cycle when other output columns keep changing. Do not silently
drop such rows or substitute a different recursion algorithm.

Execution uses an iterative frontier loop. Complete an iteration, then replace
its frontier; stop on an empty frontier. An empty anchor does not execute the
recursive term. The result includes the anchor and every admitted later
frontier. No row order is promised without an explicit final ORDER BY.
An outer LIMIT is not a termination or correctness proof: complete and validate
the recursive relation before certifying downstream success.

The anchor fixes field order, names and value domains. Top-level nullability is
admitted conservatively for iteration; recursive outputs must match those
domains without implicit numeric widening, decimal rescaling or timezone
reinterpretation. Column lists rename the declared output consistently. Bind
the recursive term against the frontier schema before opening producers.

An explicit positive session `recursion_limit`, default 1,000 recursive-term
evaluations, bounds iteration work independently of parser or plan nesting.
Exactly that many evaluations may run; another required nonempty-frontier step
fails deterministically instead of truncating output. Callers may choose a larger
positive bound through the same context/session and CLI policy. The limit never
grants memory, storage or execution fallback. Cancellation, checked counters,
memory reservations and native spill quota remain independent constraints.

## State, inputs and failure

All frontiers, result replay, visited-row state, schema/plan metadata and replay
containers compete with existing inputs, retained results and operator scratch
for the same session grant. Reuse existing row membership for resident distinct
recursion and native set difference against retained frontiers for spill-backed
distinct recursion. These are correctness paths, not a performance claim; no
new unbounded in-memory visited set is hidden behind spill-enabled execution.

Read-once producers are declared for native replay before their first demand if
iteration or another consumer needs them again. File inputs retain generation
validation. No iteration may reopen an exhausted producer, silently sample
payload to discover a schema, or use a different input generation. Standard
native operators execute each step with fresh mutable state; only explicitly
admitted source/relation replay persists within the action.

Check cancellation during iteration, membership work, replay and source access.
Any source, schema, arithmetic, memory, iteration-limit, spill, consumer or
publication failure invalidates the action. No partial fixpoint becomes a
successful result/certificate/write. Retry starts with fresh frontier, visited
and materialization state under the existing source and publication contracts.

## Evidence and acceptance

Add independently specified complete typed references for finite chains,
branching graphs, duplicate paths, finite cycles, NULLs, empty anchors and
nullable/rich payloads. Cover typed parameters, multiple ordinary/materialized
CTEs, named-plan snapshot replacement, joins, grouping sets/FILTER, windows and
final ordering in one composed workflow. Test malformed self/forward/mutual
references, recursive type drift and unsupported shapes before effects.

Exercise equivalent native declarations through all five public interfaces,
admitted Vortex and compatibility inputs, native persistence and independent
readback. Prove a chain beyond the plan-nesting guard, exact iteration-limit
boundaries, one producer evaluation for explicit materialization, fresh actions,
retained outputs, real spill versus resident refusal, quota denial, cancellation,
early close, changed inputs, failed publication and successful fresh retry.
Record actual retained/spilled state and iteration/materialization work in native
evidence; counters or case totals alone do not establish correctness.

Run focused regressions, default/native Rust gates, common conformance consumers,
installed-package tests and portable evidence replay with corruption controls.
Keep failed attempts. No benchmark, release, universal dialect parity or item-20
completion follows from this continuation alone.

## Semantic reference

The [PostgreSQL recursive-WITH documentation](https://www.postgresql.org/docs/18/queries-with.html#QUERIES-WITH-RECURSIVE)
is a language-behavior reference for anchor/frontier evaluation and the difference
between UNION and UNION ALL. Its materialization and cycle discussion also makes
clear why full-row duplicate elimination cannot detect every changing path cycle.
ShardLoom owns the implementation and its explicit resource, completion, policy
and unsupported-shape contracts; no implementation code or executor is imported.
