# Native relational composition

Status: merged in [PR #1501](https://github.com/depsilon/shardloom/pull/1501) at
`71036191` after all 40 hosted checks passed. The merged and tested trees match.
Hosted Codex review was usage-limited; that is not a review approval. This continues
[universal workflow completion](universal-workflow-completion-2026-10-01.md)
and the [native relational family](native-relational-workflows-2026-10-01.md)
under PERF-02/07/10/11/12 and CG-5/20/21. It does not close the wider type,
adapter, resource-accounting or spill obligations.

## Contract

SQL derived relations and ordered DataFrame transformations lower into the existing
native relational tree. A derived relation owns a parsed SELECT or set expression;
it is never a fabricated filesystem path or a textual source replacement. Parsing,
source discovery and route inspection remain inert. Every actual source leaf uses
the existing normalization, declared-schema and generation-bound preparation path.

The frontend preserves operation order. A filter after a limit sees only the limited
rows; a limit before a join bounds that input; a filter after a window sees computed
window columns; a transformation on the right of a join executes on that input.
Nested SELECT boundaries make these distinctions explicit. No predicate is moved
across a limit, aggregate, window or outer join merely to fit the older flat renderer.
Set branches retain their own ordering and limits independently of the final result.

The admitted composition vocabulary is the existing native scan, filter, expression
projection, aggregate/HAVING, DISTINCT, stable order, limit, join, analytic window,
set and scoped predicate-subquery nodes. This contract does not infer composition
support for separate sampling, pivot, melt, rolling, tail or row-index families.
Those need their own lowering and resource proof. Scalar-value subqueries, lateral
derived relations and general SQL window frames remain separate work.

## Binding and frontend ownership

- Represent local and derived sources explicitly in the shared inert parser. A
  parenthesized query has an explicit alias. Parentheses and quoted values delimit
  nested FROM/JOIN/ON and set clauses; nested keywords cannot become outer clauses.
- Bind each derived output from its declared native schema, including empty results.
  Aliases are scoped to their query. Unknown and ambiguous columns fail before row
  execution; inner source aliases do not leak through a derived output. Preserve
  existing explicit output aliases and qualified join-column contracts.
- Resolve unquoted table identifiers against explicit primary/source declarations
  in the shared native binder, including nested and set inputs. Quoted file paths
  remain exact; ambiguous declared names fail before source access. Binding never
  rewrites matching text inside SQL literals.
- Python retains transformed join operands and source declarations. SQL remains the
  frontend transport into the same native tree; this work does not introduce a new
  public plan-JSON protocol or another execution provider.
- Computed-column chains use the preceding stage's schema and values. Replacement
  must retain column position and be distinct from adding a new name; no expression
  may accidentally read its own new value or discard an earlier computation.
  When Python lacks declared output names, it transports this intent using the
  explicit ShardLoom SQL modifier `SELECT * REPLACE OR ADD (expression AS name)`.
  The native binder replaces an exact existing name in place, or appends a new
  name. All expressions in one modifier bind against the preceding input; duplicate
  aliases, raw columns, aggregates and windows inside the modifier are rejected.
  This is a ShardLoom dialect extension, not a general SQL compatibility claim.
  Ordinary `SELECT *, expression AS name` retains its duplicate-name rejection.
- Source-subquery helpers may consume a transformed frame by wrapping its complete
  relation. Their explicit predicate/group/order/limit arguments remain later stages.
  Set-result transformations likewise apply to the complete set expression.
- The decoded reference evaluator is a separate testing boundary. A newly parsed
  shape must fail explicitly there unless that reference path supports it; it must
  never become the public native executor.

## Shared execution and resources

Use the existing Vortex 0.85 native arrays, schema binding, pruning, prepared readers,
relational kernels, call admission, cancellation and bounded result consumers.
The provider decision remains `implement_shardloom_kernel` for SQL composition over
those existing native providers. No dependency or execution engine is added.

Keep backward column demand correct through nested projections and joins, retaining
all semantic keys and predicates. Bind explicit expressions even if their output is
later discarded. Scope reuse to source preparation and typed plans; every call has
fresh operator state and performs the query. Complete writes consume that same
execution through the existing eight local sinks. Source/output alias protection and
generation validation include all nested leaves and original compatibility inputs.

PulseWeave and capillary admission stay below frontend syntax. A nested SQL boundary
does not grant a new CPU/memory pool, remove state admission or force an intermediate
file. Preserve consumer backpressure, cancellation and owner cleanup. Existing
reservation exclusions for upstream scratch remain explicit. No new spill support,
total-RSS bound, early-termination or performance claim follows from composition.
Small collection retains its existing row/field/byte limits; complete writes retain
their independent bounded-batch contract. All execution reports
`fallback_attempted=false` and `external_engine_invoked=false`.

## Reuse ownership

The October 2 maintainer clarification makes modular reuse an acceptance requirement
for both breadth and ClickBench-relative optimization work. This composition unit
adds parsing, binding and ordered frontend lowering. It adds no row-execution kernel,
format-specific executor, Python evaluator or intermediate-output protocol.

| Responsibility | Reused owner | Composition behavior |
| --- | --- | --- |
| Ordered Python stages | `python/src/shardloom/_relational_sql.py::render_stages` | LazyFrame and SQL-result methods share the same renderer; it emits inert syntax only. |
| Native source preparation | `public_relational_sources.rs::Sources`, `prepared_source_binding`, `ResidentVortexSession` | Actual leaves share normalization, schema admission, retained readers and generation checks, including repeated and nested references. |
| Native plan and execution | `relational_query`, `prepare_relational_with_schema`, `PreparedVortexRelational` | Every derived boundary lowers into existing nodes in one resource envelope; it does not create another runtime. |
| Numeric ownership and text hashing | `native_numeric_owner::NativeNumericOwner`, `compound_count_partial::string_hash` | Relational keys reuse components also consumed by optimized aggregate, distinct and sort paths. Original widths, validity and dictionary ownership stay below frontend syntax. |
| Matching, grouping and ordering | `native_relational_keys`, `native_relational_index`, `native_relational_set`, `native_relational_order` | Join, set, aggregate, window and subquery operators share native key and state components where their semantics match. |
| Delivery and persistence | `result_batch`, `completed_result`, `native_sink`, columnar compatibility sinks | Collection and all eight writers consume the same execution; serializers own format-specific representation only. |

The general relational aggregate and specialized scan-aggregate strategies are
distinct native algorithms today. Shared ownership/hash/sink components do not prove
that a composed aggregate executes every ClickBench specialization. Future PERF-10
work must name the existing component, its other callers, the exact semantic gap and
the shared extension or strategy it needs before adding another implementation.
Reuse a provider or extract a common component where contracts match; retain distinct
strategies only for documented ordering, typing, state or materialization differences.
Validate the affected existing callers and a non-ClickBench composed workflow, then
apply the existing measured retain/drop gate. Do not infer speedup from this map.

The decoded SQL reference evaluator remains a separate test/proof boundary. Derived
relations fail explicitly there; public composition dispatches to the native plan.

## Acceptance

Freeze independently specified complete results for order-sensitive chains:

- sort/limit/filter/project, repeated filters and projections, computed columns and
  replacements, aggregate/filter/regroup, and window/filter/project/order;
- transformed left and right inputs through duplicate/null inner and outer joins,
  then further projection, filtering, grouping and another joined input;
- ordered/limited set branches, post-set transformations and set inputs to joins;
- transformed IN/NOT IN, ANY/ALL and EXISTS inputs, including nested source bindings;
- renamed schemas, empty relations, source replacement, declared string keys,
  malformed scopes, ambiguous names, admission denial and cancellation.

Verify SQL, CLI and Python/DataFrame parity, repeated retained execution and all
eight writers with reopened complete outputs. Include a result above the small
collection boundary and prove a failed consumer cannot publish success. Run inert
parser/source-discovery checks without existing source files. Keep existing flat
grammar and no-default-feature builds covered.

Focused parser/lowering and Python tests precede required workspace/native gates.
Freeze the final implementation, executable, sources and harness before public UAT
and Full43 regression acceptance. Large checks remain serial under the local storage
and process guards. Full43 is regression evidence; the new independent result matrix
establishes composition correctness. No package publication or speedup is implied.

## Local validation

Runtime, Python and harness revision `cf406611d97a4eb23cc3e26e199e0553f4371b67`
was built with Rust 1.99 and `release-user-surfaces`. Frozen executable SHA-256:
`92d03b93899ecfc54e60e2ea4ec75af4f8d48056a069da8ffedba5f3167d72e4`.

| Check | Result |
| --- | --- |
| Default workspace all-target tests | 3,439 passed |
| Native Vortex library with `release-user-surfaces` | 2,167 passed; 23 existing ignored tests |
| Native CLI all-target tests | 1,562 passed |
| Python suite | 699 passed; 144 existing retired/environment-dependent skips; 843 total |
| Formatting and default/native all-target Clippy | Passed, with warnings denied |
| No-default-feature workspace and Rust 1.96 lean/native builds | Passed |
| Public surface index, front-door, local-sink and route validators | Passed |

The 46-case public matrix passes **560 complete-result checks**: three retained
collections, SQL parity and all eight reopened local writers for each case, plus
eight complete writes of a 65,541-row derived result. The latter collection fails
explicitly at its unchanged 65,536-row limit. The packet retains all 1,217 public
envelopes, including that expected diagnostic with no partial result or success
certificate. Source generations and hashes, the executable, Python client, query
builder, shared stage renderer and both harness files are unchanged after the run.
Receipt:
`native-composition-20261002/python-uat/logs/native_relational_20261002T090434501005Z/summary.json`,
SHA-256 `1ab31c49a669f84fd91a4e72157d09830a44a4af29fc30d36c6c0960f7baa8c8`.

The new CLI tests cover empty output schemas, unknown/ambiguous scope, inert
source discovery, resource denial, failed consumers, midstream cancellation,
repeated execution and nested-source mutation. Every writer rejects native source
paths, hardlinks and symlink aliases. Existing shared sink tests, rerun in the
native suite, verify late errors, cancellation and mutation of native or original
compatibility inputs leave no published prefix, leaked reservations or owned files.
Derived SQL adds no second writer implementation to obtain this behavior.

Full43 passes **129/129 complete-value comparisons** across all 43 queries, each
executed three times in a fresh process under the existing 24-GiB/12-worker policy.
Receipt: `clickbench-100m-uat/logs/full43_20261002T090515037416Z/summary.json`,
SHA-256 `b5e80493dcb499d59cebc0b65fd1a3b9ce1f9810db3ca3e876818b7997fea75b`.
The resident 15,682,956,489-byte input is fully hashed before and after acceptance;
all 43 retained reference identities and compressed/raw output-log hashes match.
These are native regression references, not an independent oracle. OS page cache
and ordinary host activity are uncontrolled. Owned locks are released.

The [portable acceptance packet](../benchmarks/evidence/native-relational-composition-2026-10-02.json.xz)
contains the build, complete public envelopes, local checks and resolved gate
failures, Full43 records, reference identities, supervisors and verification code.
Its SHA-256 is `a926eba8e723f98c78cf3c1ee880c6330a18ee3276ae6c1af7f793159ec535e9`.
Rebind path placeholders to resident local inputs when replaying the checked-in
runner. This immutable packet predates hosted PR checks. Earlier development
attempts and their guard failures remain separate retained evidence; the final
matrix uses a fresh admitted workspace without raising storage limits.

The admitted SQL remains bounded to 256 KiB and 24 nesting levels. This unit does
not add the separate unary reshape/sampling/rolling nodes to the relational tree,
wider payload types, relational spill or fanout. It does not establish a speedup,
total-RSS bound, production certification, competitive-gate completion or package
publication. Those obligations remain with the existing PERF and CG owners.
