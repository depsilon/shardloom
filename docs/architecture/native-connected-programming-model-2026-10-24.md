<!-- SPDX-License-Identifier: Apache-2.0 -->

# One composable ShardLoom programming model

Status: accepted October 24 maintainer addition, following the active
[surface-parity](native-user-surface-execution-parity-2026-10-24.md),
[provider-refresh](native-vortex-provider-refresh-2026-10-24.md),
[warm-source reuse](native-warm-source-reuse-2026-10-24.md),
[public naming](shardloom-public-naming-2026-10-24.md),
[ingestion](native-ingest-ownership-preparation-2026-10-24.md),
[rich-data lifecycle](native-rich-data-lifecycle-2026-10-24.md) and
[universal I/O](native-universal-io-performance-coverage-2026-10-24.md) packages.
The subsequent [everyday analytical completion](phased-execution-plan.md#everyday-analytical-completion)
request extends this package's SQL scope and follows it with statistics,
temporal/value semantics and explicit batch functions. It does not reorder the
preceding packages or close CG-1 through CG-23.
The following [AI-data package](native-ai-data-workflows-2026-10-24.md) consumes
these same plans, results and explicit application-function boundaries for
model-ready batches, feature preparation, retrieval and agent access.
Current parity implementation continues first. RFC 0033's CG-21 workflow
contract, RFC 0044's shared native execution, and the existing SQL/expression,
resource, extension and remote-API RFCs remain governing boundaries.

## Product contract

`sl.context(...)` with explicit resources is the ordinary entry point. Read a
file, build DataFrame expressions, name the plan, use it in SQL, continue with a
join/window/filter, then write. Changing syntax must not execute, collect,
serialize rows, create a temporary data file or convert through Arrow.

Use one public DataFrame concept, documented as lazy. Consolidate the existing
wrappers deliberately rather than introducing another parallel class. SQL,
readers, `table()` and transformations return that composable object. `collect()`
returns the existing native-backed `Result`; `to_rows()` remains an explicit
conversion. Batch delivery returns a stream and a write returns a completed
receipt. The preceding naming pass should prepare this terminology without
claiming the new composition contract already exists. With no public adoption,
update actual known callers directly; do not invent compatibility machinery.

The maintainer's review is against `16ff752d`. Current source confirms separate
[`LazyFrame` and `SqlWorkflow`](../../python/src/shardloom/query.py) concepts and
[`SessionLazyFrame` and `SessionSqlWorkflow`](../../python/src/shardloom/session.py)
wrappers. [`SqlSourceBinding`](../../shardloom-cli/src/native_runtime.rs) and
[`NativeRelationDeclaration`](../../shardloom-cli/src/native_runtime_plan.rs) are concrete
binding/ownership seams. This inventory does not establish that every proposed
mixed operation is absent. Recheck the actual APIs, tests and capability index
before implementation; extend their shared machinery instead of recreating it.

## Ordered implementation

1. **Composable SQL and DataFrames.** Bind SQL results and DataFrame plans into
   the same logical relation. The optimizer must see the complete computation
   across the boundary, retaining eligible projection/filter pushdown, native
   grouping, ordering and source reuse. A table-shaped report wrapper is not
   sufficient. Reuse the parity package's normalized binding and reports.
2. **Session-local named plans.** Add create/replace, `table()`, list, schema
   inspection and drop through an in-process catalog. Register immutable plan
   snapshots and source ownership, without executing them. An already-bound
   query keeps its meaning when a registration is replaced or dropped. A view
   is not a result cache, and two references do not imply one stored execution.
   Resolve relations in the binder, never by replacing SQL text. Define name
   normalization, duplicate registration, unknown names and cycles explicitly.
   Cross-context registration must preserve lifetime, authorization and shared
   resource contracts or fail deterministically before effects. A persistent
   metastore is not required for temporary views.
   Extend the existing bounded nonrecursive CTE implementation to scoped plan
   references with nested WITH, shadowing and column-name lists. Replace repeated
   text expansion with bound relations; materialization controls need explicit
   ownership, effects and resource semantics. Prove deep/wide composition beyond
   the current 64-name/256-KiB expansion and 24-level plan bounds using scalable
   traversal and admission. Recursive CTE execution is a following implementation
   group with termination, cycles, iterative state, spill and cancellation defined;
   it is not a permanent exclusion or a prerequisite for accepting ordinary
   composition. Sequence that larger addition with the following analytical
   completion priorities. Audit GROUPING SETS, ROLLUP, CUBE, aggregate
   FILTER and ordered aggregates against actual shared providers before selecting
   missing implementations, coordinated with the statistics/reducer package.
3. **Functions, types and reusable windows.** Expose supported native expressions
   through `shardloom.functions`, schemas/types through `shardloom.types`, and a
   reusable `Window`/specification API. Do not introduce a `shardloom.sql` module
   that collides with the existing callable. Audit basic expressions, aliases,
   casts, exact aggregates/distinct counts, conditionals/nulls, text/byte length,
   regex, temporal operations, nested constructors/access and windows. Reconcile
   existing kernels and constructors first. A function's shared definition
   should drive binding, type/null rules, documentation and native selection;
   do not implement calculations in Python. Preserve decimal scale/precision,
   timestamp units/timezones, nested nullability, frame bounds, ties and null
   ordering throughout readers, expressions, results and writers.
4. **Typed parameters and explicit relation bindings.** Add named scalar
   parameters, including exact decimal/date/null values, through typed syntax
   nodes. Specify positional parameters separately if supported. Missing,
   extra, ambiguous and unsupported parameters need deterministic diagnostics.
   Value parameters, relations and identifiers are distinct contracts, never
   interchangeable string substitution. `relations={...}` uses the same binder
   as the catalog but has query-local scope. Prepared structure reuse must bind
   fresh values and recompute parameter-sensitive pruning/physical decisions;
   prior selection, aggregate state or results must not leak.
5. **Reusable transformations and observations during actions.** Connect the
   existing `plan_transform` mechanism to the unified DataFrame. Python builds
   the plan once; native expressions perform data operations. Named expression
   templates may expand through the same typed binder. Add selected aggregate
   metrics at an explicit plan position during a write or other action, without
   another input pass. Cover counts, null checks, sums and min/max for supported
   types. The optimizer cannot move the observation across a filter, join or
   range when that changes its domain. Publish complete metrics only after
   execution completes; distinguish completed data processing from successful
   output publication. Cancellation, producer errors and failed publication
   cannot become a successful write or complete quality check.
6. **Local standard integrations.** Connect notebook SQL to an explicitly
   selected existing context/catalog. Implement DB-API and columnar ADBC
   adapters around that same plan/session/result contract, including real typed
   parameter binding, schemas/metadata, incremental fetch and cancellation.
   Define support/transaction claims honestly and validate them with independent
   clients. Driver dependencies require the existing license/provider review.
   Do not make a second implicit global session or an alternate compute route.

Explicit application batch functions are requested in the following analytical
completion package, with schema, null/determinism, effects, cancellation,
exceptions, ownership and memory contracts under RFCs 0011/0023. Native plan
composition remains the preferred reusable-logic path. Persistent catalogs and
remote plan submission retain separate transport, recovery and authorization
contracts; they are not prerequisites for local composition. A future
Spark connector needs explicit partitioned exchange; a distributed DataFrame
cannot be treated as a local zero-copy buffer. No Spark/PySpark dependency,
compatibility claim or external query-engine execution is authorized.

## Acceptance is one connected workflow

Use the existing conformance/report tooling to read different supported formats,
filter with expressions, register a plan, query it with nested CTEs and typed parameters, add a
DataFrame join/window, observe selected metrics and write/reopen the result.
Exercise the equivalent query-local relation binding too. Verify complete
values, types, order where defined, publication and metric completion.

Before the action, instrument that registration and syntax composition do not
scan/copy payload, execute producers or create output/preparation files. Inspect
the composed native plan and actual execution report to prove optimization
visibility across boundaries. Metadata/schema inspection remains an explicit,
source-grounded admission activity, not permission to sample data silently.
Test replacing/dropping views after binding, repeated references, close/drop
lifetimes, cross-context rejection/admission, retained results, resource shrink,
parameter changes, malformed names/types, cancellation and partial publication.

Run supported functions through SQL and DataFrame declarations against one
typed semantic reference, including nullable/mixed-numeric and Unicode/byte
cases. Verify reusable window frames and resource behavior, not just spelling.
Notebook/DB-API/ADBC acceptance exercises the same named plans and parameters;
six disconnected happy-path examples do not establish the connected contract.

Freeze finite performance/resource controls before measuring changes. Keep the
fastest correct native path fast, preserve existing engine-time definitions and
exactly-once isolated Full43 rules, and use targeted mixed workloads where
Full43 does not activate the feature. No performance, release or CG completion
claim follows merely from exposing a new namespace or method.

## Primary design references

PySpark is a programming-model reference only. Its documented
[`SparkSession.sql`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html)
returns a DataFrame and supports typed literal parameters. Its classic and
Connect view-resolution timing differs, so ShardLoom must define its own stable
binding-time snapshot semantics rather than inherit accidental behavior.
[`createOrReplaceTempView`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceTempView.html)
documents session-scoped view lifetime. The
[`observe` API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.observe.html)
is a reference for aggregate observations during actions. The
[ADBC specification](https://arrow.apache.org/adbc/current/format/specification.html)
describes its standard columnar interface and points to the canonical API
definitions. Verify those exact definitions when implementing the driver.
