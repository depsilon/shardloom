# Native relational workflows

Status: native Rust and public frontend paths implemented; final validation is in
progress. It continues the [universal workflow plan](universal-workflow-completion-2026-10-01.md)
under PERF-02/03/06/07/10/11/12 and CG-5/6/20/21. The ten unary families and their
accepted output paths remain covered by the [unary contract](native-unary-workflows-2026-10-01.md).
The current unit covers the four relational families in the existing finite
inventory: joins, set operations, analytic windows and scoped subqueries.

## Provider decision

Use pinned Vortex 0.85 arrays, immutable scans, native typed key access,
`ArrayRef::take`, `ChunkedArray` and `StructArray`. Implement ShardLoom's relational
state, planning and resource policy. This is `implement_shardloom_kernel` over
admitted native providers; no dependency or external execution engine is added.

Native take admits duplicate, nonmonotone and nullable indices. It supplies join
multiplicity and outer null extension while retaining the underlying native
payload. Sorted unique scan selections have a different contract and cannot
represent duplicate join matches. Array structural hashing is not row-key equality.
Vortex's float comparison has a different signed-zero contract, so the relational
key boundary below must normalize explicitly rather than inherit it accidentally.

For window ordering, pinned Vortex 0.85 provides native key values, sortedness
statistics and sorted-search operations. It does not provide this SQL partition,
peer, navigation and source-order delivery contract. ShardLoom therefore orders
reserved source ordinals with a fallible, cancellable comparison over native
keys. Identical partition/order specifications share one sort. Native payload
stays in its source owners until final bounded gathering; result columns retain
buffer credits. Whole-partition state is admitted against memory and currently
fails on denied capacity; this is not a window spill claim.

Reconcile the earlier unmerged September 20 relational draft with current source;
preserve that separate checkout. Reuse existing original-width numeric owners,
native UTF8/dictionary access, resource leases, result streams and local writers.
The old SQL row evaluator remains a test oracle, never the public native executor.

## Preparation, execution and composition

Bind typed relational requests and schemas before row execution. File inputs share
one resident session, with repeated references to the same immutable source sharing
its prepared owner. A call owns one cancellable CPU/memory admission. Every source
generation is validated before work and again after the final consumer completes.
An invalidation is terminal for that call; no reopening and replay within the call.
Repeated execution uses fresh operator state and never a cached query answer.

Carry native batches and exact schemas between operations. Bind names and types
without sampling values. Preserve authoritative empty schemas, root and field
validity, row identity and original integer widths. Apply source projection and
safe predicates before state construction. Retain payload as native arrays and
gather only admitted output rows. Key execution and final serialization are
explicit materialization boundaries, with no zero-decode or zero-copy claim.

Small collection keeps the existing 65,536-row and 8-MiB limits. Complete local
writes use bounded batches and the same execution, without a JSON intermediate or
query replay. Provisional batches and staging files are not successful publication.

## Semantic contracts

- Equijoins admit inner, left, right, full, left-semi and left-anti forms.
  Composite equality matches only when every key pair is nonnull and equal.
  Ordinary joins retain every duplicate pair; semi/anti emit each qualifying left
  row once. Unordered output follows left source order, right source order within
  each match, then unmatched right source order. ON precedes outer null extension;
  WHERE follows it. Cross and existing non-equi forms use the same ownership and
  output boundaries, with their predicate semantics checked independently.
- Integer key equality is exact across widths and signedness, without a floating
  conversion. Finite floating keys compare in their widened floating domain with
  signed zeros equal. Mixed integer/float keys need an explicit cast. Nonfinite,
  nested, extension and binary keys remain rejected until separately admitted.
  UTF8 uses exact bytes and boolean keys retain their type. Hash equality only
  selects candidates; complete value equality proves a match.
- UNION ALL preserves branch order and duplicates. UNION DISTINCT, INTERSECT and
  EXCEPT use explicit set semantics, including null-equal row membership and first
  occurrence order before an explicit final sort. Align columns by position and
  bind a lossless common schema before execution; reject unproved coercions.
- Analytic windows cover the existing parsed ranking, navigation and distribution
  functions. Partition keys, order keys, null ordering, peer equality, stable ties,
  offsets/defaults and result types are bound explicitly. Preserve input row order
  unless a final ORDER BY changes it. Source-order rolling is a separate family;
  it does not establish general window-frame support.
- Scoped predicate subqueries cover the existing scalar-column and row-valued
  IN/NOT IN, comparison ANY/ALL and EXISTS/NOT EXISTS inventory, including its
  explicitly correlated and projected inner plans. IN/NOT IN retain right-side
  null and empty-set information for SQL three-valued logic; empty ANY is false
  and empty ALL is true. Correlation is admitted only through an explicit native
  plan; no per-row external execution or decoded evaluator shortcut. A scalar-value
  subquery in a SELECT expression or comparison RHS is outside the existing
  parsed inventory and requires a separate capability contract.

The SQL statement, set and recursive projected-subquery parsers are inert: source
reads and subquery materialization begin in the later decoded-reference preparation
functions. Native lowering must consume the parsed relations before that stage.
The existing predicate-to-expression helper expands already-materialized subquery
values and cannot be reused for parsed subquery nodes with empty value caches.

## Native composition operators

The relational tree also binds the existing core expression IR to native column
kernels. Projection, SQL three-valued filtering, stable lexicographic ordering,
and exact output ranges compose without constructing decoded row maps. Native
key comparison preserves exact cross-width integers and null semantics. Arithmetic
uses checked integer operations and explicit floating coercion; casts bind their
result schemas before reading values. Conditional branches evaluate only selected
rows. Unicode transforms, predicates, substring operations, and replacements use
admitted string scratch and allocator-owned result buffers. Regex compilation has
an explicit size limit. These are ShardLoom scalar kernels over Vortex arrays;
the older decoded SQL evaluator is not an execution provider.

Grouping reuses the native null-equal set/index boundary, retains only unique key
payloads, and records first-occurrence group order. COUNT DISTINCT uses exact
native `(group ordinal, value)` membership and excludes null measure values.
MIN/MAX preserve source widths and selected values, including floating signed
zero; string extrema receive compact owned buffers. SUM/AVG retain the existing
ordered floating accumulation policy across input batches. Scalar aggregation of
empty input emits one typed row; grouped empty input emits no groups. HAVING,
computed arguments, final projection, and ordering are separate typed nodes.
These states currently require admitted memory; they do not establish general
aggregate or sort spill support. Output ranges currently bound delivery; they do
not certify early termination of every upstream operator.

Native ON predicates evaluate bounded candidate pairs before outer null extension;
keyed and non-equi joins share the same NULL, multiplicity and output contracts.
Parameterized subqueries bind each outer row as a native singleton input to the
inner tree, so its grouping, HAVING, ordering and limit run in that row's scope.
Nested parameters refer to the nearest scope. They reuse prepared source readers
but currently rescan inner inputs per outer row; this is not a decorrelation or
performance claim. Query state is fresh and charged to the same call's grant.

SQL lowers the inert parser's admitted relational nodes directly into this tree.
Schema lookup and final binding share the retained readers. Backward column demand
keeps join keys, ON/WHERE expressions, window keys and nested outer-scope references,
while removing unused source payload columns. Sources can contain more than 128
columns when the required intermediate schema fits the admitted width. Explicit
expressions remain type-checked even when a later projection does not retain them.
Only unconditional cross-input equality conjuncts become join keys; the complete
ON predicate still evaluates, and equality under OR/NOT is not extracted.

## Public paths and source declarations

CLI `run sql --sql ... --bounded true`, Python `ctx.sql(...).collect()` and
admitted DataFrame chains select `native_vortex_relational_collect`. The matching
`native_vortex_relational_write` route carries native batches directly to Vortex,
Parquet, Arrow IPC, Avro, ORC, JSON, JSONL and CSV. It does not require a query LIMIT.
Small collection remains bounded independently. A one-column CSV null record is
written as a quoted empty field so readers retain the record; CSV still lacks
static types and does not distinguish an empty string from null by itself.

All parsed source leaves, including nested/projected predicates and set branches,
normalize once to generation-bound Vortex inputs. Source replacement checks cover
both the normalized artifact and its original compatibility input, including each
writer's final consumer and commit checks. An output cannot alias either source.
String literals that happen to contain a source path are never rewritten.

DataFrame source formats and declared compatibility schemas survive joins, set
operations, limits and nested predicate helpers. The transport uses
`--source-bindings` with a JSON object mapping source URIs to `input_format` and an
optional `source_schema` string such as `key:utf8,amount:int64`. The low-level Python
client accepts the corresponding `source_bindings` mapping and ordinary schema
mapping/sequence syntax. Explicit formats take precedence over filename suffixes.
Conflicting or unused declarations fail explicitly; Vortex's embedded schema is
authoritative. Native DataFrame schema hints are not sent as ingest overrides.
Route inspection parses source references and declarations without source I/O.
Execution can prepare compatibility inputs before source-dependent name/type
binding fails; such failure never publishes a result or invokes another engine.

Analytic SQL supports ROW_NUMBER, RANK, DENSE_RANK, LAG/LEAD with a nonnegative
offset and null default, NTILE, PERCENT_RANK and CUME_DIST. Final ORDER BY can use
a window alias. Predicate subqueries support the parsed IN/NOT IN, row-valued IN,
ANY/ALL and EXISTS/NOT EXISTS forms, including grouped and explicitly correlated
inner plans. Native subquery limits use native resource admission; the decoded
reference evaluator keeps its separate 32-value materialization bound.

The public finite grammar and the Rust tree remain distinct. In particular, the
DataFrame flat join renderer rejects pre-join transformations or a transformed
right-hand frame instead of moving or discarding them. Source-subquery helpers
likewise reject transformed frames; use their explicit predicate/group/order/limit
arguments. General derived tables, arbitrary chains, scalar-value subqueries,
arbitrary window frames/default expressions and relational fanout remain pending.
The current relational payload admits bool, original-width integers, F32/F64 and
UTF8 with validity. Binary, F16, nested and extension payloads require further
native admission. Numeric nonfinite keys remain unsupported.

Public repeated calls retain the request, source readers and lowering, with fresh
operator state per call. A request or resource change replaces the handle; source
invalidation fails that call and clears the handle. The report contains complete
rows for small collection, actual prepared-source opens, completed execution
counts and a certificate for the same execution as the writer. Provider decoder
bytes remain uninstrumented and reservation accounting excludes upstream scratch.

`scripts/run_native_relational_uat.py` checks 16 renamed-schema SQL/DataFrame
workflows: three collections and all eight local writers per case, for 176 complete
checks. Binary writers are reopened through native input; text writers are checked
against independent literal values. The cases include duplicate/null outer joins,
ON filtering, all four set forms, ranking/navigation windows, empty output,
correlated grouping, nested predicates and declared leading-zero string keys.
The harness freezes source generations, binary, Python and harness identities and
uses the serial local storage/process guards. This is correctness and availability
evidence; no speedup or total-RSS bound is claimed.

## Resources and acceptance

Reserve native key owners, lookup capacity, duplicate links, matched-row bits,
sort ordinals, output indices and metadata before allocation, including overlap
during growth. Check cardinality arithmetic. Consumer backpressure and cancellation
must propagate through every stage. Resource denial is never output truncation.
Owned buffers keep their credits until their final owner drops.

Broader state requires a real native spill adapter. COUNT/DISTINCT reducers do not
establish join or window spill support. Extend the existing run store with exact
schema/source/partition identities, ordinals, multiplicity, skew processing,
checksums, quotas and cleanup before claiming those pressure transitions. Preserve
deterministic output across spill. Report unsupported spill and upstream allocator
exclusions directly; reservation accounting is not a total process RSS bound.

Before promoting a public shape, validate its complete values, schema, nulls and
order through Rust, CLI, SQL and Python/DataFrame calls and every admitted writer.
Use renamed schemas, independent literal/oracle results, empty sides, nullable and
duplicate keys, integer boundaries, signed zero, independent dictionary domains,
hash collisions, output above collection bounds and repeated source reuse. Verify
denial, cancellation, mutation, owner cleanup and each claimed spill/recovery path.
Run workspace/native gates and Full43 on the final immutable implementation.
Record exactly which shapes pass; this unit cannot close unimplemented type,
adapter, spill or resource obligations by inference. All execution retains
`fallback_attempted=false` and `external_engine_invoked=false`.
