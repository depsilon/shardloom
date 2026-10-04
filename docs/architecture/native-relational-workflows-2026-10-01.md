# Native relational workflows

Status: merged in [PR #1500](https://github.com/depsilon/shardloom/pull/1500) on
October 2 after all 40 hosted checks passed. Merge `588bf4c7be6c42262f3e2dd2022422efd994e5c8`
has the identical tree to tested head `d6329c9aca3317b3c82740d10e68b7b22c6459ef`.
No review threads remained; hosted Codex review was unavailable due to its usage
limit, so no automated review approval is claimed. It continues the
[universal workflow plan](universal-workflow-completion-2026-10-01.md)
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
  signed zeros equal. Mixed integer/float keys need an explicit cast. UTF8 uses
  exact bytes and boolean keys retain their type. The [typed key contract](native-typed-keys-2026-10-03.md)
  extends flat binary, exact Decimal128 (matching precision and scale), Date32
  and timezone-free timestamp-microsecond equality, hashing and ordering through
  joins, sets, groups, windows and subqueries. COUNT/COUNT DISTINCT/MIN/MAX,
  same-type comparisons, IS NULL, CASE, COALESCE and NULLIF are admitted. Casts,
  arithmetic/rescaling, retained unary state, nested key equality, other extensions
  and nonfinite numeric keys remain separate unsupported boundaries. Hash
  equality only selects candidates; complete value equality proves a match.
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

The public finite grammar and the Rust tree remain distinct. At PR #1500, the
DataFrame flat renderer rejected transformed join and subquery inputs. The
[October 2 composition continuation](native-relational-composition-2026-10-02.md)
adds derived relations, ordered chains, transformed operands and post-set stages
through the existing native tree. Scalar-value subqueries, arbitrary window
frames/default expressions and relational fanout remain pending.
The current relational payload admits bool, original-width integers, F32/F64,
UTF8, binary, exact Decimal128, Date32 and timezone-free microsecond timestamps
with validity. F16 and other extension payloads require further native admission.
Nested values remain payloads only: nested key equality is unsupported. Typed
key comparison/aggregate/expression scope is tracked in the [typed key contract](native-typed-keys-2026-10-03.md).

Public repeated calls retain the request, source readers and lowering, with fresh
operator state per call. A request or resource change replaces the handle; source
invalidation fails that call and clears the handle. The report contains complete
rows for small collection, actual prepared-source opens, completed execution
counts and a certificate for the same execution as the writer. Provider decoder
bytes remain uninstrumented and reservation accounting excludes upstream scratch.

The PR #1500 version of `scripts/run_native_relational_uat.py` checks 16 renamed-schema SQL/DataFrame
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

## Local validation

Runtime, Python and harness revision `344adfb8cc1007105af07b2ea500fd4b2f8a0e53`
was built with Rust 1.99 and `release-user-surfaces`. Frozen executable SHA-256:
`01b397b355b59d9a1d1c8560b88ffbb01ebf5b54b437b533e6fbc01cce34db30`.

| Check | Result |
| --- | --- |
| Default workspace all-target tests | 3,436 passed across 102 targets |
| Native Vortex library with `release-user-surfaces` | 2,167 passed; 23 existing ignored tests |
| CLI all-target tests with `release-user-surfaces` | 1,551 passed across 75 targets |
| Python suite | 693 passed; 144 existing retired/environment-dependent skips; 837 total |
| Formatting and default/native all-target Clippy | Passed, with warnings denied |
| Front-door, local-sink and user-route capability documentation validators | Passed |

The broad native library and Python runs precede the final CLI-only preparation
reporting correction. Final CLI all-target tests and native all-target Clippy cover
that correction; no library, Python or harness source changed after those broad
checks. The packet retains the exact commands, order, logs and hashes.

The frozen-build Python receipt is
`relational-workflows-20261001/python-uat/logs/native_relational_20261002T062126509199Z/summary.json`,
SHA-256 `a2944458b56b2b9ed5098a5679e12f4e610a95a80496634d1620dc8d34335b92`.
It verifies all **176 complete results**. The packet contains all 322 public
request envelopes, six source-file hashes and unchanged binary, Python client,
query builder and harness identities.

Full43 passes **129/129** complete-value comparisons across all 43 queries.
Receipt: `clickbench-100m-uat/logs/full43_20261002T062148955236Z/summary.json`,
SHA-256 `1bfdaf5126724d102853056c42268ab1ec5f184b261d7677cad7dbd69f14d09e`.
Each query executes three times in a fresh process under a 24-GiB/12-worker policy.
The resident 15,682,956,489-byte source is fully hashed before and after acceptance;
all 43 retained reference identities and compressed/raw output-log hashes match.
These references provide native regression evidence, not an independent oracle.
OS page cache and ordinary host activity are uncontrolled. All guarded runs pass,
owned locks are released, and the frozen source and executable remain unchanged.

The [portable acceptance packet](../benchmarks/evidence/native-relational-workflows-2026-10-01.json.xz)
contains the build, local tests, public envelopes, Full43 records, reference
identities, supervisors and verifier sources. Its SHA-256 is
`5277489759d1f05589988ebdbddaa54dbd4cb86efe4e1d16e2bfb2cbc3cbd5ca`.
Rebind path placeholders to resident local inputs when replaying the checked-in
runner. This packet records local acceptance before hosted PR checks; its scope
and timestamps remain immutable. It does not establish a performance improvement,
total-RSS limit, broader type/spill support, competitive-gate completion or package
publication.

### Lean-build correction

Hosted CI exposed a missing direct Serde `derive` feature in the CLI manifest.
The all-target test dependencies and native providers had enabled it transitively,
masking the omission in the initial local checks. The lean workspace check reproduced
the failure. Revision `eeb0e0cf1b76e964f73d23ee4618527def567d5c` enables that already
locked feature explicitly. No Rust, Python, harness or lockfile source changed.

Default and no-default workspace checks, Rust 1.96 no-default and native all-target
checks, a plain CLI build, formatting, default/native all-target Clippy and default
workspace tests all pass after the correction. Rebuilding `release-user-surfaces`
produces exactly the frozen executable bytes above; a complete file comparison and
SHA-256 verification confirm that the original workflow and Full43 runtime evidence
still applies. The [CI correction packet](../benchmarks/evidence/native-relational-ci-2026-10-02.json.xz)
preserves the failure, nine successful local checks, build and byte-comparison proof.
Its SHA-256 is `a0f682c82aa959b8731b51cc1af1201658e4e1841e31dea00285ec834c6a5b5e`.
