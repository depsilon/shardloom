# Native positive predicate proof protocol

Status: prospective semantic and measurement contract for item 23's capability
continuity audit. This document adds no runtime consumer or support claim.
Existing exact unfiltered metadata count and conservative no-match pruning
remain the controls. The dropped B3 encoded-reduction implementation stays
dropped; its original results and disposition are unchanged.

## Decision and owner

Use the existing bound source-demand, predicate proof and certificate owners.
An admitted fact belongs to a source generation, physical region, logical field
mapping and typed predicate. It is not a global property inferred from a format
name, filename, row hint, sample or a previous execution. No new query engine,
cache, public syntax or external execution fallback is involved.

The current source consumers are
`universal_format_io/demanded/statistics.rs`, `demanded/orc.rs`,
`demanded/metadata_count.rs` and `local_primitive_relational_metadata_count.rs`.
They currently do not establish a filtered-count positive proof. A future
consumer must pass the protocol below before entering an accepted runtime.

## Semantic admission

Each region has one of three outcomes for SQL row selection:

| Outcome | Required fact | Permitted consequence |
| --- | --- | --- |
| All match | Every row in the complete region evaluates to SQL TRUE | Omit this predicate's evaluation only when all other obligations below hold. |
| No match | No row evaluates to SQL TRUE; FALSE and UNKNOWN both fail selection | Omit this consumer's region work only when the shared demand union permits it. |
| Unresolved | Any missing, inexact, incompatible or insufficient fact | Retain the ordinary native predicate and its required columns. |

Admission requires all of the following:

1. Exact region coverage: validated nonnegative counts, checked file/region
   totals, no gaps/overlap, and stable generation and directory/manifest
   membership. An empty region contributes zero without implying value
   validation. Bounds are associated with the physical field by each file's
   schema, including reordered union fields.
2. Exact type and null semantics: initial positive-proof domains are the
   already admitted primitive integers, Boolean and exact UTF-8 order. Missing
   nullable fields and Hive constants come from admitted logical mappings.
   Ordinary comparisons with nulls cannot be all-match without a proved zero
   null count. `IS NULL` and `IS NOT NULL` use exact null cardinality. Missing
   null counts, truncated string bounds, nested leaves, floating/special values,
   decimals, temporal conversions and collation changes remain unresolved
   unless a separate domain contract and tests admit them.
3. A bound, pure and proved infallible predicate: no casts with possible
   failure, scalar callbacks, effects, external reads, or hidden evaluation.
   An all-match child of OR or no-match child of AND does not authorize dropping
   another observable expression. Boolean composition must preserve the
   language's three-valued selection and error rules; inability to prove that
   obligation is a decline, not a guessed short circuit.
4. Residual and validation obligations remain: fact consumption cannot remove
   an independent residual, computed projection, required strict-float check,
   complete-source ingestion validation, source check, cancellation check, or
   publication barrier. Statistics certify a bounded predicate fact, not the
   integrity of undemanded payload pages. Any saved I/O claim is scoped to the
   ordinary demanded query contract.
5. Every consumer is accounted for. Predicate-only columns may be omitted only
   when no remaining consumer, expression, result or validation needs them.
   One consumer's all/no-match proof cannot narrow a shared producer whose
   other consumer has unresolved demand. Aliases and direct projections may
   preserve facts through explicit field mappings; computations do not inherit
   facts merely because their output names resemble the input.

An exact filtered count may sum all-match region cardinalities and zero from
no-match regions only when the entire relevant source is covered. Any
unresolved region executes the ordinary native filter/count for that region,
or the complete optimization declines if the consumer cannot combine the two
paths without changing semantics. Prefixes, offsets, distinct counts, joins,
fanout and scalar subqueries require their own bound-plan proof; unfiltered
metadata cardinality cannot be substituted for them.

## Executable correctness gate

Freeze fixture identities and independent expected results before candidate
execution. The positive/negative grid must include all-match, no-match and
unresolved regions in one source as well as each outcome alone. Cover empty,
all-null and mixed-null regions; signed/unsigned endpoints; absent/inexact
statistics; invalid file/region totals; non-ASCII strings; and predicates whose
strict inequalities touch extrema. Include typed-null missing fields, null and
non-null Hive constants, reordered union members, and partition-name collisions.

Exercise aliases and plain projections, conjunction/disjunction controls,
fallible and effectful declines, independent residuals, late invalid values,
source mutation and membership changes. Verify a shared consumer that needs a
predicate-only field prevents its omission. Compare single and fanout outputs,
all five public interfaces, and ample/pressured shared grants. Require exact
complete schemas, row order or multiplicity as declared, values and diagnostics;
external oracles are tests only.

Tests must assert actual reads/decoded rows/evaluation where observed, alongside
typed considered/admitted/declined/executed decisions and their reasons. Prove
that a newly skipped predicate or read would occur in the control; a decision
flag alone is insufficient. Missing work counters remain unavailable. Require
owner refunds, cancellation/drain and unchanged output publication guarantees.

## Separate measurement gate

Before any positive-proof performance run, freeze the candidate identity, the
matched accepted runtime, exact case list/order, source/result identities,
resource limits, deadlines and engine clock. Include both admitted and declined
controls, complete workflows and the unchanged analytical regression cohort.
Preserve every case and failed observation. Reuse accepted competitor evidence;
do not run another engine or Full43 under this protocol's authority.

Run candidate queries in isolation after correctness and resource gates pass.
Record physical work separately from elapsed engine time; reduced decoded rows
alone are not a speedup. Apply the standing complete-cohort binary gain/loss
rule under unchanged limits: a lower complete engine total may ship after all
gates; an unchanged/higher total drops. Preserve any explicit maintainer
exception as a separate decision. Item 18's accepted I/O tradeoff is not an
exception for this candidate. No individual all-match win can hide losses in
the complete declared cohort, and no result revives B3 unchanged.

## Risks and limits

The principal risks are incomplete coverage, incorrect null/type mapping,
silenced errors, lost shared-consumer demand, and certificates describing a
planned optimization that never executed. Unknown facts decline cleanly to the
existing ShardLoom-native path. Persisted sortedness, general runtime filters,
skew adaptation and certified caches retain their independent contracts and
are outside this proof-consumer protocol.
