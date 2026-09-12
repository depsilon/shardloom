# UTF8 group and integer DISTINCT workers

This focused continuation of `2ad143da` admits one nonnullable UTF8 identity
group and one nonnullable integer identity `COUNT(DISTINCT value)` measure
through the existing native compound workers. The Struct root is nonnullable.
All eight integer widths preserve original signedness and exact bits. A finite
positive LIMIT plus checked OFFSET is required; order is count descending and
exact UTF8 bytes ascending, with an optional explicit identical tie term.
The admission rule uses types and request structure, never field or query names.

The same request/schema decision controls source CPU-driver restoration and
actual worker admission. HAVING, transformed values, nullable fields, additional
groups and residual predicates retain their existing native admission. Initial
capacity denial may decline before contribution. Once work contributes,
committed-state pressure drains/cancels the existing workers and fails explicitly
before copying into an unbudgeted legacy exact map. Explicit UTF8 DISTINCT spill
is not added; existing integer DISTINCT and compound COUNT spill are unchanged.

## Complete pair reduction

Pinned Vortex 0.85 supplies the held-file scan, logical fields, typed integer
and UTF8 execution, dictionary domains and allocator. The existing ShardLoom
compound kernel retains complete `(integer value, UTF8 group)` pairs and
positive input row weights. Codes only identify values inside their retained
native dictionary owner. Equality compares exact integer bits/signedness and
UTF8 bytes, preserving empty strings, embedded NUL and Unicode normalization
differences. No external query engine or decode-to-Arrow execution is introduced.

This family arranges partials by the text hash while continuing to look up
complete pairs by their full hash and exact equality. Every value of a group
meets in one of the existing 64 partitions. The existing pair COUNT arrangement
loop remains unchanged; text arrangement has its own bounded cancellation and
checked hashing evidence. Partition hash collisions cannot merge unequal groups.

At EOF, after all contribution/reconciliation jobs drain, one occupied complete
pair contributes **one** to its group count. Row weights certify source-row
conservation and never become DISTINCT counts. Reserved domain-count scratch
and reserved per-partition selection coexist with complete pairs. A bounded
global heap selects exact count/text order and copies retained group strings
only after their credits are admitted, including old-plus-replacement overlap.
Only then may full pair state release. Candidate counts describe all complete
groups, including groups outside the requested prefix. There is no per-chunk
top-K approximation or second global unbudgeted string interner.

Worker table/domain/retry owners, completed-index evidence, column names, EOF
scratch and selected strings retain their leases through their payload lifetimes.
Actual completed worker indices are reported; requested or created lanes alone
do not prove execution. Typed source-allocation replay keeps its existing held
source rule, and corruption remains authoritative over concurrent allocation
denial. Provider allocations bypassing HostAllocator, JSON output and process
RSS remain outside this scoped memory accounting.

## Ordinary public result boundary

Completed UTF8 counts enter the existing exact-result finalizer and ordinary
JSON result route. Other output boundaries remain unchanged and are not newly
certified by this focused test matrix. This extraction adds no public owned
UTF8 result API, retained UTF8 preparation, SQL syntax or transport protocol.
The existing optional preparation explicitly returns an unretained operation
for this schema and executes the already held source once. It does not reopen
the source or delegate to another engine. Original integer owned results and
all previously admitted prepared shapes retain their contracts.

## Evidence and current status

The source mechanism comes from `a00b755a` (`9c96da5b` original) and its validated
`8fb50e8a` forms. Its global selected-row owner is reused in a UTF8-only form,
without the unrelated three-key/minute or owned-output families. No new
scheduler, provider, dependency, codec, ingest policy or reader cache is needed.

The broader `8ba36c76` candidate measured Q14 medians of 7.644246583 seconds
for accepted `2ad143da` and 1.562093583 seconds for the candidate, with three
complete paired runs. Renamed duplicate-heavy/mostly-unique/skew profiles at
P1/P12 also passed complete values and actual worker evidence. Those results
justify this extraction; they are not measurements of this new source tree.
The immutable receipts are `phase-query-8ba36c76-p12-r1.json` and
`utf8-distinct-8ba36c76-r1.json` under
`/Users/dylan/LocalData/shardloom/perf-all-20260906`.

Source-only tests cover complete pair identities, dictionary domains, hash
collisions, all integer widths, global winners/ties/offsets, exact source and
worker row totals, nullable/schema denial, initial/committed/EOF pressure,
active cancellation, selected-string ownership and replacement failure.
Ordinary native tests cover complete values and existing public dispositions.
Root's `phase-resume-focused-distinct-r1.json` passes formatter, all19 new tests,
36 existing integer DISTINCT checks,28 compound checks and9 worker-job checks,
plus release-user-surfaces all-target and minimal-native Clippy. Filters overlap;
these counts are not a distinct test total. The source anchor is `0a4c282f`
with formatter-only Rust diff SHA256
`1620fa952c9eeebcde2f9d8b6efffc7bb602d09c9ebbfea01b51fda16f01ea53`.
Required workspace checks and matched performance remain pending.

Focused filters: `utf8_integer_distinct`, `compound_`, `exact_distinct_`,
`aggregate_chunk_jobs`, existing prepared/source-retry and sink controls.
Matched Q11/Q14/Q17/Q23/Q34/Q35, the unchanged 96-call renamed generic screen,
then Full43 govern performance retention. The source-only port advances no
accepted control and makes no competitive claim.
