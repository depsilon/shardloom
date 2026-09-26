# Shared dictionary/run-end expressions — R1.c

Status: **drop the duplicate shared-transform proposal at source admission**.
The inspected active paths already share the length/domain/time transformations
identified in this audit.
No new runtime behavior, benchmark speedup or general RunEnd capability is claimed.
R1.c closes as a candidate decision; broader profiling and capability gates remain
open. Next is R5.a, direct owned-array handoff between operations.

## Existing implementation and remaining boundaries

| Proposed work | Source-grounded state | Decision |
| --- | --- | --- |
| Share URL length and domain extraction during ingest | `universal_format_io.rs::append_embedded_derived_columns_to_batch` pairs adjacent same-source specs. `embedded_utf8_length_and_url_domain_arrays` visits each plain source value once. The typed dictionary helper visits each dictionary value once, computes both outputs, preserves length keys where valid, and remaps domain codes. | Already implemented; no second text traversal to remove here. |
| Share time extraction during ingest | The same dispatcher pairs minute extraction and minute truncation; `embedded_extract_and_date_trunc_minute_arrays` uses the combined typed helpers. | Already implemented for admitted source types. |
| Share transformed group keys over a dictionary | `local_primitives.rs::update_general_direct_from_transformed_dictionary` counts selected IDs, then transforms each active domain value once per chunk and applies its exact weight. Dense and compact paths have corresponding domain handling. | Already implemented within their explicit admission; not global cross-chunk reuse. |
| Share repeated length measures | `update_weighted_utf8_dictionary_value_with_fusion` computes length once for multiple admitted SUM/AVG/MIN/MAX length consumers; it also shares identity min/max handling. | Already implemented; `value.len()` alongside domain extraction is constant-time byte length, not another string scan. |
| Reuse persisted derived fields | `rewrite_simple_aggregate_for_embedded_derived_columns` rewrites available group, measure and predicate transforms to hidden fields. It deliberately preserves an ordered same-source transformed-dictionary lane with length measures. | Preserve both existing choices; do not claim all queries bypass source transforms. |
| Add RunEnd domain reuse | The inspected ingest helper specializes Arrow dictionaries, not RunEnd. The retained inventory has Zstd for the five original source strings and Dict-related child encodings for derived domains, including RunEnd. A flattened encoding list does not establish a separate repeated RunEnd text-transform workload. | No measured, admitted new target established by this audit. Require such a target before implementing another provider/kernel path. |

The source-dictionary exception is important for workloads such as transformed
grouping with same-source length measures: a preserved source lane can still
decode/intern strings and reconstruct counts. That work is not a newly missing
expression-sharing mechanism. Source ownership and dictionary preparation remain
R2.a/R2.b; repeated invariant dispatch/binding remains C2.a. Do not count those
same opportunities again as a new R1.c win. This audit does not claim those costs
are small or already eliminated.

Generic AND predicates still evaluate their terms separately, and the dictionary
matching helpers can visit a domain for each term. That is an unimplemented
sharing direction, not covered by the existing length/domain fusion. The current
ClickBench substring terms operate on different columns; this audit establishes
no material same-domain repeated-predicate target. Treat a future such workload
as new admission evidence. Do not describe R1.c's disposition as universal
predicate or RunEnd sharing completion.

## Evidence and limits

Source anchors in the audited tree:

- `universal_format_io.rs`: specs at 1532; paired dispatch at 1613; plain combined
  text helper at 1698; combined time helper at 1839; typed dictionary helper at 1987.
- `local_primitives.rs`: shared weighted measures at 24868; dense domain path at
  32391; general active-domain loop at 32593; derived rewrite at 46229; preserved
  source-lane admission at 46292.

The latest retained-file physical inventory was already complete and guarded;
this audit reads that saved report, not the 15.68 GB artifact again. The saved
`172832Z` ingest records 13,033 ms of derived-metadata build and 24,365 ms of nested
source-batch production. These broad spans may overlap other stages. They do not
attribute a duplicate expression traversal or establish exclusive recoverable CPU
time. No seconds are claimed as saved or unsavable from those totals.

Ten relevant existing native tests pass in the already completed R6.b native
suite: dictionary URL metadata, non-i32 keys, transformed group/domain reuse,
selected rows, HAVING after merge, dense/compact state, and exact weighted measures.
The [receipt](../benchmarks/shared-domain-expression-audit-2026-09-26.json) lists
their full names, source hashes, saved evidence identities and admission limits.
No new expensive benchmark or full UAT is warranted for reimplementing the same
mechanism. Keep the implementations and their tests; no prototype was added.

Reopen with a distinct active workload that repeats material domain computation,
or an exact RunEnd/multiplicity provider opportunity with measured cost and
admission. Check the existing Vortex dictionary/RunEnd scalar-function providers
before inventing an abstraction; preserve selection, nulls, domain epochs,
observable errors and reservation ownership. A later candidate must still meet
the frozen suite/query/ingest gate and complete applicable UAT.
