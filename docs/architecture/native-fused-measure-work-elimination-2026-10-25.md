<!-- SPDX-License-Identifier: Apache-2.0 -->

# Consume computed integer measures without an intermediate column

Status: historical candidate protocol; the maintainer subsequently drops the
whole item-15 batch after its combined Full43 checkpoint. The implementation is
removed; the [drop record](../benchmarks/native-workflow-drop-2026-10-25.md)
preserves the decision and restored-main check. The protocol below retains its
original premeasurement scope and earlier failures.
This is the second mechanism in item 15's workflow work-elimination package.
The comparison baseline includes the separately retained counted join on merged
#1605 (`167ac248ba662ae7967a7801995e9b160b17bae5`). RFC 0044 and RFCs
0013–0017/0021–0022 retain ownership of resources, semantics and execution.

## Existing path and provider decision

The composed native scan/filter/project/aggregate path constructs a full integer
projection, then prepares that output as an aggregate value column. The proposed
consumer removes that computed column and prepares its original integer source
once. It reuses the existing expression arithmetic, exact grouping, ordered
aggregate updates and finalization. Scanning, predicate evaluation and selected
input gathering remain unchanged. This makes one concrete intermediate removable
without changing validation of other filtered columns.

Pinned Vortex 0.90.0 provides the existing ArrayRef, Dict, native primitive,
validity, take/slice, allocator and file-layout providers. Existing ShardLoom
KeyColumn/RowSet preserve dictionary equality and full backing ownership.
The primitive dictionary-partial reducer already serves its own admitted
workloads; it does not consume this composed projection. Extend the existing
native consumer, rather than introduce another expression interpreter, key table,
executor, pool, dependency, or Arrow execution boundary. This is a
`wrap_vortex_concept` decision with native residual execution and
`fallback_attempted=false`. Encoding preservation needs actual observations,
not merely a dictionary-shaped fixture declaration.

## Admission and semantics

Admit a grouped aggregate directly over project → filter → scan, including
the ordinary binder's equivalent project → scan-with-pushed-predicate shape.
The projection
contains only identity group keys (aliases allowed) and exactly one computed
integer measure, with every output consumed. Measures may be COUNT(*),
COUNT(computed), SUM(computed), or AVG(computed); at least one consumes the
computed measure. The expression is one add, subtract or multiply between a
column and a non-null I64 literal, in either operand order, with I64 output.
Prove both endpoints of the complete declared integer domain fit I64 using
checked I128 arithmetic. A small observed data range cannot admit I64 * 3.

Do not admit casts, division, remainder, functions, lazy branches, floating
expressions, computed group keys, unused projections, other reductions or
explicit spill. They keep ordinary native execution. Preserve null propagation,
first-occurrence group order, aliases and exact result schema, complete producer
and generation checks, cancellation and resource denial. The existing spill
selection runs first. No source replay or automatic strategy switch follows
allocation failure.

The admitted expression cannot fail on valid values. Prepare its original
column and identity keys in projection order, then use the ordinary grouping
owner. Compute each selected integer once and update every requested state.
Each state sees the same row sequence and existing compensated integer-input
SUM/AVG behavior; do not replace that behavior with weighted or reordered sums.
No output is delivered before the complete producer succeeds. Charge temporary
owners and retained group state through the current native context.

## Frozen finite comparison

One identical ignored Rust harness calls the real prepared relational API in
both builds. Freeze source closure, features, Rust 1.99.0, release profile,
harness and executable identities. Generate each immutable native file once,
outside measured preparation, with the existing sequential native flat writer,
65,536 rows per chunk, no file statistics and no external engine. Store complete
input witnesses and file hashes. Each operation opens/binds its file afresh;
preparation and returned engine execution clocks stay separate. Record their sum
for the first-execution control, serialization, resource observations and cleanup.
Host/cache state is uncontrolled and prehashing is explicit; this is not a
storage-cold experiment.

| Case | Rows / key domain | Purpose |
| --- | ---: | --- |
| dictionary_dense | 262,144 / 64 | Dictionary UTF8, 60% predicate selection; target |
| dictionary_skew | 262,144 / 64 | Nine of ten non-null keys use key zero; target |
| dictionary_sparse | 262,144 / 64 | Dictionary UTF8, 3% selection; target |
| plain_high_cardinality | 65,536 / 8,192 | Plain UTF8, many retained groups; control |
| tiny | 17 / 7 | Dictionary UTF8 and setup-sensitive execution; control |
| ineligible_i64 | 65,536 / 64 | I64 input multiplication cannot pass the domain proof; control |

All keys use non-ASCII text, with a null key every 97th row. The nullable
integer is `(row % 1009) - 504`, null every 19th row; it is I32 except the
ineligible I64 control. The I64 predicate value is `row % 5`, or `row % 100`
for the sparse target. Select values from 1 through 3 inclusive, project the
integer multiplied by 3 and the identity key, then group by the key with
COUNT(*), COUNT(value), SUM(value) and AVG(value). Independently derive full
ordered results using an ordered Rust value map plus first-occurrence positions,
exact integer reference sums and authoritative UTF8/U64/F64 schemas. Fixture
values bound reference sums within exact F64 integer range.

Use one CPU allocation, a 128-MiB grant and no spill. Run two complete cohorts
of three paired samples per case: 72 release calls total. Alternate role order
within cohort one and reverse it in cohort two. Each target must improve median
engine time by at least 2% in both cohorts. Every case's engine and
preparation-plus-engine median must remain within 5% of baseline; only tiny
also allows an absolute 50-microsecond increase. Freeze before measurement;
preserve every sample, failure and partial cohort without silent retry.

Separately run one debug observation per case and role (12 calls). Count actual
constructed expression rows, mapped rows/stages, prepared value owners and
dictionary domains. Require zero computed-expression output rows in eligible
candidates, the expected selected-row count and unchanged complete values,
schema, source consumption and native dictionary grouping. Candidate debug
observations separately count actual dictionary-backed key domains at the
existing key owner; the three dictionary targets must each observe at least
one. This counter is absent from the already frozen baseline. Release timing has
no debug observation code. A failed gate means revise or drop under a new
explicit protocol, preserving the original decision.

## Correctness and package boundary

Test all narrow signed/unsigned domains, both operand orders and every admitted
operator at domain endpoints; reject unsafe full-range arithmetic. Include
mixed/all-null and empty inputs, aliases, duplicate dictionary values, sliced
and reordered dictionary codes, multi-batch group order, independent reference
reductions, unused/erroring projections and non-admission shapes. Check
late errors, changed sources, cancelled consumers, denied reservations and
released credits; explicit spill must retain ordinary correct behavior.

Retained code needs focused and broad native/default checks and public
SQL/DataFrame/write-readback proof in the coherent package. Run one isolated
standard Full43 before that package's PR. This finite composed experiment does
not establish a whole-engine gain, publication readiness, or CG-gate closure.

## Premeasurement harness correction

The first observation attempt (`fused-measure-observations-r1`) stopped after
the baseline dense case. The shared test helper parsed complete JSON values
through `serde_json::Value` without the `float_roundtrip` feature; selected
correctly emitted average literals were reparsed one F64 step away from the
direct arithmetic reference. A separate small program using the exact compiled
JSON library reproduced three such values, while Rust's standard F64 parser
returned their original bits. Native aggregation uses the same division as
the reference, and native JSON delivery serializes its finite F64 directly.
The failed command, complete assertion output, source and executable identities,
supervision and unchanged-input checks remain saved. No candidate observation
or release timing call ran in that attempt.

Revision two retains the complete raw emitted JSON as an additional witness.
The identical test harness in both builds reads the sum/average numeric tokens
through Rust's standard F64 parser and requires exact complete ordered values;
the independent verifier also parses the saved original JSON. A focused
regression covers the three rounding examples, nulls and exact counts.
This changes test parsing only. Both runtime source versions, fixed native
inputs, six cases, admission rules, resource policy, 12 debug observations,
72 release calls and every benefit/control threshold stay as specified above.
Both versions are rebuilt and refrozen before revision two runs; the failed
first attempt is separate and receives no acceptance credit.
