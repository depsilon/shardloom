<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dictionary-domain text execution investigation

Status: the finite experiment is complete and **retained locally** at
`263f052ce7ef84618b042985cbcb4a3f73ca3bff`. This resolves the bounded N extension
under item 20/RFC 0044 in the
[metadata/encoded-consumer queue](native-metadata-encoded-consumer-investigations-2026-11-13.md).
The [complete comparison](../benchmarks/native-dictionary-domain-2026-11-14.md)
and [portable evidence](../benchmarks/evidence/native-dictionary-domain-2026-11-14/README.md)
verify all 11,060 fresh results, 80 lifecycle operations and 210 dictionary-work
witnesses. The complete 5,530-request engine total decreases from 676.301549673
to 668.884468618 seconds; separate lifecycle engine time decreases from
0.098959580 to 0.077330714 seconds. All 3,207 individual regressions remain in
the evidence. The added 1,380 dictionary/control cases are slightly slower as
a group; the standing decision applies to the two complete totals.

All required default/native, feature, minimum-Rust, provider and installed-package
checks pass. The captured source fingerprint is
`c64f40da7b3693e3eff7e4eebeca2f64a171a7d02deba387f440d021f586b4ef`;
all 5,762 captured files match the preservation commit. The build retains its
actual `c74d0ebb4` parent and dirty-source identity. The packet preserves the
initial interrupted native suite, failed harness capture, exact baseline reuse
and bounded recovery, plus 28 saved-evidence, 12 transport and 36 packet controls.
Relocated replay works with original source/runtime/evidence reads forbidden.

The accepted scalar-regional runtime is this experiment's comparison baseline.
The per-measure and constant-key drops remain unchanged. Named source/domain
facts extending C, followed by ALP/FastLanes predicate admission, are next.
Broader metadata/encoded work, required item-19 spatial execution, item 20,
RFC 0044 and CG-1 through CG-23 retain their remaining obligations. This local
acceptance does not merge, publish, release or imply competitive superiority.

The following sections preserve the prospective mechanism and semantic contract.
The comparison report records the later harness repair without changing requests,
inputs, runtime code, independent references or the acceptance rule.

## Observed opportunity and scope

The unchanged shared expression evaluator prepares a native UTF8 dictionary's
values and codes, then applies lower/upper/trim/character-length/byte-length/regex
once per logical row and builds a row-length result. A pre-candidate file-scan
test on `c74d0ebb4` with only two test-file additions verifies complete values,
one four-entry native value domain and 2,048 expression result slots. It does
not supply a performance observation. Its source, original failed feature
selection, corrected passing check and full counters are preserved in the local
N evidence workspace before changing production code.

The independently checked original 10k declarations contain 403 scalar-call
cases, 197 additional predicate records within those cases, and 3,810 UTF8
group/distinct control cases. There are 63 shared case identities, giving a
4,150-case union. The calls use lower, upper, trim, length and octet_length;
they contain no regex call. This is declaration coverage, not physical encoding
or runtime activation. Separately versioned fixtures must establish actual
dictionaries and the missing semantic/consumer combinations.

An encoded-engine maintainer should be able to trace each transformed slot to
one referenced source value, every output row to its original code/null, and
every group or distinct identity to logical equality rather than a code number.
The candidate covers the six existing prepared UTF8 kernels only. Binary byte
length, URL-domain legacy providers, fallible parsing/casts, arbitrary multi-input
functions and nonconstant siblings keep their current native behavior.

## Vortex-first provider check and reuse map

Decision: `implement_shardloom_kernel` for the semantic admission and value-domain
application, reusing the pinned Apache-2.0 Vortex 0.93.0 `DictArray`, native arrays
and allocator. No dependency, copied upstream implementation, unsafe block or
query-engine integration is added. This remains within `shardloom-vortex`'s
existing local-primitives boundary.

The pinned dictionary scalar-function rewrite demonstrates the representation
contract but applies to registered Vortex scalar functions. Its guards reject
oversized domains, fallible work over unreferenced values, nonconstant siblings
and non-strict behavior over null codes. Vortex's scalar-function inventory does
not provide ShardLoom's Unicode lower/upper/trim/character-length/regex kernels.
Its byte-length provider is not character length, and the existing provider
decision retains ShardLoom's dictionary-derived length owner. The
[upstream explanation](https://spiraldb.com/blog/what-if-we-just-didnt-decompress-it)
motivates computing on values while retaining codes; the pinned source and local
tests, rather than its historical performance examples, govern this candidate.

| Existing owner | Gap and bounded extension |
| --- | --- |
| `Expression::evaluate_function` and `scalar::Function::evaluate_prepared` | Keep complete argument preparation and existing Unicode kernels; admit a dictionary-domain branch after preparation. |
| `KeyColumn::Variable` | Borrow its already validated native values, validity and original-width numeric codes; expose only the necessary private accessors. |
| `result_batch::build_expression_column` | Build each referenced transformed value once, keeping the existing credited variable-output builder and numeric/bool result semantics. |
| `result_batch::copy_numeric_column` and owned buffers | Copy already prepared codes with their width and nulls into independent receiving ownership; retain dictionary metadata credit on the resulting buffers. |
| Native filter, grouped aggregate and distinct owners | Consume the returned Vortex dictionary using existing selection and exact value equality. Do not introduce code-only equality or a new aggregation family. |
| Projection output reuse | Identical completed expressions and compatible dependent outputs retain the same existing batch-local reuse rules. No global or cross-source expression cache. |
| Resident execution context and native I/O certificate | Record completed domain/code/result work at batch boundaries, with explicit decline and unobserved-byte limits. |

All five public front doors continue lowering to these owners. Lazy selected
branches preserve their current inputs and evaluation scopes. The previous
detached-code provider repair remains intact; no private code-slot assumption
or decoder callback is introduced.

## Semantic and resource admission

Start only from an actual UTF8 dictionary already retained by the shared key
owner. Require fewer domain slots than logical rows. Empty, flat, constant,
oversized-domain and unsupported function/sibling inputs retain the existing
row evaluator. A capacity refusal for optional representative bookkeeping can
decline before execution; corrupt codes, allocator errors and cancellation may
not be relabeled as an optional refusal or retried through another executor.

Preserve the existing complete-domain UTF8 validation before computation. Build
a bounded, credited first-reference table by visiting logical codes in order,
with the same code-domain diagnostics. Null codes remain null. Null values stay
null. Unreferenced valid entries do not run text kernels: use an unobservable
typed placeholder in those slots. The candidate does not admit fallible text
functions or evaluate an unreferenced parse/cast. Regex keeps its bound literal
pattern and strict null propagation. No query-specific threshold or route is used.

Use the unchanged kernels for Unicode case conversion, Unicode whitespace trim,
Unicode scalar-count length, byte length, and bounded literal regex. Do not
deduplicate transformed slots. Different input values can become equal and
different dictionaries can use the same code for different text; downstream
hash, collision resolution, group and distinct owners must compare exact values.
Ordering follows the transformed logical value, with existing null placement.

The existing `UNION ALL` field-alignment path invokes `cast_batch` and
`take_column`, which compact selected text into independently owned row buffers.
An expression above that boundary therefore has no dictionary to retain. Keep
that composition as an explicit no-dictionary control; do not bypass its cast,
validation or source-ownership contract in this candidate. Separate native file
chunks with different dictionaries establish the encoded group/distinct/filter
consumer obligation before scoring.

The temporary first-reference table is sized by value-domain cardinality. Codes
and transformed output buffers receive native allocator credits before
allocation. Metadata credit must survive retained arrays, children, clones and
slices. Source buffers may be released while the transformed output survives;
source changes and reuse are still governed by the existing resident generation
checks. All temporary owners release on success, failure, cancellation and retry.
No new spill policy, source reopen, persistent summary, writer default, scheduler
or fallback execution is introduced.

## Evidence and prospective decision

Record actual dictionary batches, logical rows, referenced/non-null transformed
values, domain slots constructed, code rows inspected/copied, retained dictionary
rows, and ordinary row-output/decline counts. Counts describe owner invocations,
not unique rows across expressions. A retained dictionary is not a zero-decode
claim: its values and codes may already be decoded by native preparation. Buffer
credit snapshots are not process RSS. Final output expansion and compatibility
translation remain explicit controls with their existing materialization reports.

Before measurement, freeze exact public requests, complete independent results
and schemas, source/input/runtime/helper identities, expected activation/refusal,
resource/error/lifecycle contracts and the complete-cohort decision. The original
4,150-case union follows manifest order without selecting by saved performance.
Reuse accepted-runtime observations only after exact request, input, reference
and runtime identity checks. New dictionary controls must cover the six kernels,
filter/group/distinct/full output, compatible shared outputs, Unicode collisions,
null codes/values, unused/invalid entries, changed dictionaries, sliced/sparse
domains, flat and high-cardinality controls, and complete public-surface parity.
Freeze their exact finite list before scoring; no incomplete original 10k result
is presented as a new 10k run.

The prospective missing-control matrix has six native-file profiles:
`dictionary`, `sparse`, `all-null`, `unique`, `flat`, and `empty`. Each has a
71-row public form and a 12,291-row work form (zero for empty), with three
explicit chunks and changing dictionary values for the same codes. Sparse
domains have 16 public or 128 work slots with valid, unreferenced long values;
malformed-domain and corrupt
code failures remain separate correctness checks. Flat files carry the same
logical population as the repeating dictionary. Unique domains are at least
as large as each chunk and must decline the optimization.

For every profile/size and each of the five public surfaces, declare grouped,
distinct and filtered consumers for lower, upper, trim, character length,
octet length and literal regex. Also declare complete compatible output,
normalized group/distinct, a union-before-transform control, and a selected
input control: 23 request forms, or 1,380 new declarations. Combined with the
unchanged 4,150 saved identities this is a prospective 5,530-case cohort.
References enumerate every row independently in Python. Verify native fixture
readbacks before freezing. A fixed-size long value and bounded work row count
keep complete outputs within the existing per-command evidence budget.

Focused proof precedes broad default/native workspace checks, provider regression
checks, feature checks and installed-package proof at the final source boundary.
Preserve every failed attempt. Run one isolated observation per fresh declaration
under existing storage, deadline, overlap and cleanup controls; full hashes and
inventories stay outside query execution. Prepared lifecycles have a separate
declared complete-operation comparison. No warmups, routine repeats, cache
manipulation, Full43, full10k or competitor rerun follows from this investigation.

Correctness, exact code/value identity, actual consumer retention, no-fallback,
memory and cleanup proof are prerequisites. Under the standing decision, lower
complete-cohort engine time retains; equal or higher drops the candidate. Keep
all family and individual regressions. A drop restores the accepted runtime and
preserves the complete candidate source and evidence.

PulseWeave and capillary scheduling remain the existing bounded batch owners;
this candidate reduces their per-value work without changing batch geometry.
Shared memory admission bounds optional work; native dictionary representation
postpones materialization. Engine timing and evidence-tier contracts stay intact.
Required spatial execution and all remaining metadata/encoded queue items keep
their order and independent acceptance criteria.
