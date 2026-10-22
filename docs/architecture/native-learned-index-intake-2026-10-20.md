<!-- SPDX-License-Identifier: Apache-2.0 -->

# Weighted native search: finite learned-index intake

Status: prospective ordinary-path observation, before predictor implementation
or candidate timing. This is conditional row 15, `PERF-10.learned_indexes`,
under RFC 0044 and PERF-10. Start from
`8721506c487b873cc0598ead0b78b4e22339ff99`. The conditional-first order remains
active; the separate ingest-overlap measurement prerequisite, 37 required
substeps and CG-1 through CG-23 keep their original gates.

## Contract and current owner

A database execution maintainer must be able to check the complete seeded draw
sequence, exact first-equal cumulative boundary, state lifetime and real public
consumer. A standalone search helper over synthetic pre-sorted keys is not
sufficient evidence for this row.

The resident `Sample` in `local_primitive_unary_sample.rs` already retains one
credited `Candidate` per eligible input row for replacement sampling. Its
inclusive cumulative Float64 weights are nondecreasing in original source
order. Each output access searches that array with
`partition_point(cumulative_weight < threshold)` and clamps to the last entry.
Weights are positive and finite; cumulative overflow fails. Positive tiny
weights can round away, so repeated cumulative values are legal and must return
the first equal position. Output access can repeat per field or byte-sizing
pass; count actual searches rather than assuming one per logical draw.

The current resident population is rebuilt on every execution. Prepared input
buffers and binding may persist, but no index/model crosses executions or
source generations in this intake. Memory source admission, `ReservedVec`,
retained rows, native result construction and certificates stay authoritative.
No file, row-export helper, spill population or cache-directory search is being
claimed as this observed path. Those callers have different access/ownership
costs and remain separate.

## Vortex-first check

Pinned `vortex-array` 0.85.0 provides `SearchSorted`, `IndexOrd`, the `Left`
lower-bound convention and `SearchSortedPrimitiveArray<T>`. The slice adapter
uses partial comparison; the array adapter obtains scalar values through the
native execution context and uses primitive total comparison. Its nullable
form has an explicit null order. A bounded source review of these modules finds
ordinary exact search, not a trained predictor or certified correction window.
The source inventory with complete hashes accompanies the evidence.

The resident CDF is a field inside credited candidate records, not a separate
Vortex Float64 array. The distinct spill path stores Float64 bits in UInt64
native records behind a generation-validated positional block reader. Copying
either into a second array would add construction and ownership costs; provider
existence alone does not justify that representation change.

Classification for a later finite predictor, only if intake passes:
`implement_shardloom_kernel`. Keep exact search over the current ordered owner,
Vortex-native source/result arrays and the existing no-fallback certificate.
Any new model must own its actual metadata under the same grant, prove bounds
for all thresholds including gaps and duplicate runs, and use exact correction.
The existing provider remains an available exact-search comparator; do not
introduce another execution engine, dependency, sorted copy of the input,
implicit model cache or unreported canonicalization.

## Frozen ordinary workloads

Each source has nonnullable Int64 `id = row ordinal` and nullable Float64
`weight` with all values present. There is no filter. Project only `id`, so
hidden weights remain hidden and the one-column fixed-width consumer does not
manufacture repeated work through a wide payload. Use explicit sample size,
replacement and seed 7. `unweighted` omits the weight-column declaration.

| Cell | Population | Draws per call | Weight at ordinal i | Purpose |
| --- | ---: | ---: | --- | --- |
| uniform | 65,536 | 65,536 | 1 | large repeated search |
| periodic | 65,536 | 65,536 | 1 + (i mod 16) | nonconstant repeated search |
| shifted | 65,536 | 65,536 | 1 for i < 32,768, otherwise 1,024 | changed distribution control |
| hot_tail | 65,536 | 65,536 | 2^40 at i = 65,535, otherwise 1 | highly skewed control |
| few_draws | 65,536 | 8 | 1 | construction-dominated control |
| tiny | 32 | 128 | 1 + (i mod 4) | small owner control |
| unweighted | 65,536 | 65,536 | 1, ignored by the operation | no ordered search control |
| rounded_ties | 4 | 7 | exact bit-defined weights below | first-equal boundary control |

For `rounded_ties`, use weights `[f, t, t, 1-f]`, with Float64 bit patterns
`f = 0x3fd6ba8224f50f05` and `t = 0x0000000000000001`. The total is exactly
one. The pinned seven output IDs are `[0,3,0,3,3,3,3]`. Regenerate the seed
mix independently in Python and compare every row for every case, including
both calls. Check source weights by their exact bits and schema, not decimal
text equivalence or a result checksum alone.

Run one fresh prepared workflow per cell in table order, with two complete
calls per prepared generation. All sixteen calls must match the independent
reference, source identity, actual population and output counts. A temporary
test-only counter records population size, logical draws, actual searches and
actual comparison evaluations in the existing partition operation. It must be
disabled outside explicit observation and clear after success, error or unwind.
Empty/duplicate boundary and observer cleanup tests precede the workload.

The search-work prerequisite requires both `uniform` and `periodic` to observe
65,536 resident CDF entries, at least 65,536 exact searches and at least
1,000,000 cumulative comparisons per call, with equal results across repeat
calls. `unweighted` must record no ordered searches. Every call must remain
within the unchanged native grant, with no denied reservations and zero credits
after complete teardown. This is an observed comparison-work signal, not CPU
attribution or proof of achievable speedup. If it passes, freeze a separate
predictor/comparison protocol before implementation. Counts alone never retain
an optimization.

## Observation and safety

Use the actual public `prepare_relational_with_schema` and `collect_jsonl`
paths with one native CPU and a 64-MiB grant. Source construction uses ordinary
`ResidentMemorySource::from_columns`. Record preparation, engine execution,
serialization, result release and prepared teardown separately. Independent
reference creation, exact validation and evidence writes remain outside those
clocks. Instrumented observations carry no timing benefit or exclusive-search
time claim. Any later comparison disables observation symmetrically.

Bind complete source/build/fixture/protocol/oracle identities before execution.
Use current storage, process-drain, competing-native-work and UAT-lock guards;
retain host/process observations and failures. Store generated records outside
synced folders. Compact and verify complete results before retiring temporary
payloads. No changed memory/storage ceiling, performance superiority, broader
sampling support, Full43 result, release or publication follows from intake.
