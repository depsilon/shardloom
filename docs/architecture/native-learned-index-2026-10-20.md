<!-- SPDX-License-Identifier: Apache-2.0 -->

# Weighted native search: finite predictor protocol

Status: prospective candidate, frozen after a passing search-work intake and
before predictor implementation or candidate timing. This is conditional row 15,
`PERF-10.learned_indexes`, under RFC 0044. No mechanism is retained by this note.
The conditional-first order, separate overlap measurement prerequisite,
37 required substeps and CG-1 through CG-23 remain unchanged.

## Intake result and evidence

The [frozen intake](native-learned-index-intake-2026-10-20.md) runs the public
prepared relational API and complete JSONL collection over the actual resident
weighted sampler. All eight workflows and sixteen calls pass. The independent
reference checks 393,252 source rows by ordinal and exact weight bits and all
655,646 output rows in order, with exact schemas and no-fallback certificates.
Both uniform and periodic calls observe 65,536 CDF entries, 65,536 searches and
1,114,112 comparisons each. Unweighted sampling observes no ordered search.
The largest recorded reservation is 13,431,904 bytes inside the unchanged
64-MiB grant; every teardown leaves zero credits and no denied reservations.
These are work counts, not CPU attribution or a timing benefit.

The [compact packet](../benchmarks/evidence/learned-intake-packet-825.json.xz) retains complete
inputs/results, independent references, changed source, compiler records and
all fourteen guard outcomes: eight successful and six failed preliminary checks.
Its [manifest](../benchmarks/evidence/learned-intake-packet-825.manifest.json) binds every member.
Replay reconstructs every complete result without an engine executable and
checks 1,546 compiled local source dependencies against Git plus the archived
changes. Of those, 1,543 have individual pre-build hashes; three unchanged JSON
fixtures are bound by the pre-build commit and clean status, then checked
byte-for-byte against that commit. This distinction is explicit in the packet.

The recorded corrections are a mistyped source revision, an observer ownership
lint, the source-auditor's fixture/count handling, and the Python verifier's
uppercase primitive-type spelling. Pinned Vortex serializes `i64` and `f64` in
lowercase. Only those reference spellings changed; expected weight bits and IDs
did not. The corrected verifier replays the original sixteen calls. No failed
query result was replaced or timing cohort rerun.

## Native provider and finite mechanism

The intake's Vortex 0.85.0 provider check remains binding. Exact native sorted
search exists, while this resident population stores its CDF inside credited
candidate records. The proposed classification is `implement_shardloom_kernel`:
one private, borrowed predictor over that existing owner, with exact standard
lower-bound correction. Vortex-native input/result construction and the existing
native certificates remain authoritative. No dependency, external engine,
sorted copy, second CDF array, public index API or persistent model is added.

The model and exact-correction approach is informed by
[Kraska et al., section 2](https://arxiv.org/pdf/1712.01208) and
[Ferragina and Vinciguerra, section 2](https://www.vldb.org/pvldb/vol13/p1162-ferragina.pdf).
The implementation is original; neither paper's implementation is copied.
The [learned-index benchmark study](https://arxiv.org/abs/2006.12804v2) also
examines construction and memory-system effects. Our retention decision uses
the complete public workload below and imports no published speed claim.

Consider only weighted replacement samples with at least 4,096 candidate entries,
at least 4,096 logical draws and at least one draw per four population entries.
These are count-based rules, not fixture or distribution identifiers. Every
execution rebuilds its own population and model. A model cannot outlive or refer
to a different population; its immutable borrow ties search to the same owner.

For positive finite cumulative values `c[i]`, `n = len(c)` and
`total = c[n-1]`, evaluate the monotonic Float64 predictor in this exact order:

```text
p(t) = min(n-1, floor((t / total) * float(n-1)))
e = max over all i of abs(p(c[i]) - i)
radius = e + 1
```

Do not replace division with a precomputed reciprocal: tiny finite totals must
remain valid. Examine the existing records once; abandon calibration immediately
if `e > 8`, retaining ordinary exact search. If admitted, search only the clipped
inclusive radius around `p(t)`, using the original strict `< threshold` predicate
and final clamp. Nonfinite or out-of-domain query values use ordinary search;
normal sampling thresholds remain within `[0,total]`.

The following correctness argument is specific to this proposal. At exact lower
bound `k`, the preceding distinct boundary satisfies `c[k-1] < t <= c[k]`, unless
`k = 0`. Monotonic prediction places `p(t)` between the two endpoint predictions.
Their calibrated errors give `abs(p(t)-k) <= e+1`; `p(0)=0` covers the first
position. Repeated cumulative values retain their first-equal position, and every
duplicate contributes to calibration. Exact search inside the certified interval
therefore returns the same position, including absent thresholds and gaps.

Reserve the actual model metadata under the existing session grant before
calibration, with an absolute ceiling of 128 bytes for this finite model. Poll
cancellation at least every 256 calibration entries. If the current free grant
cannot cover metadata, omit the model and use ordinary native search. A racing
reservation denial remains the existing deterministic error. Failed calibration,
cancellation, errors and completion release all model credits. No new global
budget, source cache, result cache or spill behavior is admitted.

## Correctness and activation before timing

Before candidate timing, run focused tests for empty and singleton boundaries,
every CDF equality and its neighboring representable thresholds, absent values,
duplicate runs, zero caused by threshold underflow, tiny and large finite totals,
calibration refusal, insufficient metadata credit, cancellation and complete
cleanup. Compare complete positions with ordinary exact search; the full seeded
public workload also uses the independent Python reference. Include a large
admitted distribution containing rounded cumulative duplicates, so duplicate
correctness is exercised inside the predictor rather than only on a bypass.

Then execute one preflight workflow per role and cell, with both calls, through
the same public preparation and collection paths. Required candidate activation:
the uniform and periodic cells construct a model each call and use it for every
weighted output search; shifted and hot-tail cells attempt calibration, reject
the bound and use ordinary search; few-draw, tiny, unweighted and rounded-tie cells
do not calibrate. The baseline never constructs a model. Record actual visits,
radius, credited bytes and correction searches. Every cell must pass values,
types, source, counts, certificate and cleanup checks before timing begins.

## Frozen comparison and retention

Keep all eight input definitions, schema, source order, seed 7, replacement,
one-column projection, 64-MiB grant and one CPU from the intake. Each workflow
creates one fresh prepared generation and makes two complete calls. The common
measurement harness and independent reference must be byte-identical in both
builds. Role labels describe evidence only; they must not select runtime behavior.
Compile and bind ordinary baseline and integrated candidate sources separately,
including every local compiler dependency and fixture, and verify fresh builds.
Disable the test-only observation session symmetrically during timing.

Run two cohorts, A and B. Each has one unscored warmup and seven scored samples
per cell and role. A visits cells in intake table order; B reverses that order.
For each sample index (warmup 0, scored 1–7), alternate role order by index:
A uses baseline/candidate for even indices and candidate/baseline for odd ones;
B uses the opposite order. A role invocation is one complete fresh workflow.
Run every invocation sequentially under the unchanged storage, native-work,
process-drain and UAT-lock guards. Preserve all observations and failures.

Within each cohort and cell, take the median of seven whole-workflow engine
totals per role, each total being the sum of its two engine execution clocks.
The primary ratio is candidate median divided by baseline median. Both uniform
and periodic targets must each have ratio **at most 0.98 in both cohorts**, in
line with the current prospective 2% engine target. Every cell, including both
targets, must have engine ratio **at most 1.05**. Also require complete-workflow
ratio **at most 1.05** for every cell in both cohorts. Complete workflow includes
native source construction/preparation, both calls with result serialization,
result release and prepared teardown. Fixture generation, reference creation,
hashing, validation and evidence writes stay outside those clocks in both roles.
Report engine time in routine messages; preserve other clocks in evidence.

All calls must pass full exact comparison and no-fallback certification. No
denied reservation is permitted in this workload; peak credited bytes may exceed
the matched baseline by at most the declared 128-byte model ceiling, and teardown
must leave zero credits. A target gain cannot compensate for a failed control.
Both cohorts must pass for retention. If one passes and the other fails, do not
retain or claim a repeated regression. Do not repeat an unchanged failed cohort,
tune thresholds after observation or promote only a favorable distribution.

An integrated retention requires ordinary public CLI/Python sampling coverage,
full native and workspace checks, unchanged provider/resource semantics and the
applicable PERF-10/12 acceptance evidence. A production integration change that
alters the measured runtime must face the same frozen comparison again; earlier
prototype results cannot substitute. If the mechanism is not retained, remove
experimental runtime/observer code, preserve complete compact evidence and verify
the restored runtime. This finite experiment cannot close all learned-index,
sampling, memory, platform or competitive obligations.
