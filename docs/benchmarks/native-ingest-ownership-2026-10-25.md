# Full-ingest ownership: finite measurement protocol

Frozen before the first new complete ingest, October 25, 2026. This protocol
does not claim a measured benefit. Preserve every observation, including failed
admission, correctness, publication and performance observations.

## Contract and controls

Use the accepted Parquet input at
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/sources/hits.parquet`
(14,779,976,446 bytes; verify the full accepted SHA-256 before measurement).
The accepted provider-refresh artifact contains 99,997,497 rows, 105 original
fields in source order, and seven explicitly derived fields. Freeze the full
source/target hashes, source generation, code and executable identities,
resource settings, host observations and current codec/layout recipe in a
machine-readable receipt before execution.

Keep the existing standard ingest command, 24 GiB memory admission, maximum
parallelism 12, source batch policy, compression configuration, derived fields,
publication and storage safeguards. Run through
`scripts/run_clickbench_ingest_uat.sh`. Use a new output destination outside
synced folders. Do not raise storage or concurrency limits. Keep task-owned
builds, tests and other substantial workloads idle during measurements.

## Attribution and candidate selection

1. Build an observation-only control that preserves validated copies. Record
   actual copied buffer bytes, allocation charges, buffer-region counts and
   repeated exact visible regions within each batch. Keep the original owners
   alive while comparing region identities; an address alone is not an owner.
   No cross-batch cache is allowed. Record ownership admission/copy/validation
   as a nested work span separately from the inclusive Arrow conversion span.
   These are overlapping elapsed work spans, not exclusive wall or CPU time.
2. Execute one complete guarded control ingest. If ownership work is below 10%
   of the inclusive conversion work, or no actual copied bytes are observed,
   stop the ownership performance candidate and document the attribution.
3. Prefer exact duplicate removal when repeated bytes reach at least 20% of
   copied bytes. Otherwise investigate the provenance-checked internal transfer
   candidate: consume the input batch, require Arrow's unique standard allocator
   ownership proof, zero allocation pointer offset, explicit alignment, full
   allocation size no larger than the existing copy charge, and retained native
   memory credit. Shared, offset, empty, oversized or opaque owners keep copies.
   Generic external-memory intake keeps its existing copy contract.
4. Admit only one of those mechanisms to the first complete candidate screen.
   Preserve the accepted artifact representation. No new compression portfolio,
   scheduling experiment or re-enabled warm cache belongs in this candidate.
   Tests must establish the full ownership/lifetime/resource contract first.

## Complete-ingest screen and matched confirmation

The observation-only control is the comparison executable. Keep it immutable
after its source and executable identity are recorded. The candidate must use
the same attribution fields and recipe. Initially run the candidate once. Stop
without a repeated cohort if it removes less than 20% of copied bytes, fails
any correctness/publication/resource check, exceeds the control artifact size
by more than 0.5%, or exceeds control complete-ingest process time by more than
5%. Require at least 3% complete-ingest improvement in this first observation
before admitting repeated confirmation. A preliminary result is a screen,
not a speedup claim.

Only a candidate passing that screen receives three fresh matched pairs in
the declared order control/candidate, candidate/control, control/candidate.
Each execution is a complete successful ingest. Keep all observations. Require
at least 5% improvement in the median of the three within-pair percentage
reductions in complete-ingest process time, and no pair with a candidate
regression greater than 5%. These diagnostic process clocks are used only for
this explicitly requested ingestion investigation. Do not substitute them for
query engine clocks or put them into routine query timing updates.

Every measured output must pass complete row/schema/publication and encoding
inventory checks. Its size must stay within 0.5% of its paired control, and
observed peak RSS within 5% of its paired control. Allocation reservations,
cleanup, no-fallback evidence and source generation checks must all pass.
The five-percent benefit threshold selects a meaningful complete-workflow
gain; the historical twenty-percent illustration is not an acceptance gate.

## Independent readback and consumer acceptance

Before accepting a candidate, independently reopen both the observation-control
artifact and the retained candidate output. Compare every original value, type,
null and row order against direct pinned Parquet decoding. Verify all seven
derived columns with independent formulas. Use bounded column-at-a-time,
batch/window comparisons; reject unsupported or unknown columns and incomplete
coverage. Preserve generation checks, concurrency bounds and explicit memory
scope. Parquet/provider allocations outside the credited allocator must remain
identified as excluded; a work grant is not proof of their full ownership.

For later paired outputs, exact size and SHA-256 equality to an already fully
validated output establishes byte identity. Otherwise compare every value
against the independently validated control using the existing bounded native
artifact comparator before retiring redundant large payloads. Preserve every
report and full hash first.
Only one new full-size payload should remain once its checks and replacement
requirements permit retirement; never remove the accepted source or artifact.

For a retained correction, run one isolated standard Full43 pass on the new
artifact: exactly one execution of each query with the complete current
independent result reference. Freeze the executable, all inputs and supervision
before the pass; keep all other task work idle during queries. Report only the
sum of its 43 engine clocks. A complete engine sum more than 10% above the
accepted provider-refresh observation triggers query-level attribution before
queue advancement; do not silently repeat the suite. This is a consumer
regression check, not a matched competitive ranking.

## Stop and retain rules

Reject the optimization on any ownership, value, type/order, generation,
publication, accounting, cleanup or control failure. Preserve the failed
observation and reason; do not relax a gate after seeing the numbers. Fix an
implementation defect before any newly declared run. Retain useful validated
readback coverage and truthful attribution separately if the performance
candidate is rejected. Actual repeated preparation or probe work requires
source evidence and a separate frozen finite mechanism; existing fused derived
passes and numeric-probe avoidance are already implemented.

## Preliminary complete readback

Before the first new complete ingest, the existing accepted provider-refresh
file passes the new direct-Parquet readback: **99,997,497 rows, 112 columns and
11,199,719,664 values**. All 105 original fields and seven independently derived
fields match, including native schema, names, nullability and row order. The
comparison completes 1,367,184 equal spans. Input generations remain unchanged,
both native scan tasks drain, and native plus Arrow work reservations return
to zero. This is a later correctness proof; the original provider-refresh
acceptance did not claim complete readback.

The first attempt fails closed at its Arrow batch limit. A diagnostic replay
identifies `Title`, with 29,511,164 retained bytes for 65,536 rows against a
16,777,216-byte ceiling. Both failed observations are preserved. The complete
pass reduces reference and native batches to 8,192 rows while keeping the
16 MiB Arrow ceiling, 8 MiB value limit, 512 MiB native source admission,
two-task concurrency and 8,388,608-row scan windows. Every row is still checked;
the largest observed Arrow batch is 11,489,688 bytes. Decoder and other upstream
allocations outside the native allocator remain explicitly excluded from its
reservation claim.

The source SHA-256 is
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`;
the accepted Vortex file SHA-256 is
`95a393d153a8c2e1ffeb5084b2656ac61465c6ef9dad435d032365844f1ac057`.
The readback executable SHA-256 is
`acf92e36719eac28a484545a9314adc04900af018c42c3440cf480e1efbcfc47`.
The exact command, build/source identities, supervision, complete report and
earlier failures are retained under
`/Users/dylan/LocalData/shardloom/ingest-ownership-20261025/`, indexed by
`baseline-readback-acceptance-r3.json`. No new ingest or ownership optimization
is accepted by this preliminary readback, and the frozen performance gates
above are unchanged.
