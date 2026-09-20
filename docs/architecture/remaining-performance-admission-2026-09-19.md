# Remaining performance candidate admission

This follows Q19 PR #1447 (`2be959bf`) and Q33 PR #1448 (`676e1f10`), both
merged with complete-result UAT and passing CI. It screens G/H/I from the
[domain-transfer packet](performance-domain-transfer-2026-09-19.md), under
PERF-02/03/07/08/09/11/12. It does not close broader PERF or CG obligations.
The runtime control is `676e1f1006efb23c296f821a4e53f4e57fcbece6`.

## G: drop another ingest traversal-fusion candidate

The source audit identifies no remaining same-semantic duplicate traversal
with evidence that it can clear the 10% complete-ingest gate. In particular:

- `universal_format_io.rs::append_embedded_derived_columns_to_batch` already
  fuses UTF8 byte length with URL domain, and minute extraction with minute
  bucketing. Dictionary inputs retain their specialized producer paths.
- `project_record_batch_for_vortex` already avoids identity projection work.
  The retained `identity_projection()` call records a counter.
- `vortex_ingest_numeric_encoding.rs::NumericDataStrategy::new` already
  passes `with_stats(&[])` to avoid another `Stat::all()` traversal before
  BtrBlocks computes its required statistics.
- `measured_probe` already bypasses the discarded dictionary probe for
  canonical primitive chunks when dictionary encoding is excluded. Encoded
  inputs still require the provider's root decision.

Owned input conversion, compression after coalescing, zoned/file statistics,
generation validation and persisted-artifact verification have different inputs,
lifetimes or obligations. Removing one requires an actual matching producer and
consumer contract; overlapping elapsed spans do not supply that proof. The
[retained ingest profile](../benchmarks/ingest-stage-balance-2026-09-12.md)
does not establish another removable 9.6 seconds of complete ingest work.

This is a source-based non-admission of the proposed optimization, not proof that
ingest has no further opportunities. No ingest prototype, replacement artifact,
new ingest timing or storage improvement is claimed. Codec portfolios, writer
overlap and the slower smaller text artifact remain parked.

## H: drop the grant-only serving candidate at the current boundary

`PreparedVortexSource::with_native_execution` takes the session admission mutex
before invoking the native callback and holds it until that callback and source
generation validation finish. `PreparedVortexCount::execute`, projection and
temporary-provider-driver paths use the same admission owner. A waiting call
has not reached the worker grants whose quantum the proposal would change.

The native writer fixture in `resident_file_serving_tests.rs` explicitly holds
that callback at a controller channel checkpoint. Its short count callers
prove that the mutex is occupied before the controller releases the writer.
There is no fixed 60 ms sleep. Those bounded queue observations cannot establish
a production arrival rate, service-time distribution, throughput or p99.

`plan_flow_inventory` and `compute_scarcity_ledger` derive reports and resource
decisions from inputs. They do not resume another session callback while its
current owner holds admission. The current callback accepts an arbitrary native
operation; it has no cooperative suspend/resume protocol for writer state,
provider drivers and the waiting operation. Smaller internal worker grants
therefore do not discharge the measured admission wait.

Their batch-window decisions do have planning consumers elsewhere. Faster active
service could indirectly shorten this wait; that would require evidence of a
service-time improvement, not establish cross-request fair queueing. Existing
worker drains and retirement retain the encompassing session gate.

The original proposal is not admitted: no existing grant-only change has a
demonstrated path to its 30% p99 / at most 5% throughput-loss gate. A future
admission/lifecycle design would first need a declared fixed-arrival workload,
queue-versus-service attribution, a subsecond objective and explicit ownership
and cancellation contracts. No scheduler change or production fairness claim is
made here. This is a mechanism mismatch found in source, not a failed p99
experiment, and does not imply that all service delay is irreducible.

## I: drop the bounded SUM/AVG owned-result candidate

At this candidate's measurement, `PreparedVortexAggregate::execute` admitted integer SUM/AVG, while
`execute_owned` currently admits only its documented COUNT/DISTINCT shapes.
The existing owned COUNT results remain retained. Tiny Full43 output rendering
remains dropped from the heavy-query queue.

September 20 capability completion broadens ordinary retained aggregate schemas
and expressions. It does not change this candidate's measured outcome or establish
owned SUM/AVG output. See `native-runtime-completion-2026-09-20.md` for current scope.

The bounded `computed_result_cost` example selects one missing family without
changing execution: one nonnullable I64 group key and one I64 measure, SUM or
AVG ordered descending with signed-key ascending ties, returning either 32 or
65,536 rows. The source contains 262,144 rows in 8,192-row writer inputs and
65,536 groups. Each group receives four values; AVG results are exactly
representable halves. A standard-library oracle checks every returned key,
value and position. P1/P4 each use fresh aggregate state through a retained
source; owned execution must reject the missing shape before scanning.

The clock includes the public prepared execution, report/certificate creation
and returned-result drop. Preparation, fixture creation and complete-value
validation are excluded; all raw calls, including the first warmup, are retained.
Existing caller timing spans and serialized summary bytes are reported. They
are not exclusive CPU, transport, copied bytes or allocator totals. The screen
does not compare a candidate or establish an owned-result speedup. Retention
still requires at least 20% and 100 ms lower complete latency, with lifetime and
sink proofs, on the same declared workload.

Provider decision: reuse the pinned Vortex writer/reader and current ShardLoom
prepared aggregate API for measurement. No new execution provider, native
Python binding, source representation or fallback engine is introduced.

All 24 complete calls and eight pre-execution owned-shape rejections passed.
The saved [screen and source evidence](../benchmarks/remaining-performance-admission-2026-09-19.json)
retains every sample. Fastest measured calls, excluding the explicitly retained
first warmup in each cell, are:

| Function | Requested workers | 32 output rows | 65,536 output rows |
| --- | ---: | ---: | ---: |
| SUM | 1 | 17.989 ms | 88.189 ms |
| SUM | 4 | 22.976 ms | 85.095 ms |
| AVG | 1 | 22.949 ms | 83.789 ms |
| AVG | 4 | 22.353 ms | 85.088 ms |

Every complete call, including warmups, was below 100 ms. Even eliminating the
entire call cannot save the required 100 ms on this declared workload. Drop this
bounded extension; retain the example as a reproducible admission screen.
This is not a claim about wider computed results, a remote transport, Python or
another workload. No owned SUM/AVG implementation or speedup is claimed.

The large reports contain about 3.2–3.4 MB of serialized summary text, produced
from a 758,456-byte native source. The numerical SUM/AVG JSON values follow the
engine's existing floating-result contract; the small fixture values permit exact
comparison without tolerance. Finalization spans include ranking and state
cleanup, so their 34–40 ms observations are not pure removable delivery cost.

The screen ran on Apple M5, 10 logical CPUs, 16 GiB RAM, macOS 27.0 (26A428),
Rust 1.98.0, Vortex 0.85.0, ordinary release settings with no PGO, P1/P4 and the
1 GiB session policy. OS cache and unrelated host activity were uncontrolled.
Source and binary digests, exact build/run commands, the guarded supervisor and
all storage observations are retained in the evidence JSON. The supervisor
enforces a 180-second deadline, 1 GiB workspace, 16 MiB logs and 12 GiB free-disk
headroom plus a 64 MiB reservation; it joins terminated children and removes only
a proven child-owned experiment lock. These are screen guards, not process-RSS
enforcement. Use the same local unsynced storage and serial-run controls when
reproducing:

```sh
cargo build --release -p shardloom-vortex --example computed_result_cost \
  --features 'vortex-local-primitives vortex-write'
```

Resolve the executable with Cargo metadata, then pass `--workspace` and
`--source-revision` as recorded in the receipt. The revision identifies the
unchanged engine; the separately hashed example identifies this new screen.

## Refreshed query selection after the finite packet

G/H/I's bounded screens are exhausted; broader ingest, serving and result
ownership obligations remain open. No additional Full43 run was needed to
select the next target: this packet changes no production runtime source.

The latest saved Full43 (`06da983b`) passed 129/129 comparisons and has an
unpaired 82.196766-second best sum. Its largest per-query best observations are:

| Query | Latest best complete process time |
| --- | ---: |
| Q29 | 9.721 s |
| Q17 | 6.123 s |
| Q19 | 5.392 s |
| Q34 | 5.140 s |
| Q35 | 4.880 s |
| Q10 | 4.672 s |
| Q23 | 4.593 s |
| Q36 | 4.144 s |
| Q6 | 3.906 s |
| Q18 | 3.695 s |
| Q16 | 2.826 s |
| Q27 | 2.744 s |

These rank observations, not diagnosed regressions. Faster historical valid
controls remain in the ledger. Do not attribute every slower sample to a
particular host process or rerun unchanged suites merely to improve the table.

**Selected next ship/drop screen: reuse Q33's persistent complete-partition
reduction for proven single-integer COUNT grouping, with Q36 as the target.**
In Q36's first run, which is also its best 4.144340-second run, the worker caller
records 3.330456 seconds merging chunk partials, versus 0.002377 seconds waiting
at join. Worker busy spans sum to 1.302421 seconds across overlapping owners;
they are not additional serial wall time. There are 1,550 completed chunks,
99,997,497 input rows and 9,762,046 complete groups. The ordinary caller
accessor/update counters are zero because work moved into the existing workers;
they must not be read as zero execution cost.

The new hypothesis removes that repeated caller merge by retaining complete
physical integer keys under partition ownership, sorting/reducing exactly and
selecting winners after every contribution arrives. It transfers the retained
Q33 mechanism, not its numeric-pair admission or late-measure rescan. Existing
dependency proofs must still certify the four logical grouping expressions and
reconstruct their outputs. This is distinct from already-shipped Q36 worker
admission. No query-number dispatch or universal aggregate-state replacement is
allowed. Check generic renamed scalar-key fixtures and signed/unsigned bounds,
duplicates across chunks, ties/OFFSET, cancellation, denial and source lifetime.
Reject overflow-sensitive or otherwise unproved reductions before execution.

The credible opportunity is the same-run 3.33-second serial merge, not a forecast
that sorting is free. Retain only after at least one second of comparable
complete-query savings, or at least 30% lower OS peak RSS with nonregressing
complete time; preserve all samples and use the fastest valid run symmetrically.
Failure must remove the prototype and retain its evidence.

**Subsequent attribution targets:** Q29's first-run accessor span is 7.575 seconds
but is not exclusive CPU or a URL-parser measurement. It covers 1,798,248 groups
before HAVING, 74 afterward and 25 output rows. Attribute source decode, hashing,
dictionary construction and transient ownership before another candidate;
the rejected owned partial/parser-only ideas stay dropped. Q34/Q35 already use
complete-key exact partitions and record 18,342,019 groups, so another partition
or heavy-hitter proposal alone is duplicate work. Their next screen must identify
actual dictionary/ownership traffic that the existing partitions do not avoid.

## Verification and limits

Formatter, workspace all-target Clippy and tests, and native-feature example
Clippy plus both adversarial verifier/lock tests pass. The screen validates all
24 complete results and all eight deterministic owned rejections. An independent
review checked G/H's source claims and the screen's clocks and oracle; its SUM
JSON-type and abnormal-lock-cleanup findings were fixed before the retained run.
Q33's prior full UAT remains the production-runtime proof; no production runtime
logic changed in this packet. Package publication, broad capability completion
and all parked codec/topology/binding experiments remain outside this decision.

The first PR CI run exposed a preexisting lifecycle assertion race in
`resident_segment_reuse_io_tests`: a successful Vortex segment delivery wakes its
consumer before the provider necessarily releases the coalesced parent buffer.
The read observer's zero pending jobs proves completed I/O, not joined provider
cleanup. Terminal whole-session zero-credit assertions now retain the memory
pool, drop the resident session to join its drivers, then require exactly zero
reserved bytes. Per-call cache-retention and read-drain checks remain in place.
The ignored release experiment uses the same terminal boundary and names its
evidence `session_owned_bytes_after_resident_teardown`; query timing intervals
are unchanged. This is a test/evidence lifecycle correction, not a runtime
memory or performance fix.

The corrected segment-reuse suite passes all 21 active tests. The complete CI
native-library command also passes with default parallelism: 1,835 passed and
nine existing manual experiments ignored. Native release all-target Clippy and
formatter checks pass. Independent review found no remaining ownership gap in
the changed assertions. The ignored release experiment was compiled but not
rerun; this packet claims no new timing from that experiment.
