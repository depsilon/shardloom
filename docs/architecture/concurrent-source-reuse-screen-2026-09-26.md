# Concurrent source reuse — R8

Status: drop eager shared-producer routing for the tested workflow; retain the
existing explicit owned-array API. No new runtime cache or producer is retained.
PERF-INTAKE / RFC 0044, following merged R9.b in PR #1465.

## Existing behavior and measured question

Owned-array sources already share immutable native buffers and their original
owners. Prepared aggregates reuse lowering and source identity while constructing
fresh mutable state. Reimplementing either is duplicate work. File-backed serving
shares a held descriptor and parsed footer, but builds a reader tree over each
call's I/O scope. That scope owns cancellation, active I/O and drain before the
call returns its CPU grant. Current segment reuse closes at the operation boundary.

R8 must first locate material repeated work across compatible concurrent calls.
Repeated segment demand alone is not exclusive time saved, and a shared file
handle does not establish shared decoding. The screen records successful
positional-read ranges and bytes, complete native-call and cohort times, output
ownership and memory release. It does not measure physical device traffic or
attribute every provider decode allocation. The accepted concurrent-host timing
explanation is not reopened by this experiment.

## Bounded source screen

Generate one 131,072-row native file in local temporary storage: eight batches of
16,384 rows, a renamed exact integer beyond floating-point precision, and renamed
nullable Unicode text. Use the upstream default writer and report the resulting
file identity and size, capped at 64 MiB. No retained ClickBench artifact changes.

Use the existing serving session, P4 and 256 MiB, with one caller CPU lane per
general operation and no reserved metadata lane. Compare one and four identical
projections, two disjoint key/text projections, and four callers split into two
repeated projection groups. Start each cohort from a bounded readiness
gate. Preserve all warmup and measured observations (one warmup plus three
measured cohorts per role/scenario); alternate observed and ordinary order.

The test-only reader wraps the real `ResidentFileReadAt` using the same descriptor,
allocator, provider handle, concurrency and per-call I/O scope. Record at most
4,096 completed read events per call, with start/end timestamps relative to the
cohort release. Errors invalidate this favorable-workload screen;
successful futures correspond to completed positional reads. Cancellation-time
physical reads are not inferred from this observer. Installing it creates an
additional test-only file view, so measure the ordinary path separately and
report instrumentation cost without subtracting it from another clock.

Each call produces bounded owned native output, then drains its I/O and checks
the source generation. Owned projections may retain encoded children; this is
read attribution, not a full-decode timer. Independent scalar verification of every typed output
and null is outside timing and must trigger no additional source reads. Bound
output rows, bytes and arrays; drop all results/source/session and require zero
remaining reservations. Report individual calls and complete cohort elapsed
time, never add overlapping intervals as saved wall time. Few cohort samples
are not production p95/p99 evidence. No result cache or external engine is used.

## Vortex-first provider check

Decision: `use_vortex_native_provider` for the existing observation path.
Pinned Vortex 0.85.0 provides `VortexFile`, cached `LayoutReader` trees,
`FileSegmentSource`, `SegmentCache` and the shared-future mechanisms already used
by ShardLoom's query-local segment reuse. Replacing a file's segment source clears
its cached reader tree. Its Moka byte cache does not supply ShardLoom's operation
cancellation, shared reservation, bounded consumer lifetime or source-generation
contracts. No new cache is justified merely because that provider exists.

Any admitted follow-up must preserve independent consumer errors/cancellation,
source-generation fencing, credits through the last owner, bounded slow-consumer
retention and drain of actual I/O. Sharing a future owned by the first caller's
cancellation scope is insufficient. Measure duplicate decode separately if reads
are inexpensive. Retention uses useful complete-operation gains with correctness,
resource and regression acceptance; the numerical serving target is a priority
target, not an automatic rejection cutoff. Full serving acceptance requires a
frozen arrival trace and adequate tail samples after a candidate exists.

No runtime throughput, latency, decode-reuse or production fairness claim follows
from this design or source audit. Broader serving/operator obligations remain open.

## Existing owned-source fanout screen

Before creating a broker, compare the already-shipped owned-array handoff with
four concurrent calls through one retained file-aggregate handle. Use the same
fixture and P4 serving session. The request computes row count, nullable text
count, exact integer DISTINCT and maximum text. Independent expected values come
from the fixture generator. Mutable aggregate state remains per execution.

The shared-producer arm charges the complete file projection, bounded owned-source
construction, consumer preparation, all four complete aggregate reports and
producer/source-owner drop. The control reuses its already-prepared file handle;
it does not pay another file open or lowering per cohort. Both use the same native
aggregate family. Preserve one warmup and five measured pairs in alternating
order. All output values are checked outside the native cohort clock, and report
drop is separately timed. This is an explicit composed workflow, not automatic
query coalescing or decoded-value sharing: owned arrays may retain encoded children.

Also verify that one pre-cancelled consumer leaves three peers and subsequent
execution successful, then drop every owner and require zero reservations and
exact temporary-directory cleanup. A positive result would describe a use of the
existing API, not a new runtime optimization or production serving-tail proof.

## Decision and evidence

The [machine-readable summary](../benchmarks/concurrent-source-reuse-2026-09-26.json)
and compressed raw archive preserve every sample, frozen binary/build receipt,
guard receipt and observer source. Both screens used the same 845,540-byte native
fixture content (`b45676ea…e8015020`), generated locally with no retained artifact
replacement. The source-read screen passed **88 complete calls** across 32 cohorts,
including warmups. Each result's complete typed values and nulls matched the
independent generator; verification caused no additional observed reads.

| Source-read cohort | Requested bytes | Unique interval bytes | Ordinary cohort, all measured samples |
| --- | ---: | ---: | --- |
| One full projection | 841,252 | 841,252 | 0.276 / 0.203 / 0.149 ms |
| Four identical full projections | 3,365,008 | 841,252 | 0.422 / 0.420 / 0.343 ms |
| Two disjoint key/text projections | 841,252 | 841,252 | 0.223 / 0.216 / 0.202 ms |
| Four calls in two repeated projection groups | 1,682,504 | 841,252 | 0.239 / 0.216 / 0.211 ms |

Read counts come from separate instrumented cohorts. The identical four-caller
case repeats 75% of requested bytes; the disjoint control repeats none. Read
await intervals overlap, so their sum is not a wall-time saving. The observer
adds a file view and locking; all observed and ordinary times remain separate.
The process peaked at 43,696,128 bytes RSS; cumulative session reservations
peaked at 3,370,832 bytes. These are whole-screen figures, including untimed
verification, not per-call or production memory bounds.

The follow-up compared four complete native aggregates through the existing
file and owned-array paths, with producer construction charged to the shared arm:

| Arm | All measured cohort times | Best | Median |
| --- | --- | ---: | ---: |
| Retained file aggregate | 26.408 / 22.982 / 26.734 / 24.320 / 24.910 ms | 22.982 ms | 24.910 ms |
| Eager owned-array producer | 28.486 / 25.995 / 23.590 / 25.314 / 24.503 ms | 23.590 ms | 25.314 ms |

The shared arm wins two pairs and loses three. Preserve the pairing when assessing
those effects:

| Pair | Shared minus file cohort | Shared relative to file |
| --- | ---: | ---: |
| 1 | +2.078 ms | 7.87% slower |
| 2 | +3.013 ms | 13.11% slower |
| 3 | −3.144 ms | 11.76% faster |
| 4 | +0.994 ms | 4.09% slower |
| 5 | −0.406 ms | 1.63% faster |

These five mixed pairs do not establish a repeatable advantage or a stable
regression. Separately, the shared arm's best observed cohort is 0.608 ms (2.65%)
slower than the control's best, and its median is 1.62% slower. Those independent
summaries do not describe paired effects; the median paired delta is +0.994 ms.
All 48 paired aggregate outputs, including
warmup, matched all four independent expected values. Three peers and a later
execution also succeeded after another consumer's **pre-admission cancellation**;
the exact cancellation diagnostic was checked. Active-consumer cancellation is
not newly established by that check. Whole-process peak RSS was 110,100,480 bytes,
and session reservations peaked at 3,368,176 bytes; neither is an arm comparison.
Both screens ended with zero reservations, zero reservation denials and successful
removal of their exact temporary directory. The source-read screen additionally
checks zero active scoped I/O after every cohort.

**Do not adopt an eager pre-scan/fanout default from this inconclusive screen.**
The scoped ship/drop decision declines new automatic routing, not the observed
individual wins or a possible benefit on other workloads. The existing
explicit API remains available, including its R5.a memory benefit in other
workflows. Repeated bytes alone do not justify a new broker. A streaming shared
decoder, larger working set, cold reads and mixed-query arrival trace were not
measured; they need new dominant-cost attribution before reopening R8. This
decision does not close broader PERF serving, spill or public-call obligations.
C2.a executable block recipes is next in the ranked queue.

The first observer run failed a harness assertion that expected provider credits
to disappear immediately after result drop (416 versus 320 bytes). The provider
can finish small bookkeeping cleanup during runtime teardown. The corrected
screen observes intermediate credits and asserts zero after teardown joins;
the failed receipt is preserved. Review also corrected the disjoint-control
label, constructor-failure cleanup, feature gate and cancellation assertion.

Production runtime files are unchanged except a test-only module declaration.
The existing Full43/ingest acceptance from PR #1465 remains the latest runtime
acceptance; these screens do not claim a new Full43 or ingest measurement.

## Reproduction and validation

Build with `cargo test --offline --release -p shardloom-vortex --features
release-user-surfaces --lib --no-run --message-format=json`, resolve and freeze the
test executable from Cargo's output, then run these exact ignored tests serially
under `scripts/run_local_readonly_proof.py` with an admitted local-only `TMPDIR`:

- `resident_session::source_reuse_bench::concurrent_source_reuse_attribution`
- `resident_session::source_reuse_bench::fanout::existing_owned_producer_fanout_pairs`

The guard's immutable source is the frozen harness executable; the small dataset
is generated inside its owned temporary directory. Reserve 64 MiB for that fixture
at preflight. The receipt does not imply use of the retained ClickBench file.
Keep native-feature test files and the build receipt as additional guarded inputs.

Workspace fmt/Clippy/all-target tests, release-user-surfaces Clippy/all-target
tests, the write-only feature compile and three documentation validators are the
completion checks; exact commands and logs are in the evidence archive.
They passed: 3,425 workspace tests and 1,954 native all-target tests. The native
suite lists 17 ignored measurement/regeneration helpers; the two new R8 helpers
were separately executed in release mode as recorded above.
The extra strict write-only Clippy attempt reports the pre-existing unused test
helper `large_source_text_vortex_write_strategy` in unchanged `vortex_ingest.rs`.
The write-only compile succeeds; the required workspace/native strict Clippy
checks remain separate. No unrelated runtime code was changed to silence it.
