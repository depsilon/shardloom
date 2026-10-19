<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native spill merge-tail investigation — October 19, 2026

Status: frozen attribution design; the
[finite result](native-merge-tail-2026-10-19.md) found no eligible material signal.
No candidate schedule was implemented. The text below records the protocol
and its conditional candidate boundary. This is the seventh investigation in
the [conditional sequence](native-conditional-investigations-2026-10-19.md), under
`PERF-06.merge_cost`, RFCs 0014/0017/0044 and the existing PERF-06 verification
contract. Broader owners and CG-1 through CG-23 remain open.

## Existing boundary and proposed question

The shared relational `Ordering` in
`shardloom-vortex/src/local_primitives/native_relational_spill.rs` creates sorted
level-zero native runs, combines equal levels during ingestion, then drains the
remaining stack by merging its rightmost adjacent pair. Each run already records
its actual row and serialized-byte counts. Left input wins equal sort keys;
adjacency and left/right source order preserve stable ties. The shared store
charges actual output bytes before removing either input. Its two readers share
the existing 128-KiB work lease and the original query grant.

The finite question is whether a different adjacent binary schedule for this
already-held final stack has material potential. Ingestion-time merges, fan-in,
run creation, sort buffers, reader ownership and disk-overlap admission stay
unchanged. The separate numeric query-sort fan-in is outside this mechanism.

Vortex-first classification: `use_vortex_native_provider`. Pinned Vortex 0.85.0
native arrays, `NativeWriter` and `SequentialNativeFlatLayout` remain the existing
read/write providers inside `shardloom-vortex`. Native file bytes are observed
weights, not an assumption that encoding output is additive or predicts CPU time.
The query-owned stable merge schedule is ShardLoom policy around those providers;
no alternate query engine or Arrow execution substrate is admitted.

## Attribution before a candidate

The frozen local protocol is `merge-tail-attribution-776-v4`. The first V1
preflight stopped at public policy admission before any workflow executed:
its 128-KiB spill buffer was below the existing 1-MiB minimum. V1 sources,
protocol, build and failed attempt remain immutable and receive no decision
credit. V2 corrects that configuration before any measurement, preserving the
workload, samples, ordering, thresholds and production behavior. Before any V2
workflow, a source audit established that two frozen outputs exceed the public
collector's 8-MiB cap. That build was deliberately interrupted and drained; its
source, protocol and build evidence remain. V3 uses the existing bounded JSON
batch consumer, keeping every row and preserving public admission limits.
V3's first empty-memory-source operation returned correctly, but the harness
then mistook the session's file-open counter for a memory-preparation counter.
No complete workflow record or measured sample was emitted. V4 records the
actual successful memory-source construction separately, binds the existing
per-plan source counts, and requires zero file sources and file opens. The
original V3 source and failed evidence remain. Its earlier storage refusal
before child launch and the subsequent unchanged-guard continuation also remain;
no files were removed to resume that attempt.
A temporary,
test-only observer records every initial run, parent pair, actual rows/bytes,
source-order interval, level, final frontier, merge span, simultaneous reader
count and store disk snapshot before write, after output creation and after input
removal. No per-row timer or sort algorithm change is allowed. Full stable output,
logical dtype, native certificate, source opens and zero final reservations remain
mandatory for both observer-off and observer-on roles.

Sixteen complete prepared Sort/streamed-JSON workflows cover seven payload-width profiles
at 16,383 and 32,767 rows and two unspilled controls. Nullable Int64 keys sort
ascending NULLS LAST and descending NULLS FIRST, with a nonnullable Int64 source
ordinal used only as an output witness. Nullable UTF8 payloads vary independently
of sort keys. All use one lane and a 64-MiB native grant; spill cases explicitly
authorize a 1-MiB buffer and 128-MiB temporary quota. Empty and 17-row cases
join every complete cell in preflight.

Spill permission does not force run creation. A larger source that remains below
the retained-input threshold may also finish in memory. Its owner/cleanup trace
and every store, reader and run counter must agree on zero spill work; it
contributes no merge opportunity. The verifier uses actual run creation rather
than a row-count assumption.

Each JSON batch is bounded to 2,048 rows and 2 MiB; the test client refuses more
than 16 MiB of JSON or 1,024 batches in total. Retained strings and their handle
container keep native memory credits. Every batch schema, row count and byte
count is checked, and the complete assembled output remains the independent
oracle boundary. Both roles record nonoverlapping synchronous sink/consumer
spans through a test-only hook at the existing public JSON batch consumer.
`QueryTiming::from_elapsed` subtracts those spans from the complete operation
to preserve the engine/result clock definition. Result release includes every
retained batch and container. No V1 or V2 workflow completed.

Three samples in each of two reverse-order cohorts retain every result. One
warmup per role/cohort is retained and excluded. Fixture generation, oracle
work and witness persistence remain outside compared clocks; preparation,
native execution, complete consumption, release and owner destruction are
included in the corresponding complete-workflow record. Routine timings use
the existing engine clock. Host/cache observations and other clocks remain
diagnostic evidence.

Before release compilation, the corrected frozen sources must pass the nine
focused spill tests and both complete twenty-workflow preflight roles in a
debug-profile build. Every output, schema, trace and resource record is checked.
Debug observations receive no timing-comparison or decision credit. A fresh
release build then repeats all forty preflight workflows before the unchanged
measured cohorts; passing debug verification cannot replace release evidence.

An independent checker reconstructs exact run lineage and actual disk overlap.
It computes an interval dynamic program over each observed final frontier,
preserving adjacency and every weight. The minimum additive-byte cost is an
estimate; it is never reported as executed work or an accepted speedup.

At least one same spill cell must qualify in both cohorts: a distinct estimated
tree saves at least 10% and 1 MiB of weighted input work; measured final-tail
merge spans total at least 20% and 2 ms of engine work; and observer-on median
engine and complete-workflow costs do not exceed off by both 10% and 1 ms.
Exact semantics, accounting and cleanup gate every record. Otherwise this finite
scope has no eligible material signal, with excluded or insufficient cases
recorded explicitly. No thresholds or cells change after observation.

## Conditional next stage and limits

Positive attribution permits a separate candidate protocol; it does not accept
a schedule. Such a candidate must bound and credit planner metadata, check
cancellation, preserve actual two-reader/input-output admission and complete
values, and pass whole-workflow benefit/control gates plus the applicable
integration acceptance. Existing corrupt-run, quota, cancellation and cleanup
contracts remain mandatory. If attribution fails, retain its complete evidence
and remove temporary observer code rather than promoting the estimated tree.

This bounded synthetic workload cannot establish general merge optimality,
compressed-size additivity, all-source performance, new spill capabilities or
broader recovery. Rematerialization and other conditional rows keep separate
decisions. No package, release or public API change follows from this protocol.
