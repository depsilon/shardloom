<!-- SPDX-License-Identifier: Apache-2.0 -->

# Outermost-poll ingest observer revision

Status: prospective measurement revision for `PERF-09.overlap`. The two
[earlier observers](native-ingest-overlap-attribution-2026-10-20.md) remain
inconclusive. This revision admits no writer or scheduling change. It belongs
to the existing conditional-first pass, RFC 0044 and RFCs 0013/0014/0017;
all broader owners, 58 substeps and CG-1 through CG-23 retain their gates.

## Material change and provider boundary

The append-only observer emits entry and exit events for every nested future
poll, then its offline replay counts each thread only once. The new observer
will suppress nested poll pairs on the same thread and for the same observation.
It records the outermost entry and exit, plus every existing CPU, blocking-I/O,
task, readiness, child, prefetch and drain event. These retained events determine
the same occupied-driver intervals. CPU-closure events retain their separate
nesting and queue accounting, including a closure invoked inside a poll.

A scoped thread-local depth record identifies its observation owner. A poll
guard borrows that owner, cannot move across threads, and clears depth during
normal return or panic. A different nested observation is rejected before
changing the active record. Successive observations and futures moving between
threads between polls remain valid. Nested polls do not take an event ticket,
read the clock/thread ID, or clone an `Arc`. The root generic future uses stack
pinning. Per-outer-poll diagnostics retain the number of suppressed pairs and
maximum depth, published before final drain. No event sampling is introduced.

Classification: `use_vortex_native_provider`. Reuse pinned Vortex 0.85.0
`CurrentThreadRuntime`, `Executor`, `Handle` and `Task`. The exact local provider
source confirms that `Handle` has no public raw-executor accessor. Preserve the
delegated abort/join adapter: dropping its delegated result receiver can suppress
an unstarted CPU closure. Do not replace that adapter, patch Vortex, create
another pool or change driver counts. The observer and its installation remain
test-only. No payload is read by the observer, no encoding changes, and native
Vortex output/provider certificates and `fallback_attempted=false` are required.

The current source base is `24d11e58f1e789581979580d1f82586ee76057a8`.
Reintroduce only the historical observer/harness integration against current
sources, preserving all intervening accepted changes. Freeze the complete new
Rust/Cargo source closure and compiled identity before the new preflight.

## Preserved workload and gates

The [original design](native-ingest-overlap-attribution-design-2026-10-19.md)
and corrected protocol SHA-256
`83da704cd7852ba3d638052b0bebcd39512e116ec0c74b5fe86f6f7f1eaaa5cf`
remain authoritative for workload geometry and scoring. The exact protocol is
retained in the prior result packet. The source-base amendment above does not
change its numerical gates, source prefixes, role ordering, codecs or resources.

- Use official resident Parquet row groups 0, 113 and 225, at most three
  32,768-row batches, all 105 source plus seven existing derived columns, and
  the same large-source profile with 262,144-row blocks and 8-MiB targets.
- Keep three P4 targets, three P1 controls and the single-batch P4 control,
  the 2-GiB native grant, 256-MiB output cap, 32-MiB observer allowance and
  existing 192-MiB diagnostic-storage ceiling. No limit is raised.
- Freeze and execute 14 preflight workflows, then both A/B cohorts with three
  paired repeats of all seven cells: 84 measured workflows. Complete both
  cohorts before reading their comparison scores. Preserve all failures.
- Require the original exact ordered values/schema/layout/statistics checks,
  source generation/hashes, real prefetch activation, output parity, no fallback,
  staging ownership and complete task/credit drain for every workflow.
- In every cell of each cohort, observed median complete-ingest cost must be
  no more than 1.10 times ordinary median. A single failure is inconclusive.
- Only an eligible observer can admit a later scheduling design. The original
  ready-successor/CPU-queue-empty unoccupied capacity fraction must reach 5%
  in the same two P4 targets in both cohorts. Serial and no-successor controls
  retain exactly zero opportunity. Below threshold is no material signal only
  in this finite scope.

Occupied-driver wall intervals remain distinct from thread CPU, and async
runnable state remains unknown. The opportunity integral is neither an achieved
speedup nor an achievable-speedup bound. Complete-ingest diagnostics do not
replace the engine clock used for routine query timing communication.

## Proof before measurement

Retain the existing observer lifecycle tests and shared-slot, source, owned
writer, pressure and text-statistics selectors. Add deterministic checks for:

1. Identical interval histograms from the same synthetic event schedule with
   full nested pairs and with redundant pairs removed, including concurrent
   drivers, CPU closure nesting, queue transitions and readiness changes.
2. Real nested root polling emitting one outer pair, exact suppression/depth
   diagnostics, and no retained thread-local depth after normal return or panic.
3. Rejection of a different nested owner without corrupting the original owner;
   independent sequential owners and concurrent drivers remaining separate.
4. Future poll migration between threads, untouched never-polled/drop behavior,
   and all existing overflow, unpublished-event and terminal-drain failures.

The independent event verifier must recompute histograms from every complete
new trace and validate suppression counters against actual outer event counts.
It retains the old decision function without threshold or sample-selection
changes. Freeze its sources and tests with the orchestration helpers.

Use guarded sequential builds/tests and a recognized `shardloom-` executable
name for copied harnesses. The shared UAT lock, source-residency and storage
checks, sampled host context, bounded child deadlines and process-group cleanup
remain active. Validate and compact outputs before retiring exact disposable
payloads; retain compact identities, outcomes, source deltas and reproduction
helpers. The historical invalid/incomplete/overhead failures remain linked.

After the decision, remove temporary instrumentation or carry it only into a
separately frozen candidate experiment. Exact restoration and appropriate fresh
checks are required before advancing. A positive observation authorizes design
work only; it does not complete the scheduling experiment or breadth work.
