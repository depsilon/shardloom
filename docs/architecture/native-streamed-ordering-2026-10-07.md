# Completion-aware input through native ordering

Status: [complete local engine acceptance and independent packet inspection](../benchmarks/native-stateful-aggregation-ordering-2026-10-08.md)
pass for runtime `8a207745`. Affected support checks, four native examples and
local browser QA pass; hosted integration remains pending. This is a bounded
dependency for the broader stateful pressure and recovery milestone in the
[remaining-scope contract](native-local-completion-scope-2026-10-07.md), under
PERF-03/06/07/11/12 and CG-5/19/20/21. General aggregation has a separate
[pressure strategy and acceptance contract](native-aggregate-pressure-2026-10-07.md)
in the same cohesive runtime change. Join pressure remains a separate family
obligation; windows, pivots and general execution resume do not become supported
through this ordering connection.

## Decision and reuse map

Run one complete native operator tree over a demand-driven input scan. Reuse
the existing native sort, `native_relational_spill::Ordering`, query run store,
resource pool, cancellation and writers. Do not run a separate tree for each
input batch: that would sort each batch independently and reset limit offsets.

| Existing component/callers | Missing contract | Shared extension |
| --- | --- | --- |
| Prepared relational execution, file/resident scans, Python/SQL/CLI batch adapter | The previous streaming loop invoked the whole tree per input batch. | Move demand and input-owner release into the batch scan. Execute the bound tree exactly once; retain no provider in the prepared plan. |
| Native filter/project and selected payload | Lazy native views can retain the private input batch. | Preserve final-output detachment and add an explicit compact native copy before ordering retains a batch. The input release witness must expire before the next demand. |
| Native resident sort and relational ordering spill | Whole-source ordering previously rejected the streaming declaration. | Admit Sort in the finite single-source chain; keep the same keys, null ordering, stable ties, native runs, merge schedule and quota. Without explicit spill, retained state remains grant-limited and may deny. |
| Relational Limit | Per-batch reset and count-zero early return violate complete input validation. | Admit global offset/count with mandatory upstream drain. Count zero still executes/validates upstream for a streaming source. No source-termination protocol is introduced. |
| Native result consumers and Vortex writer | Stateful output needs the same final completion and publication proof. | Incremental results, bounded collect and one native Vortex destination reuse the existing producer/consumer/commit lifecycle. Compatibility output and fanout remain separately gated. |

## Vortex-first provider check

Decision: `wrap_vortex_concept`. Pinned Vortex 0.85.0 `ArrayIterator`, native
`ArrayRef`/`DType`, allocator-backed buffers, native `take` and file/sink APIs
already supply the underlying representation. `ArrayIterator::read_all` collects
chunks; its dtype contract does not enforce ShardLoom's private input ownership,
finite bounds, typed errors, completion or resource admission. An execution-scoped
borrowed adapter supplies those obligations around existing native providers.
It is not a second data representation, query engine or global registry.

The actual ordering algorithm and disk lifecycle stay in ShardLoom's existing
native sort/run-store components, under RFC 0044. Feature gates stay
`vortex-local-primitives` and `vortex-write` for disk spill/output. No upstream
upgrade, dependency, unsafe code, Arrow execution or external query-engine
integration is introduced. Execution and native-I/O reports retain provider
version, materialization, actual spill and `fallback_attempted=false`.

## Input and state lifetimes

The complete lowered plan is classified before any producer demand or dynamic
binding: exactly one matching batch scan, through Project, Filter, Sort and
Limit. The companion general-aggregation contract also admits Aggregate.
Repeated sources, joins, windows, sets, other stateful unary operators and
dynamic schema binding remain rejected before consumption. Subsequent families
require their own contracts.

An execution owns a borrowed provider and an input report. The scan may start it
only once. For each payload it checks cancellation, exact declared dtype, session,
private ownership and unchanged finite bounds, updates checked counters, and
passes a borrowed source into the ordinary projection/residual scan. When the
downstream call returns, drop the source and check its weak release witness.
Only then request another payload. An empty batch is not end-of-input.

Filter/project preserve their native lazy/expression behavior. Each ordering
retention boundary compacts its incoming native batch into independently credited
payload before handing it to resident or spill sort. This is an explicit copy,
including nested order stages, with batch/row counters; it is not zero-copy.
Sort buffers, native run reads/writes, overlapping merge output, final results
and sink metadata share the existing query grant. Source, detached state and
output can overlap and each owner keeps its credits until its final reference
drops. The input-batch count does not bound the state or whole process.

Each Sort sees all upstream rows once, preserves its existing stable tie semantics
across input and spill boundaries, then delivers its complete ordered relation.
A Limit before/after Sort applies once to the relation. It discards later output
after its requested range while continuing upstream evaluation and validation.
Late producer/schema/value errors, resource failures and cancellation therefore
remain failures even for `LIMIT 0` or an already-delivered prefix. This explicit
drain policy trades early-return latency for complete finite-source validation.

## Spill, failure and publication

Spill remains opt-in through the existing workspace/quota/buffer policy. The
existing run schema, exact source identities, stable adjacent merges, bounded
readers and owned cleanup remain authoritative. No spill setting means a bounded
resident state attempt, with deterministic denial when its grant is insufficient.
There is no hidden unlimited collection or automatic external executor.

Producer failure during build, denied state/decoder/merge allocation, disk-quota
exhaustion, cancellation, corrupt runs and consumer failure prevent a successful
execution report. Drop and explicit finish retain their existing owned cleanup
rules; unknown files are preserved. A native writer publishes only after input
end, operator/sink completion, acknowledgements, source validation and successful
spill cleanup. Failed overwrites must preserve the original destination.

Recovery means validating ownership and removing abandoned query runs for an
explicit restart. Live ownership must refuse recovery. This does not resume
operator state or claim automatic recovery of a process-crashed output staging
file; those publication/recovery promises require separate acceptance.

## Acceptance before support claims

- Exact native cross-batch ordering with stable duplicate ties, nullable keys,
  multikey directions, Unicode, integer boundaries, empty/all-filtered input,
  nested sorts, filters/projections on both sides and non-aligned batch sizes.
- Global limits/offsets above and below ordering, including zero and an offset
  beyond the result; all inputs are demanded and late failures cannot succeed.
- Input witnesses expire before every next demand; retained output clones keep
  independent credits; all execution state returns to baseline after release.
  Existing file/resident/dynamic execution keeps its prior behavior.
- A frozen larger-than-grant workflow consumes at least four times its native
  grant, performs multiple native spill merges and checks every output against
  an independent oracle. Keep no-spill constrained denial and ample-memory
  controls. Record actual spill, shared-grant peak, disk quota and complete cleanup.
- Exercise the real Python SQL/DataFrame routes through incremental results and
  native write/reopen. Include slow/failed consumers, pre/mid cancellation,
  malformed late input, exhausted grant/quota, corruption, failed overwrite and
  live/abandoned run recovery. Preserve all failure observations.
- Run focused native and public regression checks first, then the required
  workspace formatter/linter/tests, existing whole-workflow portfolio and a fresh
  Full43 correctness observation on the final cohesive runtime. Freeze sources,
  executable, fixtures, oracles, limits and clocks before acceptance measurements.
  Use the existing serial workload/storage/process guards.
- Review complete diffs and source-linked evidence, update actual support/docs,
  and complete hosted integration. Do not infer a speedup, total RSS ceiling,
  broad stateful support, cross-platform runtime parity or new package release.

## Alternatives and risks

Keeping the outer per-batch executor would require duplicating stateful execution
and would still need a global completion contract. Moving it to the scan lets
existing operators own one normal lifetime. Copying at every scan would detach
input early but needlessly copy columns/rows later removed by filters; detach at
the first retaining boundary and at externally retained output instead.

Additional compact copies cost CPU and peak overlap; measure the whole workflow
and retain explicit counters. Multiple sort stages may copy again because each
must own its admitted input independently. The current merge schedule is unchanged;
actual run-size observation may inform the separate cost-aware scheduling screen.
That experiment and join/window/pivot spill remain outside this dependency's
support claim. General aggregation is covered by its separate design and tests.
