# Native Python data and operation ownership

Status: implementation and complete local acceptance, October 11; hosted
integration pending. This contract follows the accepted local typed-input work.
The current source implements the optional binding against the shared engine.
The [acceptance report](../benchmarks/native-python-surface-2026-10-11.md) records
source gates, the isolated installed wheel, lifetime/failure checks and six paired
Python workflows. Distribution retains its own gates. The old experiment remains
historical; its unbuilt status is not a measured rejection.
Owners remain PERF-02/03/07/11/12 and the existing capability gates. Version 0.5.1
stays fixed; all remaining six areas/eight investigations remain in the queue.

The engine already computes in Rust. The priority is removing compulsory
native-columnar to Python-row to columnar conversions and per-value JSON input
transport, while retaining one shared native planner and execution implementation.

## Requested implementation order

1. Native Python result/batch owners over current ShardLoom-owned arrays, with
   continued native operations, existing writers, direct columnar export and
   explicit opt-in row conversion.
2. Direct columnar intake with exact shared schema semantics and real producer
   ownership/accounting; ordinary Python object input retains explicit conversion.
3. In-process session and complete-operation bindings using the same shared
   allocation, source identity, cancellation, diagnostics and certificates.
4. Native expression/prepared-plan handles feeding the same representation as
   actual SQL parsing. Extract callable core interfaces from CLI handlers where
   necessary; do not duplicate SQL or operator semantics inside Python bindings.

Native expression and relation declarations wrap the existing `Expression` and
`VortexRelationalPlan` trees. Their constructors describe work without source I/O;
preparation binds through the same source normalization, schema binder, kernels,
session and writers as SQL. Actual SQL retains its shared parser. A typed plan
does not render expressions into SQL, parse JSON cells or execute a Python
evaluator. The existing recursive binder-depth guard remains separate from total
operator count, which is admitted under the caller's memory allocation.

The direct Python API is `shardloom.native`: exact literals, expressions,
relations, windows and subqueries feed a retained `Session.plan()` declaration.
The existing high-level DataFrame builders still produce SQL declarations. Both
enter the same binder and native providers; this change does not silently replace
the existing expression API. `transport="native"` explicitly selects embedding
for the public client; the subprocess transport remains its default.

Ordinary embedded SQL also uses the public workflow router's existing file
provider selection. Eligible file aggregates, counts, projections and top-N work
retain those prepared providers instead of always entering the general relational
path. Selection is declaration-based and occurs before execution; errors never
trigger a provider retry. Memory and streamed sources retain their declared
bindings, and unmatched SQL uses the shared relational binder. Provider-specific
execution, resource and reuse evidence identifies the selected path. This closes
the provider-selection gap found by the complete Python workflow comparison.

Vortex-first provider check for these handles: reuse the current ShardLoom typed
frontend IR and prepared Vortex provider, rather than create an evaluator or a
second array representation. Native declarations carry conservative metadata
credits while being lowered; resulting bound plans and arrays retain their
existing owners. Compatibility source generations, changing collection
membership, spill and cancellation retain the same contracts as SQL execution.

The first three share an ownership boundary and may form one implementation
unit. Preserve the subprocess option for isolation, without capability divergence
or an implicit transport fallback. Cross-process columnar transfer requires a
real IPC/shared-memory contract; process-local C pointers cannot cross a pipe.

## Grounding and provider checks

The old `experiments/python-native` proposal is preserved on local branch
`codex/perf-remaining-work` at `be2a69f3`, outside shipping manifests. Its September
6 record says dependency resolution, build, load, tests and measurement are
pending. It did not fail a measured performance gate. Reuse sound ownership ideas
against current relational/streaming APIs; do not import its narrow operation set,
16-MiB source limit, 64-handle limit or other experimental caps into the product.

Current native owners include `ResidentVortexSession`,
`OwnedVortexResultBatch`, `ResidentMemorySource`, prepared relational execution
and per-batch native consumers. Owned-column intake rejects foreign/unbudgeted
arbitrary arrays. The new Python adapter copies imported Arrow values into
admitted native ownership when safe borrowing is unavailable; results from the
same controller reuse their existing owner. Vortex remains the execution substrate
and native persistence; Arrow is an optional compatibility boundary.

Native frames from different clients compose through the receiving operation's
existing resource admission. Preparation shares sources already owned by that
session and copies foreign input with the existing bounded native payload copier;
prepared reuse retains that transferred input. The transfer may materialize
encoded fields into private native buffers, and its columnar copy counter includes
the resulting value, offset and validity bytes. It does not export through Arrow,
construct Python rows, enter the source client's execution grant or create a new
pool. Generated ranges retain compact range metadata. Native source/result import
uses this same boundary, including after the source client closes. Original
source output bounds survive transfer; incomplete copies release their receiving
credits on cancellation or memory denial. This is an ownership-transfer contract,
not a claim that cross-client composition is zero-copy or preserves all encodings.

Official primary references checked October 10:

- [Arrow C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html):
  same-process exchange and producer release callbacks; not cross-process storage.
- [Arrow C Stream interface](https://arrow.apache.org/docs/format/CStreamInterface.html):
  streamed columnar batches, end/error/release lifecycle.
- [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html):
  Python capsule ownership and transfer protocol.
- [PyO3 parallelism guidance](https://pyo3.rs/main/parallelism): detach during
  substantial Rust-only work. Validate the exact chosen version and complete
  dependency/license graph before intake; the old proposal is not that proof.

Pinned Arrow Rust 58.3.0 and Vortex 0.85.0 offer relevant conversion/FFI surfaces.
Verify enabled features and safe provider APIs. Keep workspace unsafe-code policy
unless a separately documented, narrowly reviewed provider boundary is necessary.
No external engine, Arrow evaluator, new semantic engine or hidden row bridge.

## Optional binding dependencies

Supporting dependencies must improve native capability, complete-workflow
efficiency or safe interoperability. Evaluate their actual ownership, conversion,
scheduling and allocation behavior, including the final enabled feature graph.
Native-to-native composition retains existing owners; no compatibility library
may become a compulsory intermediate representation. Bindings depend on the
engine, and optional adapters initialize only when their boundary is requested.
Providers must cooperate with shared resource and cancellation policy. A valid
workload outside a provider's current capability remains implementation work:
extend, patch or replace the provider where justified. The adjacent timestamp
work below applies this criterion without narrowing the accepted storage domain.

The native binding crate is feature-gated; the lean workspace does not link
Python. The binding uses `arrow-pyarrow` 58.3.0, matching the existing Arrow
graph, PyO3 and its four support crates at 0.29.3, and `target-lexicon` 0.13.5.
The October 11 dependency audit rejected the initial PyO3 0.28.3 intake because
of [RUSTSEC-2026-0176](https://rustsec.org/advisories/RUSTSEC-2026-0176.html)
and [RUSTSEC-2026-0177](https://rustsec.org/advisories/RUSTSEC-2026-0177.html).
Both advisories identify 0.29.0 as the patched boundary. No waiver is used.
Arrow's adapter declares Apache-2.0 and Rust 1.85; the PyO3 family declares
MIT OR Apache-2.0 and Rust 1.83. The workspace's Rust 1.96 floor remains unchanged.
`target-lexicon` has no declared Rust floor; its actual compilation is part of
the workspace MSRV check.

The target parser declares Apache-2.0 WITH LLVM-exception. Its cached license
contains Apache-2.0 and the [LLVM exception](https://spdx.org/licenses/LLVM-exception.html),
which grants additional permissions. The dependency allowlist records that exact
expression. This intake adds Python/columnar interoperability and build-target
support, with no external execution engine.

The `arrow-pyarrow` crate archive omits root license/notice files while its source
headers refer to the Arrow project notice. The exact-version upstream
[license](https://github.com/apache/arrow-rs/blob/58.3.0/LICENSE.txt) and
[notice](https://github.com/apache/arrow-rs/blob/58.3.0/NOTICE.txt) match the files
in the already cached Arrow 58.3.0 sibling crates. Binary packaging must carry
these notices plus the selected dependency license texts; a component-only SBOM
does not supply those texts. Wheel staging, actual isolated loading and license
bundle verification pass locally. Hosted platform acceptance and public
distribution retain separate requirements.

The published 58.x adapter still requires PyO3 0.28. The reviewed
[`vendor/arrow-pyarrow`](../../vendor/arrow-pyarrow/README.shardloom.md) patch
backports Apache's [PyO3 update](https://github.com/apache/arrow-rs/pull/10134)
onto the exact 58.3.0 registry source: only the dependency requirement and an
explicit invalid-UTF-8 capsule-name error conversion change. The existing
aligned-buffer import remains intact. Registry/source hashes, patch identity and
exact-version `LICENSE`/`NOTICE` are retained in its provenance record; packaging
verifies and includes both texts. Arrow, Parquet and Vortex retain their existing
versions. This optional interoperability patch adds no execution provider or
advisory exception and must be removed when a compatible upstream adapter on the
approved Arrow/Vortex graph carries the patched PyO3 requirement.

## Per-call resources with retained results

The embedded adapter uses an opt-in serialized operation controller over one
resident Vortex session. Its memory pool can admit a different explicit allocation
between complete calls without replacing the pool, allocator, runtime or retained
array owners. Fixed-budget and concurrent-serving sessions keep their existing
behavior. Configurable sessions have no persistent background CPU drivers;
native operations use the current admitted execution-lane ceiling.

The controller intersects each call with the session's administrator ceilings,
keeps per-field origins, and holds configuration across preparation, execution and
the requested output boundary. Increasing an authorized grant does not grant an
independent pool. Lowering it below live retained ownership fails before input
demand. A call override never replaces the configured inheritance source for later
calls. The last admitted allocation remains with retained owners until the next
explicit operation; lifetime peak reservations may exceed a later reduced limit.
Native Python plan, input, result and stream operations must all use this controller.
Admission waiting checks cancellation. External caller memory and whole-process
RSS remain outside the reservation counter.
Recursive calls on the same controller fail explicitly instead of waiting on
their own admission lock. Importing a result owned by that controller reuses its
native result owner directly. A generic producer callback must not recursively
enter another operation on the same controller.
Nested callbacks may enter an available different controller, but cannot wait
for a busy one while holding their original grant. This prevents crossed imports
from deadlocking two sessions. The thread-local nesting counter controls lock
ordering only; it carries no data or resource allocation.

General embedded commands must enter this same owner after their existing Rust
argument parser resolves the allocation. Thread an explicit invocation context
through the shared dispatcher and data handlers; do not use ambient allocation
state or infer data access from command-name suffixes. A scoped admission guard
holds the existing controller through input, preparation, output and reporting.
Resource-free discovery may use an unconfigured owner; the native runtime is
created only after an explicit allocation is available. A first call's override
must not become the inheritance source or administrator ceiling for later calls.
Cancellation/deadlines use the session's child token at cooperative checkpoints.

Vortex-first decision: `wrap_vortex_concept` around the existing admitted
`ConfigurableResidentSession`, `ResidentVortexSession` and `LiveMemoryPool`.
This is ShardLoom invocation/admission plumbing; upstream Vortex has no Python
client command inheritance contract. The change must reuse existing source,
compute and writer providers, without another allocator, executor or row bridge.
Focused native tests verify shared-owner retention, lowered-grant refusal,
resource-free discovery, per-call inheritance and producer-callback admission.
General embedded commands now use the shared invocation context and session
cancellation owner. Complete source and installed-package verification remain
part of acceptance; a refused unaccounted owner is not the completion target
for useful existing operations.

Primitive result handles reuse the existing prepared native count, unary,
aggregate and sort producers. Vortex-first decision: `wrap_vortex_concept`
around their admitted Vortex arrays and the existing `OwnedVortexResultBatch`;
no new operator kernel, Arrow evaluator or query-engine integration is introduced.
Complete collection retains batches and their container credits in the same pool,
without inheriting the small JSON endpoint's row/byte bounds. Existing metadata
count, encoded scan, finalization and native spill choices remain with their
providers. Native I/O and runtime evidence comes from that same execution, and
Arrow/Python rows remain explicit conversion endpoints. Focused native tests
verify empty typed schemas, complete values beyond the former small-result
limits, pressure, cancellation, cleanup and retained-owner lifetimes.

Directory and manifest sources retain every member's native source owner and
revalidate collection membership when reused. Count, unary and structured
operations use the shared relational lowering where appropriate. Partitioned
aggregation and sorting call their existing cross-part providers and finalize
directly into native batches. Source-order limits and predicates retain their
global meaning; wide sorting materializes selected payloads in final global
order. These paths support complete collection, incremental delivery and native
Vortex writing without serializing result cells through the JSON endpoint.
Existing materialization and spill admission still applies; this binding adds
neither a new spill capability nor another operator implementation.

## Incremental delivery and explicit Python objects

The binding's incremental operation keeps one shared prepared plan and runs the
existing native batch consumer. Its worker starts on first demand, queues at most
one converted Arrow batch, and waits for the next demand before advancing. The
execution report becomes available only after final source validation and worker
completion. Closing or dropping the stream cancels its child token and joins the
worker; session cancellation also reaches the child without a stream close
cancelling unrelated session work. Retained batches remain provisional and valid
after close, but do not establish a completed transaction.

The allocation remains held during consumer waits. An attempted same-session
operation receives an actionable refusal instead of waiting for the consumer
that initiated it. A completed batch's direct Arrow export needs no new native
operation. Plan-cache release also refuses while the plan is executing.

Completed result exports retain a separate one-shot stream/report owner. Report
fields distinguish native shared bytes, representation capacity, conversion-work
reservations, canonicalization calls, demand counts and completion. These are
referenced-region observations, not unique physical bytes, measured decoding or
process RSS. Requested-schema casts are refused before capsule ownership moves.

Python row construction is a separate explicit endpoint. It reads admitted
columnar batches directly, preserves exact integers/binary/decimal/nested values,
and follows the existing temporal rule: represent calendar-compatible values as
Python date/datetime objects, retaining exact epoch integers beyond Python's
calendar range. Caller-owned Python containers are outside native reservations.
Neither Arrow export nor continued native execution invokes this endpoint.
Compilation, integration tests and packaging are separately evidenced below.

The embedded SQL plan now carries the existing explicit native spill declaration
into the shared relational preparation path. Validation uses the CLI's strict
spill parser and rejects a buffer larger than the effective call grant before
demanding a batch producer or inspecting file input. The existing Vortex-native
ordering/aggregate/join/window/pivot spill provider retains the same session;
the binding adds no spill implementation or independent resource pool.
Python plan reuse includes the spill declaration so changed permissions cannot
reuse stale preparation. Collection, incremental delivery and writers use that
same prepared operation. Focused integration verifies complete 24,001-row spilled
ordering, retained result lifetime and cleanup, plus 4,097-row public collection,
incremental delivery and native Vortex output. Invalid spill options refuse before
producer demand or output creation. The shared command owner, explicit primitive
options and typed expression handles now have their own integration coverage.

Native multi-output writing uses the existing shared `write_many_controlled`
provider and its staged publication/recovery contract. Target validation precedes
factory demand; duplicate, existing and invalid destinations refuse before input
or staging. One retained session and cancellation owner cover all targets. The
provider replays the native query once per adapter, as its report states; the
binding does not introduce result caching. Focused checks reopen all eight output
formats and verify every value, empty outputs, retained owners and producer
failure without prefix publication. Streaming batch fanout retains the shared
provider's explicit refusal; a streaming writer admits one native Vortex target.
The six provider fanout tests passed during implementation. The final installed
native binding suite passes all 63 tests, including command ownership, primitive
and partitioned operation options, typed expressions and shared SQL provider
selection. Complete local source, runtime and wheel gates also pass; hosted
acceptance remains separate. These counts overlap and are not independent
workload or performance observations.

## Object and factory intake

The optional in-process adapter accepts the public `from_rows`, `from_arrow_table`
and `from_batches` declarations. Schema metadata uses the shared exact type
interpretation. Native conversion builds typed column buffers for Python mappings;
cell values do not pass through JSON. Columnar producers use the pinned Arrow
stream provider, with imported values copied into admitted native ownership where
borrowing would leave mutable or unaccounted buffers. Caller containers and
producer allocations remain outside ShardLoom's reservation counter.

Batch factories are opened only inside operation admission. Resident factories
build fresh native sources for each call. Streaming factories feed the existing
native batch executor on demand; they do not replay prior results. The original
Python producer exception crosses back to the caller. Exhaustion closes the
producer before writer completion, and interruption closes it while draining the
native worker. Existing destination refusal occurs before factory demand.

Focused local integration includes 4,099 nonempty batches following 8,193 empty
batches, a 4,097-row Arrow batch, and two retained complete 4,097-field outputs.
The latter explicitly grants 512 MiB for their combined ownership. These checks
verify complete values and exercise resource refusal, producer failure and owner
lifetimes; they establish neither a new schema/batch ceiling nor a performance gain.
The expanded integration suite exercises complete SQL operations, native
primitive declarations, multi-file collections and typed native plans. Broad
regression and paired measurements remain separate acceptance obligations.

## Native wheel proof

`stage_native_python_package.py` combines explicitly supplied built artifacts in
a fresh local directory. `check_native_python_wheel.py` checks the exact native
library, CLI, Python sources, package tag and every wheel RECORD digest. Its
notice check requires the project's own legal files plus all texts in
`python/THIRD_PARTY_LICENSES`. The conservative bundle covers every locked
registry dependency and reviewed vendored patch, including build, test and
platform packages; it is not a linked-component SBOM or a license-policy decision.

`prove_native_python_wheel.py` builds that stage and creates a clean virtual
environment, installs the exact wheel and pinned optional Arrow consumer, verifies
imports resolve inside that environment, and runs the complete required native
binding integration suite. No-test and skipped-test outcomes fail. It preserves
failed logs and emits artifact identities, test counts and installation evidence.
Build and runtime proof on a developer Mac must still use the storage/workload
supervisor. These commands do not publish or install into the caller's environment.

The dedicated CI lane covers Ubuntu Python 3.10/3.13 and macOS Python 3.13.
The declared stable ABI is `cp310-abi3`; successful load must be observed on each
tested interpreter. Linux wheels use `linux_<architecture>` until a separate
shared-library audit proves portability. Windows native extension support and
unobserved platforms are not implied. Existing CLI-only wheels retain their tags.
CI pins [PyArrow 25.0.1](https://pypi.org/project/pyarrow/25.0.1/), the consumer used
by local integration, and build tooling separately with reviewed wheel hashes.
PyArrow is an optional interchange test dependency, not a Python execution engine.
An earlier local wheel passed a clean install and all 32 then-current integration
tests; its record remains historical. The final `native-wheel-proof-300b` verifies
the release extension and CLI, every wheel RECORD entry and notice, isolated
imports, and all 63 native integration tests without skips. Its exact source and
artifact hashes appear in the acceptance report. Hosted CI acceptance remains
required for the expanded surface.

## Lifetime, conversion and evidence

Native plan and input methods accept an optional timeout in seconds. The public
client forwards its timeout through input conversion, preparation, collection and
writers. Incremental execution starts the deadline at first demand and retains it
while awaiting the consumer. Deadlines use the existing shared cancellation token
at native checkpoints, without a timer thread per call; they do not forcibly
interrupt an arbitrary foreign producer callback. Zero seconds refuses before
input demand or output creation. Invalid values fail before work begins.

The public native client exposes `cancel()` for active and queued operations.
It renews the session cancellation owner for subsequent calls while retaining
completed results. A timed-out child operation does not revoke its parent session
or sibling operations. Generic commands now enter the same invocation owner and
carry their deadline and cancellation token through the shared dispatcher.
Focused integration checks prove deadline and cancellation behavior for the typed
plan, input, result and stream methods.

Results must retain arrays, runtime and credits after session/plan handles drop.
Explicit close revokes new work without invalidating independently retained
results. Cancellation, interrupted iteration and downstream exceptions must drain
admitted work, stop input demand and release the exact owners. Sharing external
buffers must retain the producer and account honestly; copy mutable/incompatible
input when required. Never borrow merely because an object exposes a capsule.

Native plan and input failures freeze resource evidence while their operation
still holds the admission guard. Error metadata contains the declared and
admitted allocation, retained/peak native reservations and denied-reservation
count, with the existing session-lifetime observation scope. A rejected
admission records the requested declaration and unavailable observations; it
cannot borrow a preceding operation's counters. The public client retains these
fields in returned or raised error envelopes, and failed streams never publish
a completed report. Input-producer exceptions retain their original identity.

Encoded Vortex layouts may require decoding/materialization for Arrow. Share
compatible buffers when sound and convert required representations once at the
chosen boundary. Record copies, decoding and retained memory separately. Ordinary
Python dictionaries require conversion. One boundary crossing per operation or
batch is the target; avoid per-row native calls and Python callbacks in Rust work.

Acceptance must cover current shared operator/type capabilities, empty/nullable/
nested and endpoint values, slicing/aliasing, released/malformed producers, retained
results, session close, mutation, cancellation, resource refusal and all writer
fidelity contracts. Measure complete Python workflows separately from native
Full43: large columnar intake/output, retained chained results, many short calls,
wide plans and native-file queries with small output. Keep setup, native execution,
conversion and full-return timing distinct and retain paired raw measurements.
Do not transfer historical tiny-worker latency or current Full43 timings into an
unmeasured binding speedup claim. No publication/version bump is authorized by
this implementation request.

## Explicit adjacent follow-up

Track full-domain microsecond timestamp compression and safe statistics through
the pinned writer/provider boundary under existing ingest/pruning owners. Preserve
the complete Int64 domain; do not trade correctness for statistics. Current typed
input omits global file min/max when timestamps are present and preserves timestamp
storage uncompressed because the pinned provider's calendar validation is narrower.
Restore useful compression and field-appropriate statistics only with endpoint,
pruning, complete-value, size/CPU and end-to-end evidence. This item remains
separate from Python boundary performance.
