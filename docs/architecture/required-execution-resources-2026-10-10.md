# Required execution resources

Status: implemented with complete local engine acceptance and independent
inspection. The [acceptance report](../benchmarks/required-execution-resources-2026-10-10.md)
records the exact source and evidence. Support and rendered-guide checks pass;
hosted integration remains pending. This contract was directed by the maintainer
on October 10.
The typed-input work retains its separately frozen source and evidence.
Keep version 0.5.1 fixed. This work belongs to existing PERF-03/06/11/12 and
CG-19/20/21/23 owners; the six remaining areas, eight investigations and all
CG-1 through CG-23 remain visible.

## Contract

Every operation that inspects data, prepares input, consumes a producer, executes
a plan, spills or writes must resolve a complete, explicit memory and execution
parallelism allocation before beginning that work. Pure lazy construction and
side-effect-free capability/plan descriptions remain resource-free. Remove
built-in numeric memory/parallelism defaults from production entry points and
internal execution constructors; explicit fixture allocations remain legitimate.
Missing, malformed, nonpositive, overflowing or unauthorized values must fail
with a stable actionable configuration diagnostic. Never substitute a number or
infer permission from total host hardware.

An allocation can come from execution arguments, an explicitly configured
context/session, or a platform caller using the same configuration contract.
Configuration is immutable after resolution for one complete operation. Resource
inheritance and overrides need one shared rule: start from the configured
context/session allocation, apply only supplied validated call overrides, and
require both values in the final allocation. No call override may exceed an
explicit administrator ceiling. Retain per-field origin when an override changes
only one field; do not label the entire allocation as a single origin incorrectly.
No platform adapter or administrator policy is invented merely to complete local
configuration: provide the validation boundary and report the local policy scope.

`memory_gb` denotes GiB (2^30 bytes), as the existing public implementation uses.
The internal allocation is exact bytes with checked arithmetic. Provide an exact
byte configuration path so future platform allocations need not round up to whole
GiB; reject conflicting byte/GiB declarations. An integer execution-lane ceiling
is separate from any fractional CPU quota supplied by a deployment. Never map
host core count into caller authorization. Automatic sizing, scheduling,
capillary demand and admitted native spill continue inside the declared grant.

Environment variables remain usable only through deliberate validated loading;
module import must neither capture ambient budgets nor silently repair invalid
values. Loading a partial environment must identify the missing field. Explicit
execution configuration must not accidentally consult unrelated ambient values.

Preparation, execution, retained intermediates, spill and output must share the
same allocation and actual ownership accounting. Forwarding the same numbers to
separately created pools is insufficient when their allocations overlap. Sequential
stages may transfer an exhausted/released grant; overlapping retained state must
remain charged. Input Python objects, allocator overhead and unreviewed provider
allocations are not automatically covered by a native reservation counter.

## Shared configuration and provider ownership

`ExecutionResources` is the provider-neutral validated declaration. It has no
default constructor, retains exact bytes and per-field origins, and resolves
partial overrides against an explicit inherited declaration. Deployment ceilings
intersect with inherited ceilings; neither omission nor a larger override removes
an existing authorization limit. Invalid declarations use the existing stable
`SL_CONFIGURATION_ERROR` diagnostic. This object grants permission; it does not
reserve memory, authenticate a platform or measure resource use.

The existing `ResourceBudget` and `MemoryBudget` describe runtime task limits and
memory policy respectively. `LiveMemoryPool` and native leases track actual
admitted owners. Preserve those distinctions and map the resolved declaration
into the existing owners. Do not create another allocator or memory pool merely
to hold configuration.

Vortex-first decision: `wrap_vortex_concept` for caller policy around the existing
admitted Vortex session and host allocator. The provider inventory and RFC 0044
already establish those owners; Vortex does not supply ShardLoom's public
configuration inheritance or deployment authorization. This change introduces no
new provider, encoding, execution kernel or materialization boundary. Native
execution, buffer lifetime and output remain under the existing provider gates
and certificates, with no external executor or fallback.

## Report contract

Preserve existing public evidence while distinguishing requested memory bytes
and maximum execution lanes, per-field configuration origin, admitted allocation
and policy reasons, measured live/peak native reservations, actual lane use when
measured, and actual spill events/bytes. Requested limits are neither preallocated
memory nor measured peaks. Maximum parallelism is permission, not utilization.
Missing measurements remain unavailable rather than zero. Do not claim a
whole-process RSS bound or complete provider accounting without separate proof.

## Grounded implementation boundaries

The initial inventory below describes the source before this change.

- `python/src/shardloom/runtime_defaults.py` currently resolves environment values
  at import and replaces missing/invalid values with 4 GiB and two lanes.
- Python terminal signatures embed those constants. `_terminal_resource_kwargs`
  validates positive values; direct client facades can omit both and reach the
  Rust fallback. Context/session constructors currently retain no allocation.
- `shardloom-cli/src/runtime_defaults.rs` repeats the environment fallback.
  `public_workflow_effective_resource_envelope` and the separate SQL source parser
  resolve independent numeric pairs; compatibility preparation forwards them.
- `VortexLocalPrimitiveResourceEnvelope`, `VortexLocalPrimitiveExecutionPolicy`
  and `VortexTopLevelExecutionProvider::default` still introduce native defaults.
  `ResidentVortexSession::new(memory_bytes, max_parallelism)` already requires both.
- Existing resource transport tests cover collect, writers, fanout, SQL,
  DataFrame and session requests. One test explicitly codifies invalid-environment
  fallback and must become a deterministic rejection regression.

These are starting points, not an exhaustive admission audit. Before editing,
inventory every executable production constructor, ingest and worker dispatch
route, distinguish report-only fixtures, and trace preparation ownership.

## Acceptance

Use one conformance matrix for CLI, persistent worker, Python direct client,
context/session, SQL/DataFrame, ingest/preparation, collect, incremental output,
every writer and the future in-process binding. Cover neither/one/both supplied,
configured inheritance, field overrides, invalid environment, overflow, booleans,
zero/negative numbers, administrator ceilings and source/result ownership across
preparation. Retain explicit fixture allocations throughout the existing suites.

Missing/invalid tests must observe zero producer pulls, zero prepared source opens,
zero output/spill creation and no fallback. Validate at the native boundary too,
so callers bypassing Python cannot obtain defaults. Exercise a valid operation
after rejection to prove cleanup and session reuse. Reports must bind declaration,
admission and actual measured evidence to the same operation. Update examples,
README doorway links, field guide, diagnostics, machine-readable schemas and
required broad/feature/public acceptance together. This is a deliberate public
configuration change, not a speedup claim or an artificial dataset-size ceiling.

## Eager boundaries and compatibility

Allocation validation precedes Python row inspection and cell encoding, pandas
record conversion, Arrow table/IPC conversion, calendar generation and session
preparation fingerprints. File/SQL declarations and inert batch factories remain
lazy. The existing declaration-only producer tests do not prove rejected execution
leaves a producer untouched; add explicit rejection tests for each work boundary.

This deliberately changes omitted-resource execution and the exported numeric
defaults. Migrate examples and fixtures to an explicit configured context or
execution allocation. Preserve resource-free discovery and lazy construction.
The historical defaults and host-detection options in RFC 0014 are superseded
for execution by this contract; no implicit hardware allocation is admitted.

The native session already owns an exact-byte LiveMemoryPool. Preparation's
NativeIngestMemory currently creates another pool. Trace and enforce their
lifetimes: sequential preparation may release its owner before query admission,
but retained or concurrent state cannot receive independent copies of the full
grant. The current worker retains only its latest prepared operation and clears
it before admitting a changed operation; preserve that behavior. Correct the
local-engine primitive path that currently forwards parallelism into a constructor
which replaces the request's memory budget with 4 GiB. Remove the ingest stream's
implicit 1-GiB prefetch allocation when no explicit native memory owner is supplied.

## Implemented source contract

The shared Rust and Python `ExecutionResources` declarations now validate exact
bytes, origins and intersected authorization ceilings without numeric defaults.
Python contexts, sessions and terminal calls resolve through that declaration;
CLI commands and persistent-worker requests validate it before data access.
`--resources-from-env` and `ExecutionResources.from_env()` deliberately load the
existing environment variables. Ordinary imports and requests do not consult
ambient resource settings. SQL text remains independent of the allocation.

Native session, policy and writer constructors require an allocation. Relational
compatibility preparation receives the existing session's `LiveMemoryPool`, so
retained input and preparation compete for the same credits. Sequential standalone
preparation creates its admitted owner only when needed. Writer refusal precedes
producer pulls and output creation; the shared-pool tests retain another input
owner across denial and verify a successful retry after credits are released.

All execution reports attach the declared bytes, lane maximum and origins.
Resident count, filtered count, unary, aggregate and relational paths attach the
actual admitted session snapshot after producing their native result buffers.
The memory scope is the session pool lifetime, including preparation and retained
owners; CLI envelope formatting, provider bypass allocations and process RSS are
outside that counter. Actual peak active lanes remain unavailable because there
is no measured lane-use counter. Legacy or uninstrumented paths retain explicit
unavailable admission/usage fields instead of presenting permission as measurement.

Spill evidence records actual activity. Weighted grouped count also records
cumulative native payload bytes written. Its disk quota separately includes
reserved workspace metadata, so those quantities cannot be compared as the same
measure. `execution_resource_spill_observation_scope` identifies payload-byte
coverage, an uninstrumented byte count, or observed absence of spill. No whole
process ceiling, new execution provider or performance improvement is claimed.

Resource rejection, shared ownership, worker reuse and report tests pass, along
with all 16 fresh source/feature gates, 655 Python tests, 32,497 complete public
cases, retained streaming/type/pressure families and all 129 Full43 results.
Independent inspection verifies the immutable packet and all 66,055 authoritative
resource declarations. The report preserves the original reader's projection
counting failure and its separate correction. Support and rendered-guide checks
also pass. Hosted acceptance remains required before this implementation unit
is marked complete.
