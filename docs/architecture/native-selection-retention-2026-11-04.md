# Shared selection, retention and public construction

## Status and scope

The November 4 maintainer request extends the shared-work performance series
from merged revision `ac20b82118a4626bc5f47b530842318ed006f0d7`. The accepted
complete-cohort baseline is 1,513.095098453 engine seconds for the existing
10,000 requests. This investigation precedes item 19 under the maintainer's
explicit sequencing update. RFC 0044, item 20 and CG-1 through CG-23 keep their
independent obligations.

The first execution work unit combines shared join coordinates with sorted
payload validation and retention. A coordinated public-request workstream starts
with construction-only attribution and preserves the existing Python contracts.
Window partition spans and peer discovery have a separate correctness boundary.
Computed projection proofs and dictionary unary-expression reuse follow the
first work unit. Every candidate remains pending measurement and acceptance.

## Existing owners and Vortex provider check

The pinned Vortex 0.90 provider supplies native `ArrayRef::take`, casts,
`PrimitiveArray`, validity masks, chunked arrays and selection evaluation.
ShardLoom already uses those facilities in `native_payload`, `Table::gather`
and the direct numeric gather. Preserve those provider boundaries and their
existing representation admission. Selection metadata belongs to the existing
join `Pairs` and `Gather` owners. A matching length alone does not establish
that coordinates belong to a different source domain.

ShardLoom owns declaration checks, field visitation, missing-parent semantics,
credited coordinates, cancellation and retained-output lifetime. Its decimal
storage route also protects the admitted decimal domain from upstream scalar
precision assumptions. These obligations remain with their current owners.
No new dependency or external execution provider is required.

## Join selection contract

`native_payload::validate_selection` currently decodes and reserves an
`Option<usize>` coordinate vector for each hidden field. Recursive payload
selection can then rebuild the same provider index array. Reuse coordinates
and provider indices within each output batch and side, with source identity,
source length, missing positions and allocation credits retained by the owner.

Field type and nullability checks remain in their original field order. A shared
coordinate preparation must preserve the relative order of declaration,
coordinate and selected-value failures. Recursive child validation respects
parent masks. Missing coordinates below a null parent receive the existing
unobservable defaults, while observed invalid values retain their diagnostics.

The unmatched side can validate its output declaration without constructing
discarded null buffers. Keep retained output construction on the shared native
path. Avoid introducing a coordinate allocation into an all-retained primitive
path solely to support validation-only fields.

## Sorted payload contract

Keep the existing complete ordering-key evaluation and full key sort initially.
Validate selected payloads in the original sorted batch and field order. Carry
the requested output prefix and retained-field demand separately from keys and
validation obligations. Decimal and recursive payloads need the same omission
and first-error guarantees as current flat payloads.

Discarded values must still reach their validators. Only retained values should
receive output buffers. Preserve offset semantics, ties, nullable keys,
cancellation, resource refusal and source-read obligations. Computed projection
output may use an existing bounded ordering path only when its validation proof
also covers borrowed passthrough fields at the original evaluation boundary.

## Window investigation

The existing partition-membership orderer already knows partition spans. A
retained span owner must preserve the globally ordered path, lexical partition
visitation, ordinal tie order and clean low-memory decline. Positional functions
and ordinary ROWS frames without GROUP or TIES exclusion can avoid peer work
only when every function sharing that order permits it. Ranking, RANGE, GROUPS
and peer-aware exclusions retain their required peer information. Stored and
resident paths require independent checks.

## Compatible public request ownership

First recompute the supplied setup, collection and delivery attribution from
the archived 10,000 case receipts and their original clock boundaries. Then run
a bounded construction-only diagnostic with interpreter and bytecode-cache
identity. Attribute source loading and compilation, module initialization,
dataclass construction, client initialization and declaration construction
exclusively so nested work is counted once.

Choose facade changes from that profile. Public classes remain actual classes
in their canonical Python modules, with resolvable annotation globals, source
inspection, constructor signatures, dataclass metadata, frozen and slots
behavior, subclass behavior, identity and pickle compatibility. The previous
lazy-model prototype retains its dropped disposition.

Reuse existing `NativeSession`, `NativeRelation`, `NativePlan` and native result
owners. Retained declarations must reach complete-statement provider admission
without repeated rendering, traversal or parsing where semantics permit reuse.
Each action still checks source generations and obtains fresh resource grants.
Both native and subprocess transports remain supported, including a subprocess
installation without the optional native extension. Full report access retains
its complete response. Profile remaining collection adaptation before changing
that boundary.

## Evidence and acceptance

Start with existing outer-join and wide-ranking case IDs plus semantic and
resource counterexamples. Capture per-case and per-batch work counts separately
from scored execution. The right-outer control `c8396388e8c70075`, full-outer
control `54ee753adf8f1b20`, product-left control `612dc5dd63944870` and ranking
controls `ade4e107deda3e58` and `5dcca5478175cafd` retain their existing requests,
sources and expectations. Sets and membership are shared-selection controls.

Tests must cover retained and omitted fields, nested parent masks, missing join
sides, decimal and temporal validation, finite and extended floats, field-order
errors, duplicate and sparse coordinates, empty input, memory refusal,
cancellation and producer/output ownership. Public changes also require the
five existing interfaces, transport parity, provider eligibility and Python
compatibility checks.

A complete-cohort performance decision follows the existing binary gain rule
after correctness, resource and no-fallback checks. Engine clocks and public
construction clocks stay separate. Population coverage and arithmetic savings
scenarios do not establish avoidable work or causal speedups. Preserve every
observation, including regressions and failed diagnostics.

## Applied work observations

The existing operation context will aggregate completed work at selection,
field and output-batch boundaries. Its relational certificate will carry a
versioned set of integer counters for prepared and reconstructed coordinates,
typed validation, missing-side validation, join output and sort gathering.
The counters use fixed-size operation-local storage and checked accumulation.
They contain no input values or row-level trace.

Cell counts describe logical payload cells, including nullable slots. Sort
selected-cell counts include internal compaction and preserved private values
as well as terminal delivery. Their value-validation policy stays with the
original record owner. Buffer
bytes sum visible constructed output buffers and exclude provider intermediates,
allocator capacity and total memory traffic. Existing debug observations retain
their more detailed test scope. Scored execution and detailed diagnostic runs
keep separate artifacts and their original clock boundaries.
