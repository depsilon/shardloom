# Native application batch transformations

Implemented and locally accepted inside item 20, under RFCs 0011, 0021, 0023,
0033 and 0044. The [application API](../reference/application-batch-functions.md)
admits trusted in-process Rust/Python transformations; hosted PR acceptance is
pending. The preceding performance changes are frozen in merged PR #1627; their
source, timings and acceptance remain independent of this continuation.

## Shared execution contract

Add an explicit row-preserving batch transformation to the existing native
relation/pipeline owners. Trusted Rust callbacks and Python callables use this
same operator. A caller declares the exact ordered input and output schemas,
function name/version, determinism, null handling, effects, maximum input batch
rows, output bytes and callback workspace allowance before supplying code.
The first admitted contract is deterministic, rowwise, null-aware and free of
external effects. Rowwise means that changing the input batch partitioning
cannot change a row's output. Declaring determinism alone does not establish
that property. Output row count must equal input row count; separate aggregate,
table-generator and effectful function contracts are not implied.

Registration, composition, binding, explain and capability inspection are inert.
They must not invoke, import or deserialize application code. Plans capture an
immutable callback identity and its declaration. Replacing or removing a registry
name cannot mutate a previously bound plan. The callback is a trusted local
application boundary, explicitly enabled by its caller; it is not an untrusted
plugin loader or a sandbox. Existing untrusted UDF/plugin blockers remain intact.

The operator preserves the callback's plan position. Do not push filters,
projections, limits or metadata counts across it, deduplicate calls or infer
infallibility from a pure declaration. Normal native operators remain available
before and after it. Repeated references follow ordinary plan execution unless
the caller explicitly requests the existing materialized-relation owner. There
is no cross-action answer cache and no automatic callback retry.

Each nonempty admitted batch invokes the callback once, in source delivery order.
The engine may slice a larger input to the declared maximum batch rows. An empty
relation has the declared output schema and invokes no callback. The callback
receives input validity, including nulls, and must construct output validity
according to its exact schema. Incorrect fields, types, nullability or row counts
fail before that output batch is delivered. Earlier streamed batches remain
provisional when the complete action fails.

## Ownership, errors and resources

Use the current operation's native allocation, cancellation and delivery owners.
Check cancellation before invocation, after return and before output publication.
Synchronous application code must cooperate; the contract does not promise forced
interruption. Reject same-thread native-action reentry during callback execution
before acquiring an execution lock. Application code must not start and await a
second action on its active context from another thread.

Input arrays pass an accounted independent-copy boundary before invocation, so
retained application aliases do not escape a producer's private-batch contract.
Those copies keep their buffer credits through the final application alias.
Returned application buffers pass the existing native payload validation
and independent-copy boundary before publication. This makes the output's
lifetime and retained allocation independent of arbitrary caller buffers and
backing capacity. The conversion and copied bytes must be observable; it is not
a zero-copy promise. A later managed-output admission may avoid a copy only with
explicit provenance and lifetime proof.

Reserve declared callback workspace before invocation, hold it through callback
result validation/intake and charge native output allocation separately through
the existing allocator. Refuse invalid declarations and unavailable headroom
before executing user code. Output row and visible-byte bounds are checked
before publication, including any expansion during independent native intake.
Actual intake allocations always use the shared native grant. Arbitrary
application allocations remain a caller-owned
trust contract, not an enforced process-memory cap. Do not report that allowance
as measured callback allocation. No callback state spill or new scheduling pool
is introduced.

An unwinding Rust panic and a Python exception fail the current action without
publishing the failing batch or triggering a retry. Preserve Python exception
identity/traceback through the existing failure owner. Cancellation, declaration
failure and output validation have deterministic diagnostics. Cleanup retains
the normal first-error and last-alias credit guarantees. A new explicit action
may retry; a failed action cannot become a successful materialization or write.

## Provider and interface boundaries

Vortex-first decision: `implement_shardloom_kernel` for application admission and
the scoped callback dispatch, using existing Vortex 0.90.0 arrays, DTypes,
validity, allocator and native payload ownership. The pinned
`ScalarFnVTable`/`TypedScalarFnInstance`/`ScalarFnSession` APIs were inspected.
Their scalar strictness, arity and return-dtype contract does not replace a
table-schema transform boundary or provide ShardLoom's borrowed operation grant
and cancellation. Do not install an ambient callback global to force this
operator through a scalar vtable. No Vortex query-engine integration is used.

Typed relations, DataFrame composition and SQL over caller-bound relations must
reach this operator. Python uses an explicit columnar interoperability boundary;
it does not add pandas, Polars, DuckDB, Spark or DataFusion execution. An ordinary
query cannot serialize a Rust/Python callback handle into the standalone CLI.
Unresolved or untrusted function declarations must fail explicitly there. Any
portable CLI registration/transport requires its own concrete contract and
acceptance before claiming full five-interface function parity; neither a fixture
binary nor an embedded command adapter establishes stock CLI callback support.

Reports identify the application function/version, declared contracts, completed
batches/rows, output copying and failure boundary. Trust declarations are not a
proof of arbitrary callback internals. ShardLoom invokes no fallback executor.
Unsupported application behavior remains an explicit failure, not delegation.

## Acceptance

Test complete results for nullable multi-column transformations composed with
filters, joins, aggregates, explicit materialization and write/reopen. Verify
different batch boundaries, empty inputs, stable row order and repeated actions.
Cover inert registration/binding/explain; mismatched schemas/row counts/nulls;
missing/conflicting registrations and replaced-name snapshots; exceptions and
unwinding panics; cooperative cancellation; refused reentry; reservation refusal
before callback execution; retained input/output aliases and final refunds;
context closure; producer failure and explicit retry; output byte limits; and
complete failure/cleanup observations. Tests and installed-package evidence must
state each admitted or blocked interface accurately.

Freeze finite whole-workflow performance/resource controls before making a
performance claim. The existing 347 cases do not exercise these callbacks and
cannot certify them. Full43 stays held until item 18's I/O acceptance. Item 20,
CG-1 through CG-23 and package publication remain separate obligations.

### Local acceptance — October 30

The [acceptance record](../benchmarks/evidence/native-application-batch-functions-2026-10-30/acceptance.json)
and [compressed validation packet](../benchmarks/evidence/native-application-batch-functions-2026-10-30/validation.json.gz)
bind source inventories, commands, logs, failed attempts, runtime hashes and the
clean installed-package lifecycle. The [expert review](../benchmarks/evidence/native-application-batch-functions-2026-10-30/expert-review.md)
records resolved findings and retained boundaries.

| Gate | Observed result |
| --- | --- |
| Default workspace tests | 3,290 passed; none failed or ignored. |
| Full native workspace, checked `ci-native` profile | 6,571 passed; none failed; 35 explicit manual/platform checks ignored. |
| Formatting and required strict lint | Passed for default and full native workspace configurations. |
| Source Python suite | Passed; 915 tests run and 38 optional/installed-only skip observations. |
| Clean installed package | 280 integration tests passed, none failed or skipped; exact CLI/library hashes, install and uninstall verified. |
| New callback coverage | Nine native operator tests, one Rust SQL/embedding test, and 14 installed Python tests including the verbatim documentation example. |
| Additional minimal native feature configuration | Ordinary compilation passed; strict lint reports eight warnings in unchanged source, retained as an explicit gap. |

Coverage includes renamed output schemas in joins/aggregates/materialized SQL,
three-format write/reopen, nullable/rich output, batch partitions, immutable
registrations, empty/repeated actions, cancellation, refused reentry, original
Python errors, Rust unwinding, pre-invocation memory denial, producer failures,
encoded-output expansion refusal, retained aliases and final credit refunds.

The stopped ordinary-debug native suite remains incomplete and receives no full
suite credit. Its logs and source history are preserved; exact generated fixture
directories were retired only after the checked replacement passed, with repeated
identity and open-handle checks. No unrelated files or rollback copies were used.

### Hosted review correction

PR #1628 review found that Python `dropna` could merge its null predicate into
earlier filters, crossing application, projection and observation stages. The
helper now appends the filter at the requested position. Regressions exercise
renamed callback outputs, each null-filter mode, callback exceptions and metrics
before/after the filter. The original implementation fails the two callback
regressions with six assertion failures and six errors.

The [correction record](../benchmarks/evidence/native-application-batch-functions-2026-10-30/review-correction.json)
and [complete follow-up packet](../benchmarks/evidence/native-application-batch-functions-2026-10-30/review-correction.json.gz)
retain that reproduction and the passing correction: all 16 callback tests,
283 installed-package integration tests with no skips, and the source Python
suite's 915 tests with 38 explicit skip observations. The Python-only change
reuses the exact accepted native runtime; all 2,618 Rust/manifest/lockfile source
identities remain unchanged. The original acceptance packet stays immutable.

This accepts the declared local application boundary, not stock CLI callback
transport, arbitrary code safety, a new release, a measured speedup, or all of
item 20. The [additional shared-owner queue](native-shared-owner-optimizations-2026-10-30.md)
follows hosted integration before connector/refresh completion.
