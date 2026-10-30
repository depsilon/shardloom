# Application batch-function acceptance review

The primary reviewed the final native callback, Python binding/registry, SQL
lowering, source-demand pruning, completion, ownership and exception paths
against the October 30 contract. This is an engineering review, not an
independent security assessment of arbitrary application code.

## Checked boundaries

- The application stage belongs to the existing flat relational pipeline. A
  callback's complete declared input remains observable; downstream column/range
  demand cannot cross the stage. Completion remains required so a downstream
  limit/count cannot erase a later callback failure. Renamed output columns now
  reach SQL join/materialized-relation binding through declared schema discovery.
- Registries retain immutable function snapshots. Lookup replacement/drop
  affects subsequent bindings, while existing plans keep their original handle.
  Registration and schema/explain/estimate inspection do not call user code.
- Input detachment precedes invocation, workspace is admitted before application
  work, and returned values are validated and independently owned before delivery.
  The reentry guard spans lazy output validation/intake; panic and error exits
  release it. Output bounds are rechecked after possible encoded expansion.
- Rust causes remain owned through the native error path. Python restores the
  original exception object and traceback even when it has a caller-owned
  `diagnostic` attribute. Failures do not retry, commit a write, or poison later
  explicit actions.
- No foreign query engine, dependency, process callback transport or plugin
  loader was introduced. Python's Arrow conversion is an explicit boundary.
  A plain SQL representation fails on a nonportable callback marker instead of
  silently omitting the application stage.

## Review findings resolved before package acceptance

- SQL column discovery initially kept pre-callback names. The existing pipeline
  lowering now reads the declared callback output and tests compose renamed
  fields with joins, aggregates and explicit materialization.
- The retained alias/resource tests were extended with an encoded native-output
  expansion case, proving refusal before publication when final independent
  intake exceeds the declared byte bound and proving credit refunds.
- The documentation sample is now replayed verbatim by the installed-native
  suite. The session registry path has a separate executable lookup/drop/snapshot
  test. Both are included by the existing installed suite's binding-file glob.
- New exports were initially inserted into the frozen legacy map. They now live
  only in the current map; the historical 243-name fingerprint stays unchanged.

## Hosted review follow-up

The initial review missed an existing Python filter-combination helper that
could move a downstream `dropna` predicate across the new callback stage. Hosted
review comment 4428880276 identified it. The helper now appends one filter without
rewriting preceding stages; `value_counts(dropna=True)` uses the same correction.
The original code fails both new callback regressions, including preservation of
an application exception on a null input. Corrected tests cover renamed outputs,
all admitted null-filter modes, downstream filters and observation counts.

The primary checked the adjacent statement renderers and connected-plan binder.
Their flattened paths decline application/observation stages; the connected
stage loop binds them in position. The source census is supporting evidence,
not independent semantic approval. The correction packet retains the failed
reproduction, all 16 passing callback tests and all 283 installed integration
tests, with unchanged native runtime hashes. It supplements the original packet
without replacing any earlier outcome.

## Limits retained explicitly

Trusted callback declarations do not enforce arbitrary host code's purity,
rowwise behavior or private allocations. Cancellation is cooperative; cross-thread
wait-on-self is prohibited by contract, and aborting Rust panics are not recoverable.
The stock CLI/subprocess path, dynamically loaded plugins, untrusted execution,
general scalar/aggregate UDFs and table generators remain unsupported.

The additional minimal-feature strict lint configuration reports existing warnings
in unchanged source. Its ordinary compilation succeeds. Required default and
full native strict lint pass; the separate disposition retains the exact failure.
The interrupted ordinary-debug native run stays incomplete; only the completed
`ci-native` workspace run establishes full native test acceptance.

The current workspace Rust crates remain unpublished/internal under the package
channel policy. Source embedders with exhaustive matches must account for the new
application error and pipeline-stage variants. This change does not publish a
crate or alter the installed 1.0.0 package contract.

No performance claim is made for application callbacks. The existing 347-case
observation does not exercise them and remains separate. Full43 remains held for
item 18; the additional shared-owner optimization queue remains active item-20
work after this unit's acceptance.
