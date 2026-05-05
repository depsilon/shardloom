# Vortex Adapter Integration Plan

## Purpose

ShardLoom will integrate Vortex through a narrow adapter boundary so core crates stay ShardLoom-domain-first while Vortex-specific integration remains isolated.

## Adapter principles

- Vortex-specific upstream API usage stays in `shardloom-vortex`.
- Core crates use ShardLoom domain types.
- Adapter maps upstream Vortex concepts into ShardLoom concepts.
- Adapter does not execute fallback engines.
- Adapter avoids unnecessary decode.
- Adapter exposes unsupported features as diagnostics.
- Adapter preserves native Vortex output as highest-fidelity.

## Boundary layers

- Vortex dependency boundary.
- Vortex metadata inspection.
- Vortex DType mapping.
- Vortex encoding/layout mapping.
- Vortex statistics mapping.
- Vortex read planning.
- Vortex output planning.
- Future Vortex actual read/write.

## First integration milestone

First future dependency PR should:

- Add upstream Vortex dependency.
- Add version/license notes.
- Implement metadata-only opening if safe.
- Add no actual data reads unless unavoidable.
- Add tests using synthetic/minimal safe fixtures only if available.
- Keep all unsupported features explicit.

## Second integration milestone

- DType mapping.
- Encoding/layout mapping.
- Statistics mapping.
- Segment descriptor population.
- No full decode default.

## Third integration milestone

- Metadata-only scan plan.
- Segment pruning input.
- Explain/estimate integration.
- No execution kernels yet.

## Fourth integration milestone

- Native Vortex output planning.
- Output fidelity mapping.
- Translation report integration.
- No actual write until writer contract is proven.

## Risk areas

- Upstream API instability.
- Over-coupling to internals.
- Accidental decode-to-Arrow default path.
- Metadata loss.
- License/provenance drift.
- Tests requiring external files.
- Object-store assumptions.
- Feature flags increasing dependency footprint.

## Success criteria

- Core code remains Vortex-aware but not upstream-Vortex-coupled.
- `shardloom-vortex` owns upstream dependency.
- Vortex remains native input/output.
- Unsupported features fail clearly.
- No fallback engines introduced.
- Tests prove adapter mappings.

## Adapter report contract

- All adapter reports must render non-empty diagnostics in human text.
- All adapter report `has_errors` methods must be severity-based.
- All adapter reports must keep fallback execution disabled visible.
- All adapter report text builders should avoid `push_str(format!(...))`.
- Public `Result`-returning adapter constructors need `# Errors` docs.
- `ShardLoom` and `DType` should be backticked in Rust doc comments when needed for clippy.

## Metadata summary layer

- Metadata probe results are normalized before scan planning.
- This layer is the future input to pruning/explain/estimate.
- It must remain no-decode and no-materialization.
- Object-store support remains out of scope.
- Vortex output remains highest-fidelity.

## Metadata summary to planning bridge

- `Vortex` metadata summaries now convert into scan/explain/estimate planning reports.
- The bridge remains plan-only and side-effect-free.
- It prepares future segment pruning and richer scan planning integration.
- Actual scan execution remains explicitly out of scope for this stage.

## Metadata pruning bridge

- Metadata planning now feeds conservative pruning decisions.
- `PredicateProof` derivation is conservative.
- Incorrect pruning is a correctness bug.
- Future execution should consume these pruning reports before reading data.
- Actual scan execution remains out of scope.

## Dependency footprint staging update

- Adapter dependency posture now uses feature gates: `upstream-vortex`, `vortex-file-io`, `vortex-object-store`, `vortex-write`.
- Default build keeps upstream Vortex disabled to reduce transitive footprint.
- File IO/object-store IO stay out of scope and disabled by default.
- No fallback engine dependency or behavior is introduced.

## Feature-gated metadata-only open contract

- Metadata-only local file open is the first IO seam for `shardloom-vortex`.
- The seam is explicitly no-scan/no-decode/no-write.
- The contract is designed to feed metadata summary/planning/pruning flows.
- Actual scan execution remains out of scope.
- Object-store and write support remain future feature-gated work.

## Universal input bridge

The `Vortex` adapter now accepts normalized `UniversalInputSource` values.

Native `Vortex` input can flow into metadata open/summary/planning/pruning reports, providing the input-side counterpart to native `Vortex` output fidelity.

Actual scan execution remains out of scope.

## Metadata-driven read planning

- Vortex metadata/pruning reports can now produce read planning reports.
- Read planning creates segment intents and split descriptors only.
- Split descriptors do not execute reads.
- Byte ranges are intentions, not IO.
- Actual scan execution remains out of scope.
- Object-store and write features remain future gates.

## Runtime task graph bridge

`Vortex` read planning reports can now produce `ShardLoom` runtime `TaskGraph` plans.
Segment read intents are mapped into `SegmentTask` skeletons only, and tasks are not executed.
Byte ranges remain read intentions only for future scheduling.
This prepares future scheduling/execution boundaries while keeping actual scan execution out of scope.
Object-store and write capabilities remain future feature gates.

## Adaptive sizing bridge

`Vortex` read planning reports and runtime task-graph bridge reports now feed directly into adaptive sizing planning. Segment read intents, split descriptors, byte-range intentions, and runtime mappings are converted into memory-aware sizing decisions.

Missing estimates are preserved as `NeedsEstimate` decisions rather than guessed byte sizes. Byte-range intents can contribute encoded-byte estimates only when safe to derive from known ranges.

This bridge remains plan-only: no tasks are executed, no data is read, no decode/materialization is performed, and no object-store IO or writes are issued. It prepares future scheduling and memory-aware execution while preserving no-fallback behavior.

## Scheduler and queue planning bridge

`Vortex` memory bridge reports now feed scheduler/queue planning in a planning-only bridge.
Scheduled batches are advisory only and are not executed.
Blocked tasks remain explicit in queue-classified planning outputs.
Spill-required tasks are not silently run and remain blocked until explicit spill support exists.
This bridge prepares future execution scheduling without introducing runtime task execution.

## Execution readiness gate

Scheduler planning reports now feed a deterministic `VortexExecutionReadinessReport` gate across the end-to-end Vortex planning chain.
All readiness gates must pass before any future execution path is attempted.
`VortexDryRunContract` is reporting-only and does not execute tasks.
Dry-run reporting does not read data.
Actual execution remains out of scope in this phase.

## Metadata-only execution spike contract

The current Vortex execution spike is contract-only.

It validates the full Vortex planning/readiness/dry-run chain (`vortex-input-plan` -> `vortex-read-plan` -> `vortex-task-graph` -> `vortex-execution-readiness` -> `vortex-dry-run`) while remaining side-effect free.

This spike does not execute tasks, read rows, decode data, materialize values, write files, perform object-store IO, or perform spill IO.

Fallback execution remains disabled throughout this chain.

Future real execution must pass readiness gates before any executor is introduced.

## Metadata-only executor skeleton

- Metadata-only executor is feature-gated.
- It only handles no-op and metadata-only decisions.
- It does not read data.
- It does not decode arrays.
- It does not materialize values.
- It does not write files.
- It does not perform object-store IO.
- It does not perform spill IO.
- It blocks all tasks that would require data reads or side effects.
- It is the first executor-shaped layer, not a general executor.

## Encoded-read readiness contract

Encoded-read readiness is not execution. It classifies future encoded-read candidates, blocks decode/materialization/object-store/write/spill/external-effects/unsupported paths, preserves unknown estimates as blockers, and prepares the future encoded-read executor.

## Encoded-read executor skeleton

- Encoded-read executor is feature-gated.
- It is blocked-by-default.
- It consumes encoded-read readiness reports.
- It does not call upstream Vortex scan execution.
- It does not read data, decode, materialize, or write.
- It blocks unsafe candidates.

## Encoded-read public API boundary

This boundary is not execution.

It classifies public upstream `Vortex` APIs before future encoded-read probes and keeps usage isolated in `shardloom-vortex`.

The boundary blocks data-read, decode, materialization, write, object-store IO, and fallback execution paths.

Future execution work must pass existing readiness gates and this encoded-read API boundary before any probe or execution path is enabled.

- Phase 7A adds a contract-only encoded-read probe plan that combines API boundary and readiness reports without executing scans, reading data, decoding/materializing, object-store/write/spill IO, or fallback execution.

## Metadata-only local Vortex open transition

This transition is the first real upstream `Vortex` contact for `ShardLoom` and is feature-gated behind `vortex-file-io`.

Scope and guarantees:
- local-file only;
- no upstream scan/start-read API calls;
- no decode or materialization;
- no object-store/write/spill IO;
- fallback execution remains disabled.

If metadata-only behavior cannot be guaranteed from public upstream APIs for a requested path, the contract must return deterministic `ApiDeferred` diagnostics rather than attempting unsafe IO.

## First encoded-read execution spike

- Feature-gated behind `vortex-encoded-read-spike`.
- Local file only.
- Requires readiness, API boundary, and probe checks before attempting execution.
- Does not decode or materialize data.
- Does not perform object-store IO, write IO, spill IO, or fallback execution.
- May return deterministic blocked/deferred status when public API safety is unclear.


## Minimal query primitives

`ShardLoom` now models `CountAll`, projection, and filter primitives for `Vortex` query intent.
`CountAll` can be metadata-answered when `row_count` metadata is known.
Projection/filter remain `encoded_read_required` or deferred.
No scan/decode/materialization path is introduced in this phase.
This begins visible engine behavior without broad query execution.

## Metadata-filtered count primitive
- `CountWhere` can be answered from metadata only when predicate proof is decisive.
- Missing or inconclusive stats return `NeedsEncodedRead` / `NeedsPredicateEvaluation`.

## Staged manifest file write contract

- This is a report-only write contract for `ShardLoom` `Vortex` staged manifest draft-file flows.
- It does not write the draft file yet.
- It models blockers and future write eligibility through `VortexStagedManifestFileWriteRequest` and `VortexStagedManifestFileWriteReport`.
- It keeps output-data writes, object-store IO, upstream `Vortex` write calls, and fallback execution disabled.
- Actual feature-gated local draft-file write execution is Phase 12A.3b.2b.
- No selectivity guessing.
- No scan, decode, or materialization is introduced.


## Encoded predicate and projection primitive
- Projection/filter primitives now route to encoded-read candidate planning.
- Metadata proof is attempted first for filters.
- Inconclusive predicates return `NeedsEncodedPredicate`/`NeedsEncodedRead`.
- No scan/decode/materialization is introduced.

## Incumbent gap alignment

- Vortex adapter work should support work avoidance, decision traces, layout health, and feature-footprint transparency.
- Do not turn Vortex integration into Arrow-default or DataFusion/Spark fallback execution.
- Encoded predicate/projection phases should start emitting work-avoided and decision-trace data when practical.


## Query primitive decision trace and work avoided

- Query primitives now produce decision traces and work-avoided metrics.
- Metadata answers explain why they were safe.
- Deferred encoded-read paths explain what is missing.
- Metrics never guess unknown bytes or selectivity.


## Local execution loop skeleton

- The local execution loop consumes query primitive requests plus optional metadata summaries.
- It can complete metadata-only results from available summary information.
- It defers encoded-read-required and encoded-predicate-required outcomes.
- It attaches `DecisionTrace` and `WorkAvoidedReport` through primitive analysis integration.
- It does not scan, decode, materialize, write, perform object-store IO, perform spill IO, or enable fallback execution.


## Memory-safe bounded local execution

The bounded local loop consumes metadata-only local execution results plus resource policy, completes safe metadata/no-op work, defers encoded-read-required work, preserves memory and max-parallelism policy, and does not scan/decode/materialize/write/object-store/spill/fallback.


## Local engine CLI/API surface
- `vortex-run` wraps query primitives, local execution, bounded execution, `DecisionTrace`, and `WorkAvoidedReport`.
- It is currently metadata/no-op/deferred only.
- It does not execute encoded reads, scan rows, decode, materialize, write, object-store IO, spill IO, or fallback execution.

## Local engine diagnostic propagation

`vortex-run` now preserves metadata-open diagnostics from metadata-open reports instead of collapsing them into generic missing-metadata outcomes.

Missing local files, unsupported object-store URIs, invalid targets, feature-disabled IO states, and API-deferred states remain visible to users and agents.

The local engine must not collapse metadata-open diagnostics into generic missing metadata when a more specific root cause is available.


## Spill lifecycle transition

Spill lifecycle is `ShardLoom`-native and not `Vortex`-specific.
No spill data movement is implemented in this phase.
`Vortex` execution paths may later request spill lifecycle support through memory/scheduler reports.

## Phase 12 write-readiness boundary

- Phase 12A should start with native `Vortex` write intent and staged-output planning.
- Phase 12A must not perform broad write execution yet.
- Phase 12A must not perform object-store writes.
- Phase 12A must preserve recovery/commit diagnostics continuity from Phase 11 surfaces.
- Phase 12A must keep fallback execution disabled.


## Native Vortex write intent core

- Write intent is report-only in Phase 12A.1.
- It does not call upstream `Vortex` write APIs.
- It does not write files.
- It blocks unknown schema/delete/tombstone semantics.
- It blocks object-store writes.
- It requires a future commit protocol before execution.

## Phase 12A.1a write-intent blocker stabilization

- `VortexWriteIntentReport` must keep missing commit protocol machine-readable as `BlockedByCommitProtocol`.
- `StagedOutputRequired` remains reserved for explicit staged-output planning intent.
- This phase remains plan-only and side-effect-free: no output data writes, manifest writes, object-store IO, upstream Vortex write calls, or fallback execution.


## Staged output workspace contract

- Staged output workspace is report-only in Phase 12A.2a.
- `VortexStagedWorkspaceSetupReport` and setup helper behavior are now defined for Phase 12A.2b.1b.
- Default builds remain feature-disabled and side-effect-free.
- Under `vortex-staged-output-fs`, setup may only create/confirm the exact local workspace directory when explicitly requested.
- Local `file://` staged workspace paths are normalized to local filesystem paths before checks and setup.
- Existing valid workspace directories are reported as ready/confirmed planning state rather than newly created.
- `WorkspaceCreated` effects are recorded only when setup actually creates the directory.
- Under `vortex-staged-output-fs`, staged marker writes are feature-gated and local-path-only.
- Marker writes are limited to one tiny deterministic marker file inside the known workspace.
- It does not write output data.
- It does not write manifests.
- It does not call upstream `Vortex` write APIs.
- It does not perform object-store IO.
- It blocks object-store staged workspaces.
- It requires future commit protocol before finalization.


## Staged workspace setup CLI

- `vortex-staged-workspace-setup` exposes the staged workspace setup helper through a stable `ShardLoom` command surface.
- Default builds remain feature-disabled/report-only.
- Under `vortex-staged-output-fs`, the CLI can create/confirm only an explicitly requested local staged workspace path.
- The command does not write staged marker files.
- The command does not write output data.
- The command does not write manifests.
- The command does not call upstream `Vortex` write APIs.
- The command does not perform object-store IO.


## Staged marker CLI

- `vortex-staged-marker-write` exposes the staged marker helper through a stable `ShardLoom` command surface.
- Default build remains feature-disabled/report-only.
- Under `vortex-staged-output-fs`, the CLI may write only a tiny deterministic marker file in an existing local staged workspace.
- It does not create workspaces.
- It does not write output data.
- It does not write manifests.
- It does not call upstream `Vortex` write APIs.
- It does not perform object-store IO.
## Phase 12A.3a update
- Phase 12A.2c.2 complete.
- Phase 12A.3a current: staged manifest draft core contract (report-only, no filesystem).
- Phase 12A.3b planned: feature-gated local staged manifest draft file.
- Phase 12A.3c planned: CLI/docs integration.
- Actual output payload and file writes remain deferred.

## Staged manifest draft contract

- `ShardLoom` manifest draft is report-only in Phase 12A.3a.
- It requires `VortexWriteIntentReport`, `VortexStagedOutputReport`, workspace, marker, schema/delete/tombstone, and commit protocol signals.
- It does not write manifest files.
- It does not write output data.
- It does not call upstream `Vortex` write APIs.
- It blocks object-store targets.


## Staged manifest file contract

- The manifest draft file contract is report-only.
- It defines deterministic path/content but writes no files.
- It depends on draft/workspace/marker readiness.
- It does not write manifests.
- It does not write output data.
- It does not call upstream `Vortex` write APIs.
- It does not perform object-store IO.

## Staged manifest draft write boundary

- `ShardLoom` now includes feature-gated staged manifest draft file writing in `shardloom-vortex` behind `vortex-staged-output-fs`.
- The helper writes only the exact local staged draft artifact path.
- The draft artifact is not a committed manifest.
- The flow does not write output data.
- The flow does not call upstream `Vortex` write APIs.
- Commit protocol execution remains future work.


## Staged manifest CLI
- `vortex-staged-manifest-file-plan` exposes the staged manifest file plan helper.
- `vortex-staged-manifest-file-write` exposes the feature-gated staged manifest draft-file write helper.
- Default build is report-only/feature-disabled for writes.
- Feature build writes only the local draft artifact.
- It is not a committed manifest, does not write output data, does not call upstream `Vortex` write APIs, and does not perform object-store IO.
- Commit protocol remains future work in Phase 12B.

## Phase 12A staged output closeout

- Staged marker and staged manifest draft file are local draft artifacts only.
- They are not committed manifests.
- Commit protocol and manifest finalization are Phase 12B work.
- No output data write path exists yet.
- No upstream Vortex write API path exists yet.
- No object-store write path exists yet.


## Commit intent core contract

- `ShardLoom` commit intent is report-only in this phase.
- `VortexCommitIntentRequest`/`VortexCommitIntentReport` require staged manifest draft write readiness, manifest finalization availability, commit protocol availability, schema/delete/tombstone readiness, and recovery/retry/cancellation gate readiness.
- This phase does not commit manifests.
- This phase does not finalize manifests.
- This phase does not write output data.
- This phase does not call upstream `Vortex` write APIs.
- This phase does not perform object-store IO.
- This phase does not enable fallback execution.

## Commit intent readiness integration

- `ShardLoom` commit intent can derive readiness from `VortexStagedManifestFileWriteReport`, `ShardLoomRecoveryIntegrationReport`, `ShardLoomRetryExecutionGateReport`, and `ShardLoomCancellationExecutionGateReport`.
- Missing or blocked recovery/gate reports remain explicit commit blockers.
- Commit intent remains report-only.
- No manifest finalization is executed.
- No output data is written.
- No upstream `Vortex` write APIs are called.
- No object-store IO is performed.
- Fallback execution remains disabled.

## Commit readiness integration closeout

- Commit intent can derive readiness from staged manifest draft-file write and recovery/retry/cancellation gate reports.
- Missing or blocked recovery/gate reports keep commit blocked.
- Commit remains report-only.
- Manifest finalization and commit protocol remain future work.
- No output writes, upstream `Vortex` write APIs, object-store IO, or fallback execution are enabled.


## Commit protocol state machine contract

- `ShardLoom` commit protocol state machine is report-only.
- It models states and transitions but does not execute them.
- It does not finalize manifests.
- It does not write commit markers.
- It does not commit manifests.
- It does not write output data.
- It does not call upstream `Vortex` write APIs.
- It does not perform object-store operations.

## Commit protocol request derivation from commit intent

`VortexCommitProtocolRequest` values can now be derived directly from `VortexCommitIntentReport` values through a report-only helper path.

- Derived requests preserve commit-intent readiness/blockers.
- Derived requests preserve recovery readiness/blockers.
- Derived requests preserve staged draft-manifest readiness.
- Derived requests preserve manifest finalization readiness.
- Derived requests preserve object-store target blockers.
- Commit marker readiness is not guessed.
- Commit protocol remains report-only.
- No manifest finalization, commit marker writes, manifest commits, output writes, upstream `Vortex` write API calls, object-store IO, or fallback execution are introduced.

## Commit intent/protocol CLI wrappers

- `shardloom vortex-commit-intent-plan <target_uri> <signals>` is report-only.
- `shardloom vortex-commit-protocol-plan <target_uri> <current_state> <transition> <signals>` is report-only.
- Both commands expose deterministic text/JSON planning envelopes for staged write-readiness validation.
- Neither command executes commit transitions.
- Neither command finalizes manifests.
- Neither command writes commit markers, manifests, or output payload data.
- Neither command performs object-store IO.
- Neither command calls upstream `Vortex` write APIs.
- Fallback execution remains disabled.


## Local staged write-readiness smoke test

- The smoke test runs the staged write-readiness CLI chain through ShardLoom-native planning and staged local artifact helpers.
- It verifies local staged artifacts only (workspace, marker, and staged manifest draft file).
- It verifies no output payload writes, no committed manifest writes, no commit marker writes, no upstream Vortex write API calls, no object-store IO, and no fallback execution.


## Commit marker core contract

- Commit marker planning is report-only in this phase.
- It is distinct from manifest finalization and committed manifest state.
- It does not write a marker file.
- It does not finalize manifests.
- It does not commit manifests.
- It does not write output data.
- It does not call upstream `Vortex` write APIs.
- It does not perform object-store IO.



## Feature-gated local commit marker

- Commit marker writing is feature-gated by `vortex-staged-output-fs`.
- It writes only the exact local commit marker artifact represented by the marker file reference.
- It requires the marker planning feature-gate readiness signal; readiness is not guessed.
- It does not finalize manifests.
- It does not commit manifests.
- It does not write output data.
- It does not call upstream `Vortex` write APIs.
- It does not perform object-store IO.
- Recovery and idempotency semantics remain future work.

## Competitive execution path (Competitive Engine Track: CG-1..CG-11)

- Staged write-readiness is only the control-plane foundation.
- The next competitive milestones require actual encoded reads, actual query primitives, output payload writes, commit execution, correctness, and benchmarks.
- Gate mapping: CG-1 encoded reads, CG-2 query primitives, CG-3 output payload writes, CG-4 commit execution, CG-5 correctness, CG-6 benchmarks, CG-7 kernels, CG-8 streaming/parallel/adaptive execution, CG-9 lakehouse intelligence, CG-10 object-store/distributed execution, CG-11 Python/API surface later.
- Upstream Vortex write/read APIs remain feature-gated and isolated.
- Arrow conversion remains explicit, not default.
- No fallback engines.
