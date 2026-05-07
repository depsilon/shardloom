# Systems Learning Map

## Purpose

This document captures conceptual lessons from mature systems and translates them into ShardLoom-native contracts.
These lessons are pressure tests for ShardLoom-native architecture and diagnostics.
They are not dependency targets, and they are not fallback execution targets.

## Non-goals

- No Trino dependency.
- No Dask dependency.
- No Ray dependency.
- No DuckDB dependency.
- No Calcite dependency.
- No Arrow Acero/Substrait dependency.
- No external engine execution.
- No fallback execution.
- No SQL parser implementation in this phase.
- No distributed execution in this phase.

## Trino lessons

Conceptual lessons:
- Connector capability boundaries should be explicit.
- Pushdown acceptance and residual responsibilities must be diagnosable.
- Split/task lifecycle should be first-class.
- Runtime dynamic filtering lifecycle should be visible.
- System introspection surfaces should be queryable.
- Intermediate exchange/spooling semantics should be explicit.

ShardLoom-native vocabulary:
- `PushdownProof`
- `PushdownGuarantee`
- `ProofBasis`
- `RuntimeFilterLifecycle`
- `SplitSource`
- `TaskLease`
- `IntermediateArtifactRef`
- `system.*` virtual diagnostics surfaces

## Dask lessons

Conceptual lessons:
- Keep graph layering explicit.
- Distinguish high-level graph intent from low-level task graph execution shape.
- Preserve lowering provenance.
- Keep scheduler policy distinct from plan semantics.
- Make task granularity policy explicit and auditable.

ShardLoom-native vocabulary:
- `LoweringTrace`
- `LoweringRuleId`
- `PlanGuarantee`
- `InformationLoss`
- `TaskGranularityPolicy`
- fuse/split/coalesce decision records

## Ray lessons

Conceptual lessons:
- Resource vectors should be explicit inputs to scheduling decisions.
- Placement hints should be visible and overridable by policy.
- Object-like references should preserve lineage.
- Recovery should distinguish retry, reconstruct, and reuse.

ShardLoom-native vocabulary:
- `ResourceVector`
- `PlacementHint`
- `RecoveryStrategy`
- `LineageRef`
- `ReconstructFromLineage`

## DuckDB lessons

Conceptual lessons:
- Vectorized execution ergonomics must remain developer-visible.
- Operator profile outputs should be easy to read and compare.
- Planned versus actual cardinality should be explicit.
- Pipeline breakers should be explicit diagnostics boundaries.

ShardLoom-native vocabulary:
- `OperatorProfile`
- `PlannedVsActualOperatorProfile`
- `PipelineBreakerKind`
- bytes read/avoided
- decode/materialization avoided

## Calcite lessons

Conceptual lessons:
- SQL frontend parsing/binding/validation must be explicit boundaries.
- Relational algebra boundary should be well-defined.
- Adapters are capability surfaces, not hidden execution delegation.
- Planner rules should be diagnosable with stable identifiers.

ShardLoom-native vocabulary:
- SQL frontend boundary
- parse/bind/validate-only phase
- ShardLoom Plan IR owns semantics
- unsupported SQL diagnostics

## Arrow Acero/Substrait lessons

Conceptual lessons:
- Operator graph portability is useful but must preserve native semantics.
- Validation without execution is essential.
- Export/import must report loss boundaries explicitly.

ShardLoom-native vocabulary:
- `PlanPortabilityReport`
- native-only nodes
- representable nodes
- lossy nodes
- unsupported nodes
- portability diagnostics

## Placement in ShardLoom phases

- now/docs-only: systems-learning-map, pushdown proof vocabulary, lowering provenance vocabulary, task granularity vocabulary
- near phase: diagnostics report schemas, capability report extensions, explain/estimate additions
- before real execution: task lifecycle, resource vector, operator profile, runtime filter lifecycle
- before distributed/object-store execution: split source, task lease, placement hints, intermediate artifact refs, recovery strategy
- before SQL UX: SQL frontend RFC, bind/validate/unsupported diagnostics, tiny SQL subset

## Guardrails

- No fallback engines.
- No default Arrow conversion.
- No external execution delegation.
- No new dependencies.
- Vortex remains native first-class input and output.
- ShardLoom owns runtime, optimizer, diagnostics, and policy.


## Spark and DataFusion capability lessons

- Spark and DataFusion are capability baselines, not fallback engines.
- Spark lesson: broad platform capability across SQL, APIs, deployment, monitoring, and streaming/lakehouse workflows.
- DataFusion lesson: extensible local SQL/DataFrame query engine capability with operator/function/adapters and Arrow-oriented ecosystem habits.
- ShardLoom-native translation should be tracked via:
  - SQL coverage matrix
  - operator coverage matrix
  - function coverage matrix
  - adapter certification
  - semantic profiles
  - migration analyzers
  - capability discovery
- No Spark/DataFusion dependency and no execution delegation are permitted.


## R5.3 capability-baseline clarifications
- Spark and DataFusion are capability baselines for comparison and learning only, not runtime fallback engines.
- CG-20 covers capability breadth across SQL, operators, functions, adapters, semantics, migration, and user-facing certification; it is not SQL-only.
- Adapter certification and migration reports are the native ShardLoom translation of capability lessons.
- External engines remain conceptual/baseline-only references.
