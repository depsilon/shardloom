# Vortex-First Provider Check Skill

## Purpose

Use this skill whenever a proposed ShardLoom change touches execution, source/sink I/O, Vortex
files, encoded data, physical layout, statistics, pruning, predicate pushdown, projection pushdown,
reader-backed batches, device/GPU paths, extension types, vector or media-like data, object-store
reads, write layout, compaction, or benchmark coverage.

The goal is to make Codex check Vortex first before inventing a parallel ShardLoom abstraction.

ShardLoom should not duplicate Vortex capabilities when an upstream Vortex concept can be admitted
as a native provider, wrapped through ShardLoom policy, and reported through ShardLoom evidence.

ShardLoom must still preserve its own no-fallback contract, certification model, diagnostics,
workload gates, and user-facing surfaces.

## Core Rule

Before implementing a new ShardLoom concept in a Vortex-adjacent area, answer:

1. Does upstream Vortex already have a concept, API, layout, execution path, scan primitive,
   source/sink boundary, extension type, statistics model, or I/O behavior for this?
2. Can ShardLoom use that Vortex concept as a native provider?
3. Does ShardLoom need a wrapper/certificate/report around it instead of a new implementation?
4. Does the Vortex concept rely on a query-engine integration that must remain baseline/reference
   only?
5. What evidence is required before ShardLoom may claim support?

Do not proceed with a new ShardLoom-native design until the Vortex-first check is written down in
the implementation note, PR summary, or report surface.

## Required Source Checks

When this skill is invoked, inspect the relevant current repo docs before changing code:

- `docs/skills/vortex/vortex-concepts.md`
- `docs/architecture/vortex-public-api-inventory.md`
- `docs/architecture/vortex-upstream-alignment-hardening.md`
- `docs/architecture/phased-execution-plan.md`
- `docs/architecture/rfc-phase-traceability.md`
- `docs/architecture/operational-evidence-policy-hardening.md`
- `docs/rfcs/0031-universal-native-io-envelope.md`
- `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
- `docs/rfcs/0033-user-data-workflow-etl-surface.md`
- `docs/rfcs/0034-three-engine-certified-data-execution-fabric.md`
- `docs/rfcs/0035-rest-event-remote-api-surface.md`
- `docs/rfcs/0036-foundry-integration-pack-availability-surface.md`
- `shardloom-vortex/src/lib.rs`
- `shardloom-vortex/src/source_backed_encoded_execution.rs`
- `shardloom-vortex/src/vortex_compatibility.rs`
- `shardloom-vortex/src/vortex_scan_compatibility.rs`
- `shardloom-vortex/src/vortex_compute_provider.rs`
- `shardloom-vortex/src/vortex_operational_facets.rs`

Also inspect upstream Vortex docs or source when the local repo inventory is insufficient or stale.

## Vortex Concepts To Check First

Check whether the proposed behavior maps to one of these upstream Vortex areas.

### Logical And Physical Data Model

- DType
- Array
- Encoding
- Layout
- Statistics
- Validity/null representation
- Extension DType
- Scalar representation
- Canonical versus non-canonical representation

ShardLoom should not treat a logical type as if it has one physical representation.

### Encoded Execution

- compressed-array kernels
- constant-array behavior
- dictionary behavior
- run-end or run-length behavior
- sparse validity
- selection vectors
- encoded predicate evaluation
- encoded projection
- parent/child array execution
- canonicalization boundaries
- deferred execution

Do not decode or materialize just because that is easier.

### Scan And Source/Sink Behavior

- Scan request
- Source
- Sink
- Split
- split estimates
- split serialization
- projection pushdown
- predicate pushdown
- limit pushdown
- residual expression handling
- field masks for filter-only columns versus output columns
- composite pushdown combinations

ShardLoom should align Native I/O and task/split execution with Vortex Scan concepts where
possible.

### Layout And Persistence

- file layout
- chunked layout
- dictionary layout
- zoned layout
- zone-map/statistics pruning
- layout writer strategy
- write/flush behavior
- micro-segment or segment layout concepts
- compaction/rewrite implications

ShardLoom should prefer a layout-advisor/certificate around Vortex layout choices before inventing
a separate layout model.

### I/O And Object-Store Behavior

- positional reads
- range reads
- request coalescing
- prefetch
- backend concurrency
- object-store read amplification
- useful bytes versus requested bytes
- cache behavior
- segment source behavior

ShardLoom should report these as Native I/O evidence, not hide them in timing numbers.

### Device And GPU Behavior

- device buffers
- CUDA/device session
- host-to-device transfer
- device-to-host transfer
- direct storage candidate
- device-resident output boundary

Do not claim GPU/device support unless device residency evidence exists.

### Extension And Richer Data Types

- vector
- tensor/matrix
- map
- variant/JSON
- UUID
- geospatial
- raster/media reference
- embedding reference
- document/media reference

Recognizing or preserving an extension type is not the same as executing over it.

### Vortex Integrations

- Vortex + DataFusion
- Vortex + DuckDB
- Vortex + Spark
- Vortex + Trino
- Vortex + Ray
- PyVortex / `vortex-data`

These may be baselines, references, or oracles only. They must not execute unsupported ShardLoom
runtime work as fallback.

## Classification Decision

Every Vortex-adjacent proposal must be classified into exactly one of these categories.

### `use_vortex_native_provider`

Use an upstream Vortex API as a ShardLoom-native provider.

Required evidence:

- provider kind
- provider crate/version
- provider API surface
- feature gate
- ShardLoom admission policy
- execution certificate
- Native I/O certificate
- representation transitions
- materialization/decode boundary status
- `fallback_attempted=false`

### `wrap_vortex_concept`

Do not execute yet; expose a ShardLoom report/certificate around a Vortex concept.

Examples:

- VortexCompatibilityMatrix
- VortexScanCompatibilityReport
- VortexLayoutAdvisorReport
- VortexComputeProviderReport
- DeviceResidencyReport
- IoBackendEvidence
- ExtensionTypeCapabilityMatrix

### `implement_shardloom_kernel`

Implement ShardLoom-native logic because Vortex does not provide the needed behavior, or because
the Vortex behavior is not appropriate for ShardLoom's policy/certificate requirements.

Required evidence:

- why Vortex is insufficient
- kernel provider kind
- semantic profile
- correctness fixtures
- benchmark rows
- execution certificate
- Native I/O certificate if source/sink data is involved
- no-fallback evidence

### `baseline_or_oracle_only`

Use an external Vortex integration or external engine only for comparison, reference output, or
migration analysis.

Allowed examples:

- Vortex + DataFusion benchmark row
- Vortex + DuckDB benchmark row
- Spark/Databricks/Snowflake/Fabric-style design reference
- external oracle correctness artifact

Not allowed:

- executing unsupported ShardLoom runtime work
- residual evaluation fallback
- reporting external engine output as ShardLoom execution

### `blocked_until_vortex_or_shardloom_evidence`

The idea is valid, but neither Vortex nor ShardLoom has enough certified evidence yet.

Required output:

- deterministic unsupported diagnostic
- required future evidence
- required gate
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Provider Boundary Rules

A Vortex-native provider is allowed only when all are true:

- It is invoked through an approved ShardLoom boundary.
- It is feature-gated where appropriate.
- Vortex version and API surface are recorded.
- ShardLoom admission policy is recorded.
- Materialization/decode/Arrow/object-store/write/spill/device boundaries are explicit.
- Residual expressions are either executed by ShardLoom-native code or blocked.
- External query-engine integrations are not invoked as fallback.
- `fallback_attempted=false`.

A Vortex-native provider is not allowed when:

- It silently converts to decoded Arrow without a materialization boundary.
- It delegates residual work to DataFusion, DuckDB, Spark, Polars, Velox, Trino, Dask, Ray, or
  another query engine.
- It relies on a Vortex query-engine integration to finish unsupported work.
- It cannot report representation transitions.
- It cannot report provider kind/API surface.
- It cannot produce or reference required certificates.

## Residual Expression Rule

When a Vortex provider accepts only part of an operation:

- Accepted operations may be certified as Vortex-native provider execution.
- Residual operations must be executed by ShardLoom-native code or deterministically blocked.
- Residual operations must not be delegated to an external engine.

Every residual boundary must report:

- accepted operations
- rejected operations
- residual expression
- residual executor
- residual required
- `external_engine_invoked=false`
- `fallback_attempted=false`

Valid residual executor values:

- `none`
- `shardloom_native`
- `unsupported_blocked`
- `external_baseline_only`
- `prohibited_external_fallback`

## Evidence Requirements By Area

### Execution Path

Required:

- `ExecutionProviderKind`
- provider API surface
- execution certificate
- Native I/O certificate when source/sink boundaries are involved
- correctness evidence
- benchmark evidence before performance claims
- no-fallback evidence

### Scan/Source/Sink Path

Required:

- source capability report
- pushdown report
- split refs
- sink requirement report
- adapter fidelity report
- materialization boundary report
- per-path Native I/O certificate

### Layout/Write Path

Required:

- layout strategy
- encoding strategy
- chunk/segment policy
- statistics policy
- expected read/write tradeoff
- commit/recovery policy if effectful
- fidelity/materialization report
- write/commit certificate before support claims

### Device/GPU Path

Required:

- device kind
- residency status
- host/device transfer bytes
- kernel/provider surface
- output boundary
- CPU fallback status
- no external fallback evidence

### Extension Type Path

Required:

- dtype recognition status
- metadata preservation status
- scan status
- expression support status
- write support status
- certified execution status
- unsupported diagnostics for unimplemented operations

## Do-Not-Invent Checklist

Before creating a new type/module/report, answer:

- Is this already represented by Vortex DType, Array, Encoding, Layout, Statistics, Scan, Source,
  Sink, Split, Session, Registry, or Extension Type?
- Is this already represented in ShardLoom as Native I/O, execution provider, evidence artifact,
  semantic profile, source/sink adapter, or workload constitution?
- Would this new abstraction duplicate a Vortex concept?
- Would a wrapper/report around the Vortex concept be better?
- Would using the Vortex concept violate no-fallback policy?
- What evidence would make the support claim valid?

If these answers are not recorded, stop and add a design note before implementing.

## Common Red Flags

- Creating a ShardLoom layout abstraction without checking Vortex layouts.
- Creating a source/sink abstraction without checking Vortex Scan `Source`, `Sink`, and `Split`.
- Creating an encoded kernel without checking Vortex array/encoding behavior.
- Decoding to Arrow because a Vortex physical representation is unfamiliar.
- Treating Vortex integration output from DataFusion/DuckDB/Spark as ShardLoom-native execution.
- Reporting a benchmark win without recording whether ShardLoom used native Vortex, compatibility
  import, materialization, or decoded execution.
- Adding an external engine to make an unsupported path pass.
- Reporting planned Vortex support as implemented support.
- Treating extension dtype recognition as expression execution support.
- Treating device-buffer awareness as GPU execution support.

## Required Implementation Note

Every PR that invokes this skill should include a short note:

```text
Vortex-first provider check:
- Subject area:
- Upstream Vortex concept checked:
- Decision:
  - use_vortex_native_provider | wrap_vortex_concept | implement_shardloom_kernel |
    baseline_or_oracle_only | blocked_until_vortex_or_shardloom_evidence
- Vortex API/provider surface:
- ShardLoom provider/report/certificate surface:
- Residual handling:
- Materialization/decode boundary:
- Evidence added:
- Gates still blocked:
- fallback_attempted=false:
```

## Example Decisions

### Predicate Pushdown

Good decision:

```text
Use Vortex Scan pushdown concepts.
Wrap accepted/rejected predicate operations in ShardLoom Native I/O pushdown evidence.
Execute supported predicate portions through a Vortex-native provider if policy-admitted.
Block unsupported residuals unless a ShardLoom-native residual executor exists.
```

Bad decision:

```text
Read Vortex into Arrow, run DataFusion predicate evaluation, and report ShardLoom success.
```

### Layout Optimization

Good decision:

```text
Add VortexLayoutAdvisorReport that records chunking, zoned statistics, dictionary strategy,
expected pruning benefit, write/read tradeoff, and required future evidence.
```

Bad decision:

```text
Invent a ShardLoom file-layout model without checking Vortex layouts or write strategies.
```

### Vector Data

Good decision:

```text
Add ExtensionTypeCapabilityMatrix row for vector dtype recognition, metadata preservation, scan
status, expression support status, write support status, and certified execution status.
Keep vector search unsupported until execution evidence exists.
```

Bad decision:

```text
Treat embeddings as generic arrays and claim vector search support.
```

## Codex Prompt Fragment

Use this fragment for Vortex-adjacent work:

```text
Use the Vortex Concepts skill and Vortex-First Provider Check skill. Before inventing a new
ShardLoom abstraction, check whether upstream Vortex already has an array, encoding, layout, scan,
source/sink, split, execution-step, I/O, extension-type, or device concept that should be used or
wrapped. Classify the decision as use_vortex_native_provider, wrap_vortex_concept,
implement_shardloom_kernel, baseline_or_oracle_only, or blocked_until_vortex_or_shardloom_evidence.
Do not decode to Arrow, invoke external query-engine integrations, or report support without
certificates and no-fallback evidence.
```
