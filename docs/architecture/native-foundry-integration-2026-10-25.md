<!-- SPDX-License-Identifier: Apache-2.0 -->

# Installed native ShardLoom in Foundry

Status: accepted October 25 maintainer addition, queued after
[everyday analytical completion](phased-execution-plan.md#everyday-analytical-completion)
and before [AI data workflows](native-ai-data-workflows-2026-10-24.md).
The [active queue](phased-execution-plan.md) places consolidated dependency
maintenance immediately after ingestion, ahead of connected programming, I/O
and rich-data work. This integration consumes those shared capabilities.

Attach these three working deliverables to [RFC 0036](../rfcs/0036-foundry-integration-pack-availability-surface.md),
CG-18/CG-20/CG-21/CG-23 and RFC 0024 packaging. Foundry remains an optional
integration package for the standalone engine; its implementation is explicitly
requested in this queue. The current implementation program remains RFC 0044.
No new phase or competitive-gate identifier is introduced, and CG-1 through
CG-23 retain their independent acceptance requirements.

This intake is source and documentation review. No ShardLoom package has been
installed or executed in the maintainer's Foundry enrollment by this review.
Actual environment compatibility, permissions, transactions and performance
remain unproved. The supplied handoff reviewed `b9b842ef`; naming #1604 has
since merged at `3239ade0`. Published v0.7.0 and later source/builds have separate
identities. Existing Mac Full43 observations do not predict Foundry performance.

## Host and execution contract

Execution host: Foundry. Compute engine: ShardLoom.

Use declared Foundry input handles, an optional thin adapter, the installed
native Python package and ordinary `import shardloom` / `sl.context(...)`, then
declared Foundry output transactions. Foundry schedules the job, grants access,
supplies the input snapshot and owns platform publication. ShardLoom owns query
planning/execution, native memory, workers, spill, cancellation and result
lifetimes. No Spark, DataFusion, DuckDB, Polars or other engine executes
unsupported ShardLoom work. Foundry hosting is compatible with that rule.

The existing [starter](../../examples/foundry-lightweight-transform/README.md)
builds a local CLI, imports from a checkout and writes simulated local outputs.
Its `foundry_compute_invoked=false` and other false invocation fields describe
that local proof. Preserve its historical evidence while separating real host
invocation from external query-engine execution in the new adapter's reports.
The [readiness ladder](../foundry/integration-pack-readiness.md) does not become
live-platform certification through a documentation change.

Palantir documents single-node `@transform.using(...)`, `.with_resources(...)`
and explicit input/output declarations. Pin and verify the actual transform
library APIs in the target enrollment before implementing the adapter.
[Transform documentation](https://www.palantir.com/docs/foundry/transforms-python/transforms).

## Three working deliverables

| Deliverable | Required working result |
| --- | --- |
| Native package and real Foundry proof | Install a pinned native package in the actual build and execution environments; read a declared fixture, compute with ShardLoom, commit output and independently read it back. |
| Scalable dataset adapter | Read multiple physical files as one logical relation; deliver native batch/file output with schema, explicit resources, spill, cancellation and failure-safe publication, without collecting the whole result. |
| Incremental and managed-table integration | Preserve explicit read/write modes, previous state, retries and upstream replacement; connect Foundry-managed Iceberg through the same engine interface and prove the admitted snapshot/append workflows. |

### Native package and environment proof

The current [Conda recipes](../../packaging/conda/README.md) still identify
version 0.1.0 and local source paths. `shardloom-python` is a `noarch: python`
CLI-wrapper recipe whose test imports the client. Replace that delivery with
either a platform-specific native package or a facade with a version-matched
native dependency. Preserve one ordinary user import; test loading and executing
the installed extension and any bundled CLI outside the source checkout.

The [published Linux artifact](../release/v0.7.0-publication-verification.md)
is `cp310-abi3-manylinux_2_39_x86_64`. Inspect actual CPU architecture, Python,
libc, native dependencies and installed PyArrow in both Foundry environments.
Build and audit a compatible native artifact if required; neither Conda nor a
wheel-tag change removes a libc requirement. Test Arrow interchange directly;
Rust Arrow 59.3.1 does not prescribe the same Python PyArrow version number.

Use an approved private artifact to establish integration without requiring a
public conda-forge feedstock. Foundry supports backing repositories and pinned
library versions. Package publication and deployment retain their existing
authorization and provenance requirements; this queue entry publishes nothing.
[Library workflow](https://www.palantir.com/docs/foundry/transforms-python/use-python-libraries),
[Artifact repositories](https://www.palantir.com/docs/foundry/code-repositories/artifact-repositories-overview).

BYOC is an alternative when native environment requirements justify it, with
its own installed-package and execution proof. Current documentation says
Preview is unavailable for BYOC; recheck that limitation in the target version.
[BYOC documentation](https://www.palantir.com/docs/foundry/transforms-python/bring-container).

### Dataset and output adapter

Start with a small declared Arrow fixture using `LightweightInput.arrow()`,
`context.from_arrow_table(...)`, native computation and one complete output
write. `.arrow()` materializes the full dataset view, so this proves the
installation and transaction bridge only. For larger data, connect
transaction-scoped local staging first, then admitted batch/file or range access.
Foundry exposes `.path()`, `.filesystem()`, dataset/branch identity and input
transaction bounds. [Input API](https://www.palantir.com/docs/foundry/api-reference/transforms-python-library/api-lightweightinput).

Treat all physical input files as one logical relation: per-file aggregation
followed by concatenation is not a global aggregation. Bind prepared data and
reusable source work to dataset, branch, transaction/read mode, schema and
preparation settings. Avoid repeated downloads within a build while preserving
generation and authorization checks. Verify seek/range behavior, retained
owners, cancellation and cleanup before presenting a Python file object as a
native random-access source. Reuse the ingestion and universal-I/O contracts.

For ordinary tabular outputs, stream native Parquet parts or deliver bounded
Arrow results through the declared output. Execute once, preserve complete
schema and finalize the transaction deliberately. The documented
`path_for_write_table`, file-upload/schema-finalization APIs and
`path_for_object_store_write_table` are integration seams. The latter is a
transaction-mapped local S3 proxy, not a private physical cloud bucket.
Repeated `write_table(batch)` calls are not an established append contract.
[Output API](https://www.palantir.com/docs/foundry/api-reference/transforms-python-library/api-lightweightoutput).

Propagate read, compute, schema, write and finalization failures as failed
transforms. Prove that a deliberately failed write cannot report success or
replace a valid output with an incomplete/empty snapshot. Vortex output may be
governed custom data for a ShardLoom-aware downstream reader; storing a Vortex
file does not establish general Foundry tabular support.

### Managed Iceberg and incrementality

Palantir documents `TableInput`/`TableOutput`, `IcebergInput`/`IcebergOutput`,
`.arrow()` and `.write_table(...)` for single-node transforms. Foundry resolves
Iceberg reads/writes around the chosen compute library. Use that bridge to
prove managed-Iceberg integration without making a standalone Iceberg scanner
a prerequisite. This is a proposed ShardLoom integration with documented
platform interfaces, not a tested deployment.
[Iceberg transform API](https://www.palantir.com/docs/foundry/iceberg/python-transform-basics).

Distinguish Foundry datasets, Foundry-managed Iceberg, external virtual tables
and source-system pushdown. Verify each actual resource's authorized adapter;
managed-Iceberg support does not establish support for every Snowflake-backed
handle. External pushdown remains explicit external computation. A future
direct Iceberg client must honor snapshot and delete semantics rather than
glob Parquet files in storage.

Implement complete snapshot execution first, append-style workflows next, then
stateful updates/deletes and retries under separately proved capabilities.
Preserve `added`, `current` and `previous` read modes; an incremental aggregate
cannot replace its complete result using only newly added rows. Finite batch
streaming does not establish durable incrementality. Current Palantir
documentation lists single-node snapshot and append-only support, with
changelog incremental support absent; do not promise that unavailable boundary.
[Iceberg capability matrix](https://www.palantir.com/docs/foundry/iceberg/data-pipelines),
[Incremental Iceberg transforms](https://www.palantir.com/docs/foundry/iceberg/incremental-transforms).

## Resource, fidelity and governance acceptance

- Map the job's declared allocation into ShardLoom memory and parallelism. Leave
  measured room for Python, input/output exchange and unaccounted codec owners.
  Do not derive the worker grant from physical-host capacity or claim engine
  credits are a whole-container memory ceiling. Keep spill in job-owned scratch
  with permission, quota, cancellation and cleanup; no arbitrary row ceiling.
- Test large integers, decimals, timestamps/timezones, binary, lists/structs,
  nulls, empty datasets and field order against real platform schemas. Fix
  unsupported combinations in shared type/adapter machinery; never stringify
  values silently. Keep media references/payload access separate from OCR or
  model execution.
- Preserve declared input/output authorization. Test denied access, branch
  isolation, source replacement, failed publication and retained-data isolation.
  Do not substitute hidden broad-credential reads. Redact credentials and
  respect markings, evidence visibility and export policies.
- Accept a realistic join/aggregation spanning multiple files, complete
  independent output readback and a deliberately failed publication. Record
  installed package identity, native execution/no-fallback evidence, snapshots,
  actual resource limits, output mode and cleanup. A tiny filter is the first
  deployment check, not final scalable-adapter acceptance.
- Freeze Foundry snapshots, resources, result semantics and output requirements
  for performance comparisons. Report routine engine time only; retain I/O,
  conversion and full-job clocks in diagnostics. The Mac Full43 remains a
  separate regression observation, not a Foundry performance prediction.

## Later boundaries and completion rule

Review Palantir's `foundry-platform-python` for external platform orchestration
when needed. Ordinary transforms should use their supplied authorized handles.
External production clients need scoped application authentication; do not
introduce service credentials into the first transform just to use an SDK.

Compute Modules and `python-compute-module` are later request-serving work.
Verify their current API maturity and payload protocol before adoption; JSON
item streaming is not Arrow batch transport, replica state is not durable
query state, and several replicas do not distribute one ShardLoom query.
Foundry's S3-compatible API is a later storage boundary and must preserve the
platform's branching/transaction contract. These options do not block the
three batch deliverables or authorize extra engines.

Replace the simulated starter with installed-package, transaction-backed
examples as these deliverables land, keeping local fixtures clearly labeled.
Update existing packaging and Foundry readiness records with actual evidence.
If enrollment access or a required platform capability is unavailable, retain
the concrete artifact and exact remaining live-proof gap; do not relabel local
simulation as successful Foundry integration.
