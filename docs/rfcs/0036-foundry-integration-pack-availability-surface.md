# RFC 0036: Foundry Integration Pack and Availability Surface

## Purpose

Define the optional Foundry integration and availability surface for ShardLoom.

The key architectural distinction:

```text
ShardLoom core remains Vortex-native and no-fallback.
shardloom-foundry is the Foundry-native packaging, governance, lineage,
health, workflow, virtual-table, and app-integration layer.
```

Foundry integration must make ShardLoom easier to package, run, certify,
monitor, and operationalize inside Foundry without turning Foundry Spark,
Snowflake, Databricks, BigQuery, Snowpark, Databricks Connect, Ibis, DuckDB,
Polars, pandas, or any other platform compute path into ShardLoom execution.

This RFC also records the late-stage general availability posture because
Foundry availability depends on the same release/distribution evidence:
Conda-first installation, PyPI-friendly Python packaging, GitHub release
artifacts, checksums, SBOMs, provenance attestations, external examples, and
proof-of-use outside the repo.

## Status

Accepted as Foundry integration and availability intake material.

This RFC does not add runtime behavior, dependencies, package publication,
release tags, container images, Foundry transforms, Foundry Artifact Repository
publication, Compute Modules, external compute execution, virtual-table native
execution, object-store I/O, adapters, SQL execution, DataFrame runtime, UDF
runtime, benchmark execution, production certification, superiority claims, or
fallback execution.

The Foundry and availability language in this RFC is a contract north star, not
a product claim. Public and Foundry claims remain blocked until package,
import, binary-resolution, execution-certificate, Native I/O, correctness,
benchmark, workload-scope, governance, provenance, and no-fallback evidence
exists for the declared workload and environment.

## Scope ownership

Foundry integration is cross-cutting. It belongs under CG-18, CG-20, CG-21,
CG-23, and release engineering. It is not a new core engine competitive gate
and it does not change the CG-1 through CG-23 execution identity.

| Area | Primary owner | Foundry relationship |
| --- | --- | --- |
| Public package/release readiness | CG-11 / CG-18 / RFC 0024 | Foundry consumes the same package and provenance evidence |
| Python/import and CLI resolution | CG-20 / CG-21 | Foundry transforms must import ShardLoom and resolve the CLI deterministically |
| User workflow and ETL | CG-21 | Foundry datasets, virtual tables, and outputs become workflow handles |
| Native I/O and adapter evidence | CG-19 | Foundry staging, S3-like dataset access, and sinks require Native I/O/fidelity evidence |
| Execution certificates | CG-16 | Certificates must identify Foundry transaction, branch, build, and staging context |
| REST/event/API surface | CG-23 | Compute Modules and AIP/app surfaces can expose the same control/proof model later |
| Security and governance | RFC 0019 / CG-21Q / CG-23G | Markings, redaction, credentials, agent visibility, and export policy are explicit |
| Benchmarks and baselines | CG-6 / CG-21M | Snowflake/Databricks/BigQuery/Spark/Polars/DuckDB rows are baselines only |

## Core principles

1. Foundry integration is optional.
2. Foundry does not become ShardLoom's primary engine target.
3. Foundry virtual tables are governed external table handles, not automatic
   ShardLoom-native data.
4. Snowflake, Databricks, BigQuery, Foundry Spark, Snowpark, Databricks Connect,
   Ibis, DuckDB, Polars, and pandas compute are external boundaries, baselines,
   or migration/oracle references only.
5. ShardLoom-native execution requires staged/native data plus ShardLoom
   execution certificates and Native I/O/materialization evidence.
6. Every Foundry staging, export, virtual table, data-connection, media,
   Ontology, AIP, model, or external-compute path must preserve
   `fallback_attempted=false` and `external_engine_invoked` classification.
7. Foundry governance, lineage, health, schedules, Artifact Repositories,
   Marketplace, and Compute Modules should be used as platform surfaces around
   ShardLoom evidence, not as hidden execution shortcuts.

## General availability posture

ShardLoom's public availability path should be:

```text
Conda-first
PyPI-friendly
GitHub-release-backed
provenance-attested
Foundry-consumable
```

Recommended public package identities:

| Channel | Artifact | Purpose |
| --- | --- | --- |
| PyPI | `shardloom` | Python wrapper/client |
| conda-forge | `shardloom-cli` | Platform-specific Rust CLI binary |
| conda-forge | `shardloom-python` | Noarch Python wrapper/client |
| conda-forge | `shardloom` | Metapackage depending on CLI and Python packages |
| GitHub Releases | binaries, wheels, sdists, checksums | Direct release artifacts |
| GHCR / OCI | `shardloom` image | BYOC and future server/API experiments |
| crates.io | selected protocol/client crates only | Deliberate Rust embedding surfaces |

General availability release evidence should include:

```text
git tag
GitHub Release
source archive
platform binaries
Python wheel and sdist
Conda recipe/feedstock status
checksums
SBOM
artifact attestation
changelog
compatibility matrix
known unsupported paths
no-fallback release check
```

Package publication requires explicit human approval. PyPI publication should
prefer trusted publishing/OIDC over long-lived tokens. Crates.io publication
must be deliberate because versions cannot be overwritten. Conda package
publication should remain split between platform-specific CLI, noarch Python,
and metapackage artifacts so users can install both the client and binary with
one command.

## First public user proof

The first public proof should be a small, repeatable "first 10 minutes" path:

```bash
conda install -c conda-forge shardloom
```

```python
import shardloom as sl

client = sl.ShardLoomClient.from_env()
print(client.smoke_check())
print(client.capabilities())
```

```bash
shardloom status --format json
shardloom capabilities --format json
```

The first milestone is not full ETL support. It is:

```text
install
import
resolve CLI binary
run no-dataset smoke
run tiny local .vortex fixture where certified
emit OutputEnvelope
emit fallback_attempted=false
emit capability report
```

External proof examples should include:

```text
examples/local-python-smoke/
examples/local-vortex-benchmark/
examples/foundry-lightweight-transform/
```

Each example should include a README, environment file, input fixture, expected
output, expected certificate fields, and known limitations.

## Foundry package layout

Recommended package split:

```text
shardloom-cli        Rust binary
shardloom-python     core Python client
shardloom            Conda metapackage
shardloom-foundry    optional Foundry helper package
shardloom-benchmarks optional comparison extras
```

The Foundry helper package should be a thin integration layer. It should not
add execution semantics.

Target helper shape:

```python
from shardloom_foundry import FoundryShardLoomContext

ctx = FoundryShardLoomContext.from_transform()
client = ctx.shardloom_client()

smoke = client.smoke_check()
ctx.write_certificate(output, smoke)
```

The helper package may resolve `SHARDLOOM_BIN`, capture transform metadata,
record input/output RIDs, write certificate/metrics outputs, and produce
staging/materialization reports. It must not execute unsupported work through
Foundry compute or external engines.

## Foundry maturity ladder

```text
F0  declared only
F1  package/import in Foundry Code Repository
F2  smoke transform with CLI resolution
F3  dataset source/sink staging with certificate output
F4  Data Expectations / Data Health bridge
F5  lineage and transaction/branch evidence
F6  virtual table / external compute boundary awareness
F7  Marketplace starter product
F8  Compute Module / REST service
F9  Ontology/AIP/Workshop operational integration
F10 workload-certified Foundry deployment
```

A Foundry package at `F2` does not imply virtual-table support, Compute Module
support, external compute pushdown, or workload-certified Foundry deployment.

## Foundry execution context

`FoundryExecutionContext` should capture the platform context for a ShardLoom
run:

```text
transform RID/name
repository/project
branch
preview/build/incremental mode
input dataset refs
input virtual table refs
output dataset refs
output virtual table refs
schedule/build refs
credential refs, redacted
markings/governance refs
runtime/package versions
shardloom binary path
fallback_attempted=false
```

This context should feed evidence artifacts, benchmark rows, lineage facets,
and certificate output datasets.

## Dataset transactions, branches, and build context

Foundry dataset transactions and branches should be first-class certificate
fields.

`FoundryDatasetTransactionReport`:

```text
input dataset RID/path
input transaction ID
input branch
output dataset RID/path
output transaction type: SNAPSHOT | APPEND | UPDATE | DELETE
semantic version / transform version
branch name
build mode: preview | build | incremental
materialization/staging boundary
fallback_attempted=false
```

`FoundryBranchContextReport`:

```text
branch name
base branch
preview/build behavior
branch-specific input refs
branch-specific output refs
certificate claim level
```

Preview-mode certificates must be marked preview-only and cannot count as
production or benchmark-claim evidence.

## Incremental transform alignment

Foundry incremental builds are platform-native freshness hooks, but they are
not automatically ShardLoom live/hybrid execution.

`FoundryIncrementalRunReport`:

```text
incremental_requested
incremental_actual
snapshot_inputs
strict_append
semantic_version
previous_output_transaction
current_input_transactions
changed_files
changed_partitions
ShardLoom engine_mode: batch | live | hybrid
staging/materialization boundary
fallback_attempted=false
```

Foundry incremental support can map to CG-22 live/hybrid evidence only after
ShardLoom state, watermark, checkpoint, idempotency, and recovery certificates
exist.

## Dataset source and sink components

`FoundryDatasetSource`:

```text
dataset RID/path
branch
transaction
schema
file refs
staged local path or filesystem ref
staging/materialization boundary
native_io_certificate refs
fallback_attempted=false
```

`FoundryDatasetSink`:

```text
output dataset RID/path
transaction type
table-compatible output policy
certificate/metrics output policy
optional Vortex artifact sidecar policy
commit/recovery status
materialization/fidelity report
fallback_attempted=false
```

The first Foundry transform path should stage input files explicitly, run a
certified local ShardLoom path, and write certificates/metrics/table-compatible
outputs back to Foundry. It must not call Foundry-staged local files native
object-store execution.

## Foundry S3-compatible dataset adapter

`FoundryS3DatasetAdapter` is a future object-store-like path for Foundry
datasets:

```text
dataset RID
branch
object key
range-read support
multipart/write support where allowed
bytes requested/read
request count
credential mode
native_io_certificate refs
fallback_attempted=false
```

This path remains blocked until CG-10 object-store evidence and CG-19 Native
I/O evidence are ready for the declared workload.

## Virtual table source and sink components

Foundry virtual tables should be first-class ShardLoom source/sink/workflow
handles. They are governed external table refs, not automatic ShardLoom-native
inputs.

`FoundryVirtualTableRef`:

```text
Foundry table RID/path
source RID
external platform: Snowflake | Databricks | BigQuery | S3 | ADLS | GCS | Iceberg | other
table locator
supported operations
available compute modes
update detection / versioning status
security/export-control status
materialization/staging policy
```

Virtual table maturity:

```text
FVT0 declared
FVT1 metadata/capability discovery
FVT2 schema/version/update-detection report
FVT3 staged snapshot into Foundry dataset/local/Vortex
FVT4 ShardLoom-native execution over staged snapshot
FVT5 external compute baseline/pushdown reference
FVT6 certified virtual-table workflow
```

`FoundryVirtualTableSource` should support metadata discovery, staging policy,
external-compute boundary reporting, and eventual staged ShardLoom-native
execution evidence. `FoundryVirtualTableSink` should validate output locator,
sink requirements, staged outputs, virtual table output paths where Foundry
supports them, and commit/update-detection evidence.

## External compute boundary

Foundry compute pushdown to Snowflake, Databricks, BigQuery, or similar systems
is useful, but it is not ShardLoom execution.

`FoundryExternalComputeBoundaryReport`:

```json
{
  "boundary": "foundry_external_compute_pushdown",
  "external_engine": "snowflake",
  "api": "snowpark",
  "foundry_virtual_tables": true,
  "same_source_required": true,
  "shardloom_execution": false,
  "allowed_role": "baseline_or_migration_reference",
  "fallback_attempted": false
}
```

External compute may be used for:

```text
baseline_only
oracle_only
migration_reference
capacity/cost comparison
unsupported_as_runtime
prohibited_fallback
```

It must never be reported as ShardLoom-native execution.

## Foundry Iceberg posture

`FoundryIcebergTableSource` and `FoundryIcebergTableSink` should remain
compatibility/table-format surfaces until certified:

```text
catalog/table metadata discovery
snapshot/manifest awareness
schema/partition evidence
compatibility read path only when certified
TranslationReport required for compatibility output
commit/recovery evidence required for writes
known Foundry/Iceberg limitations surfaced
```

Foundry-managed Iceberg is not Vortex-native output. It can become a
Foundry-compatible table target only through explicit translation, fidelity,
commit, and recovery evidence.

## Data Health and Data Expectations bridge

`FoundryDataHealthBridge` maps ShardLoom evidence into Foundry quality controls:

```text
certificate.fallback_attempted == false
execution_certificate.present == true
native_io_certificate.present == true
output_rows > 0 where required
schema_digest matches expected
data_quality_checks pass
benchmark_claim_status is not overclaimed
materialization_boundary_allowed == true
```

Failures should be usable as warnings or build-stopping checks depending on the
Foundry transform policy. The bridge should make ShardLoom's certificate/data
quality status visible through Foundry's normal health surfaces where practical.

## Lineage, schedules, and Data Connection

`FoundryLineageFacet`:

```text
ShardLoom plan_id / query_id / run_id
input datasets / virtual tables / media sets
output datasets / artifacts
execution certificates
Native I/O certificates
materialization/fidelity reports
external-compute boundary reports
no-fallback proof
```

`FoundryScheduleBuildReport`:

```text
schedule RID/name
trigger type
build start/end
input freshness
output freshness
previous schedule run
ShardLoom runtime/certificate refs
missed freshness SLA diagnostic
```

`FoundryDataConnectionBoundaryReport`:

```text
source/sink type
sync/export/webhook/external-transform mode
credential/source reference, redacted
external system
inbound/outbound direction
egress/network policy
external effect
ShardLoom role: native_execution | staging | baseline | export | unsupported
```

Foundry can move data. ShardLoom must certify whether it executed on
staged/native data or merely recorded an external boundary.

## Media sets and unstructured data

`FoundryMediaSetSource` and `FoundryMediaSetSink` should align with CG-21P:

```text
media set RID/path
media item refs
MIME/type/schema
OCR/extraction/materialization status
external model/effect boundaries
incremental media support status
provenance and confidence fields
redaction policy
fallback_attempted=false
```

ShardLoom must not silently OCR, transcribe, embed, classify, decode, or call
models as part of normal ETL. Those are explicit effect boundaries.

## Ontology, Functions, AIP, models, and scenarios

These are powerful adoption surfaces, but they are later than dataset/package
proof.

`FoundryOntologyMappingReport`:

```text
source dataset -> object type
output dataset -> object/link type candidate
schema -> property mapping
primary key / object ID mapping
link type mapping
action/function boundary
governance markings
```

Potential Ontology-backed objects:

```text
ShardLoomRun
ShardLoomCertificate
ShardLoomCapabilitySnapshot
ShardLoomBenchmarkScenario
ShardLoomDataQualityResult
```

`FoundryFunctionSurface` should be read/report-first:

```text
explain_shardloom_plan(object_set)
certify_dataset(dataset_ref)
compare_benchmark_scenario(...)
summarize_capability_status(...)
retrieve_certificate(...)
```

`FoundryAipLogicBridge` should expose capability snapshots, certificates,
unsupported diagnostics, benchmark summaries, and rewrite suggestions. Execute,
write, cancel, external-effect, and destructive operations are disabled by
default and require explicit policy.

`FoundryModelBoundaryReport`:

```text
model artifact/function used
model version
batch vs live inference
feature/source lineage
prediction output boundary
external effect
ShardLoom execution role
```

`FoundryScenarioBoundaryReport`:

```text
base Ontology state
scenario ID
action/model edits
materialized object set
snapshot/fork semantics
ShardLoom run mode: baseline | scenario_eval | unsupported
```

## Compute Modules and BYOC

The first Foundry ETL path should be Conda package plus lightweight transform.
BYOC is useful when the binary/runtime must be pinned in a container. Compute
Modules are later, once `shardloom serve` or CG-23 REST is real.

`FoundryByocImageReport`:

```text
image ref and digest
linux/amd64 platform
numeric non-root user policy
Python package version
CLI binary version
fixtures/smoke script
version/provenance file
SBOM/attestation refs where available
fallback_attempted=false
```

`FoundryComputeModuleSurface`:

```text
shardloom serve
REST control plane
explain/certify API
benchmark API
certificate retrieval
Workshop/Slate callable function
pipeline-mode connector
```

`FoundryComputeModuleReadinessReport`:

```text
liveness probe status
readiness probe status
replica count
concurrency limit
CPU/memory/GPU request/limit
shardloom version
certificate schema version
REST API readiness
```

Compute Module support remains blocked until CG-23 API, security, packaging,
and governance evidence exists.

## Marketplace starter product

`FoundryMarketplaceStarterProduct`:

```text
Conda dependency instructions
smoke transform
benchmark transform
certificate output dataset
data expectations bridge
optional virtual-table staging example
optional external-compute baseline example
optional compute-module API example
schedule
docs
```

Marketplace packaging should not imply production certification. It is an
adoption artifact until a declared Foundry workload reaches `F10`.

## Foundry benchmark design

Foundry benchmark rows should distinguish compute modes:

```text
ShardLoom lightweight
Polars lightweight
DataFusion/DuckDB lightweight, if used as baseline only
Spark distributed
Snowflake/Databricks/BigQuery external pushdown
```

Recommended output schema:

```text
scenario_id
engine
compute_mode
foundry_transform_kind
input_dataset_rid
input_transaction
input_format
input_rows
input_bytes
output_rows
wall_seconds
query_seconds
startup_seconds
conversion_seconds
result_write_seconds
peak_memory_mb
cpu_cores
fallback_attempted
external_engine_invoked
materialization_boundary
certificate_json
native_io_certificate_json
correctness_digest
shardloom_version
shardloom_binary_path
baseline_version
```

Single-node ShardLoom and distributed Spark rows must be labeled separately and
not treated as the same compute mode.

## Governance

`FoundryGovernanceBoundaryReport`:

```text
markings present
organizations present
inherited markings
marking propagation behavior
certificate visibility policy
redaction policy
agent_visible
export_allowed
contains_paths
contains_schema_names
contains_query_text
contains_samples
contains_credentials=false
```

Evidence artifacts may include paths, schema names, query text, sample values,
benchmark metadata, or platform identifiers. Redaction, retention, export, and
agent visibility must be explicit.

## Foundry certification rule

ShardLoom is Foundry-certified for a declared workload only when:

```text
packages install/import in the declared Foundry environment
the CLI binary resolves deterministically
the workload's source/sink handles are represented explicitly
staging/materialization boundaries are reported
ShardLoom-native execution is certificate-backed
Native I/O/fidelity evidence exists for every source/sink path
Foundry transactions, branches, build/preview/incremental mode are recorded
Data Health/Expectations integration is present where required
lineage/certificate outputs are durable and redacted
external compute boundaries are baseline/oracle/migration only
governance/export/agent policies are explicit
fallback_attempted=false and external_engine_invoked classification are visible
```

## Non-goals

Do not:

```text
make Foundry a required dependency
make users build Rust inside Foundry transforms
use BYOC as the only path
call Foundry-staged local files native object-store execution
claim .vortex Foundry table output until source/sink evidence exists
report Snowflake/Databricks/BigQuery/Spark compute as ShardLoom execution
turn virtual tables into automatic native execution support
make benchmark extras part of the core Foundry install
allow Ontology/AIP/Function/Action writes without explicit policy
publish packages, releases, or Marketplace products without human approval
```

## Implementation sequencing

The practical sequence is:

```text
1. Public release identity and provenance policy.
2. Local Conda package proof for CLI, Python, and metapackage.
3. External examples: local smoke, local Vortex benchmark, Foundry transform.
4. Foundry Artifact Repository package proof.
5. Foundry lightweight transform smoke with certificate output.
6. Foundry benchmark proof with labeled baselines.
7. BYOC proof when packaging limits require it.
8. Foundry virtual table and external-compute boundary reports.
9. Data Health, lineage, transaction, branch, and schedule surfaces.
10. Marketplace starter product.
11. Compute Module REST/API surface after CG-23 runtime exists.
12. Ontology/AIP/Workshop operational integration.
13. Workload-certified Foundry deployment.
```

This keeps ShardLoom available and useful in Foundry while preserving the core
no-fallback, Vortex-native, certificate-first execution identity.
