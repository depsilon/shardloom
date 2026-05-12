# RFC 0037: Client, Wrapper, SDK, and Ecosystem Integration Surface

## Purpose

Define ShardLoom's client and wrapper architecture.

The core rule:

```text
One canonical protocol and evidence model.
Many thin clients and ecosystem wrappers.
No wrapper becomes a separate execution engine.
```

Wrappers exist to make ShardLoom usable from Python, Rust, TypeScript, Go, JVM,
.NET, R, orchestration systems, SQL tools, notebooks, agents, and later
high-throughput data-plane clients. They must preserve the same diagnostics,
certificates, materialization boundaries, result references, and no-fallback
truth as the CLI, Python client, and future REST/API surfaces.

## Status

Accepted as client/wrapper architecture intake material.

This RFC does not add runtime behavior, generated clients, REST server behavior,
Flight/ADBC data-plane behavior, SQL execution, DataFrame runtime, DB-API
driver behavior, SQLAlchemy/Ibis/dbt/Airflow/Dagster/Prefect/MCP behavior,
native bindings, package publication, dependencies, external engine execution,
or fallback execution.

## Scope ownership

The wrapper surface is cross-cutting:

| Area | Primary owner | Wrapper relationship |
| --- | --- | --- |
| CLI JSON protocol | CG-11 / RFC 0030 | First stable transport and golden fixture source |
| Python client | CG-20 / CG-21 | First thin wrapper over CLI JSON |
| REST/OpenAPI | CG-23 / RFC 0035 | Future control-plane transport and generated-client source |
| Flight/ADBC | CG-23 / CG-19 | Future explicit Arrow data-plane boundary |
| Packaging | CG-18 / RFC 0024 / RFC 0036 | Wrapper packages and provenance evidence |
| Capability surface | CG-20 / RFC 0032 | Wrapper maturity and support status |
| Evidence/policy | Priority 3.7 | Shared envelope, policy, lifecycle, and parity contracts |

## Architecture

The conceptual stack is:

```text
protocol schemas
  -> transport adapters
  -> client core
  -> language SDKs
  -> ecosystem wrappers
```

Wrappers must not parse ad hoc human output. They must consume canonical
request/response schemas, `OutputEnvelope` fields, problem/unsupported
diagnostics, result refs, certificates, and report artifacts.

## Protocol schemas

The shared protocol layer owns:

```text
OutputEnvelope
CapabilitySnapshot
ExecutionCertificate
NativeIoCertificate
EvidenceArtifactEnvelope
EvidenceArtifactSafety
ShardLoomExecutionPolicy
ResultRef
ProblemDetails / unsupported diagnostics
EngineSelectionReport
MaterializationBoundaryReport
AdapterFidelityReport
BenchmarkClaimEvidenceReport
ProtocolSurfaceParityReport
WrapperCapabilityReport
```

Schemas should be language-neutral where possible:

```text
JSON Schema
OpenAPI schemas
AsyncAPI schemas later
golden fixtures
versioned examples
```

## Transports

Approved transport families:

```text
CLI subprocess transport
REST HTTP transport
Flight/ADBC data-plane transport
mock transport
recording/replay transport
```

The current Python client is a CLI subprocess transport wrapper over
`shardloom --format json`. Future REST and Flight/ADBC transports must expose
the same protocol truth rather than invent independent support statuses.

## Client core

Client cores should expose stable operations independent of transport:

```text
status
capabilities
adapter discovery
plan validation
explain
execute
query status
cancel
results
certificates
profile
benchmark
migration
diagnostics
```

Execution and write operations remain unavailable or policy-gated until the
underlying ShardLoom surface is certified for the declared workload.

## Wrapper maturity

Wrappers must report a maturity level:

```text
W0 declared only
W1 package/import smoke
W2 side-effect-free capability discovery
W3 typed envelope parsing
W4 plan/explain/validate support
W5 execute certified local paths
W6 result delivery and certificate access
W7 workload-certified integration
```

No wrapper may report broad support because an endpoint, package, or generated
client exists.

## Wrapper families

Language SDK registry:

```text
Python
Rust
TypeScript / JavaScript
Go
Java / JVM
.NET
R
future generated OpenAPI clients
```

Python ecosystem registry:

```text
Python DB-API
SQLAlchemy dialect
Ibis backend
pandas / Arrow helpers
notebook display surfaces
```

Workflow wrapper registry:

```text
dbt adapter
Airflow provider
Dagster integration
Prefect collection
CI/report-viewer integrations
```

Remote and data-plane posture:

```text
ADBC
Flight SQL
JDBC via Arrow Flight SQL
ODBC later only if needed
Superset/BI readiness through SQLAlchemy
Grafana data-source plugin posture after SQL/API maturity
```

Agent wrapper posture:

```text
MCP resources: capabilities, schemas, plans, certificates, benchmark reports,
diagnostics.

MCP tools: validate_plan, explain_plan, estimate_plan, certify_query,
inspect_capabilities.
```

Agent execute, write, cancel, destructive operations, external effects, and
credentialed operations are disabled by default and require explicit policy.

## Wrapper invariants

Every wrapper must preserve these invariants:

1. Importing the package does not execute ShardLoom.
2. Constructing a client does not probe datasets, catalogs, object stores, or
   external services.
3. Capability discovery is side-effect-free.
4. Unsupported operations return structured diagnostics.
5. External engines are never runtime fallback.
6. Result materialization is explicit.
7. Certificates are preserved, not discarded.
8. `fallback_attempted` and `external_engine_invoked` remain visible.
9. Wrapper version and protocol version are reported.
10. Golden contract tests cover envelopes, errors, capabilities, result refs,
    materialization reports, and certificates.

Data wrappers additionally must preserve:

```text
pandas/Arrow/data.frame conversion is a materialization boundary.
SQL support is staged by parse, bind, plan, execute, and certify.
Large results use artifact refs, Arrow/Flight, or Vortex artifacts, not giant JSON.
```

## Non-goals

- Do not build full JDBC or ODBC drivers from scratch before Flight/ADBC and SQL
  maturity justify them.
- Do not build a native PyO3 binding as the default Python path.
- Do not build Spark, Polars, DuckDB, DataFusion, or Java/Scala query-engine
  wrappers as runtime execution paths.
- Do not make BI connectors imply production SQL support.
- Do not let generated clients drift from `OutputEnvelope`, certificates,
  diagnostics, result refs, materialization reports, or no-fallback fields.

## Acceptance

- `ProtocolSurfaceParityReport` maps every CLI JSON field to Python, REST,
  generated-client, MCP, and future data-plane surfaces or records an explicit
  unavailable reason.
- `WrapperCapabilityReport` records wrapper version, protocol version, maturity,
  supported transports, exposed fields, unavailable fields, result behavior,
  materialization boundaries, certificate access, and fallback status.
- Every wrapper has golden contract fixtures before moving beyond W2/W3.
- No wrapper executes unsupported work through Spark, DataFusion, DuckDB,
  Polars, Velox, Trino, Dask, Ray, pandas, Snowflake, Databricks, BigQuery, or
  Vortex query-engine integrations.
- No wrapper reports planned features as supported.
- No wrapper hides decode/materialization, external effects, or residual
  evaluation.

## Phase placement

This RFC governs Priority 3.8 in `docs/architecture/phased-execution-plan.md`.
It links RFC 0030, RFC 0032, RFC 0035, RFC 0036, and the Priority 3.7
operational hardening contracts without creating a new core execution gate.
