# Wrapper And Connector Implementation Registry

Status: completed report-only contract for `GAR-0037-A`.

Schema: `shardloom.wrapper_connector_implementation_registry.v1`

Report id: `gar-0037-a.wrapper_connector_implementation_registry`

Primary surfaces:

- `shardloom capabilities api-surfaces --format json`
- Python `ctx.capabilities().wrapper_connector_registry`
- Python `ctx.wrapper_connector_registry()`

## Purpose

ShardLoom has a one-protocol, many-thin-wrappers architecture, but that does not
mean every wrapper or connector exists. This registry separates implemented
local wrapper surfaces from planned, report-only, and blocked ecosystem
connectors so users do not infer runtime support from RFC language.

The registry is intentionally conservative:

- the source-tree Python CLI JSON wrapper is marked `ready_local`;
- typed Python capability views and scoped generated-source helpers are marked
  `ready_local`;
- generated clients and local report viewers remain `report_only`;
- DB-API, SQLAlchemy, Ibis, dbt, Airflow, Dagster, Prefect, MCP, Flight SQL,
  ADBC, JDBC/ODBC, BI, and Grafana connectors remain `blocked`.

Current row ids:

- `python_cli_json_client`
- `python_typed_capability_views`
- `python_generated_source_helpers`
- `rust_client`
- `typescript_javascript_client`
- `go_client`
- `java_jvm_client`
- `dotnet_client`
- `r_client`
- `rest_openapi_generated_client`
- `ci_report_viewer`
- `foundry_transform_wrapper`
- `python_dbapi`
- `sqlalchemy`
- `ibis`
- `dbt`
- `airflow`
- `dagster`
- `prefect`
- `mcp`
- `flight_sql`
- `adbc`
- `jdbc_via_flight_sql`
- `odbc`
- `bi_connector`
- `grafana_datasource`

## Stable Fields

Top-level fields:

- `wrapper_connector_registry_schema_version`
- `wrapper_connector_registry_report_id`
- `wrapper_connector_registry_docs_ref`
- `wrapper_connector_registry_support_status_vocabulary`
- `wrapper_connector_registry_row_order`
- `wrapper_connector_registry_ready_local_count`
- `wrapper_connector_registry_report_only_count`
- `wrapper_connector_registry_blocked_count`
- `wrapper_connector_registry_diagnostic_codes`
- `wrapper_connector_registry_required_evidence`
- `wrapper_connector_registry_dependency_expansion_allowed=false`
- `wrapper_connector_registry_wrapper_ecosystem_claim_allowed=false`
- `wrapper_connector_registry_fallback_attempted=false`
- `wrapper_connector_registry_external_engine_invoked=false`
- `wrapper_connector_registry_all_rows_no_fallback_no_external_engine=true`
- `wrapper_connector_registry_claim_gate_status=not_claim_grade`

Per-row fields use this prefix:

```text
wrapper_connector_registry_row_<row_id>_
```

Each row emits:

- `family`
- `planned_package`
- `maturity`
- `primary_transport`
- `support_status`
- `user_visible_surface`
- `implementation_evidence`
- `deterministic_diagnostic_code`
- `required_evidence`
- `explicit_execution_available`
- `dependency_added=false`
- `network_listener_started=false`
- `data_plane_bridge_supported=false`
- `external_engine_invoked=false`
- `fallback_attempted=false`
- `claim_gate_status=not_claim_grade`
- `claim_boundary`

## Status Vocabulary

`ready_local`
: A scoped local wrapper surface exists. This currently applies to the Python
  CLI JSON client, typed Python capability views, and scoped generated-source
  Python helpers. It does not imply production package publication or a wrapper
  ecosystem claim.

`report_only`
: Architecture or docs exist, but no package/runtime support exists.

`blocked`
: The connector or wrapper would require missing runtime proof, a server,
  transport, data-plane bridge, dependency, or claim gate. Blocked rows are
  explicit so they do not disappear from user-facing capability views.

## Claim Boundary

No generated clients, DB-API, SQLAlchemy, Ibis, dbt, Airflow, Dagster, Prefect, MCP, Flight SQL,
ADBC, JDBC/ODBC, BI, Grafana, Foundry package, or REST server is implemented by this registry.
No external engine can satisfy a wrapper row, and No fallback execution is permitted.

This registry does not add generated clients, DB-API, SQLAlchemy, Ibis, dbt,
Airflow, Dagster, Prefect, MCP, Flight SQL, ADBC, JDBC/ODBC, BI, Grafana,
Foundry package, REST server, dependency expansion, network listener, external
engine execution, or fallback.

It also does not create production SQL/DataFrame, object-store/lakehouse,
Foundry, performance, Spark-replacement, package-publication, or broad wrapper
ecosystem claims.

## Verification

Expected local checks:

```powershell
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-core wrapper_architecture --lib
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-cli --test capability_discovery_snapshots
python -m unittest python.tests.test_cli_client
python -m compileall -q python/src python/tests scripts
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-contract-tests --test release_readiness_metadata
```
