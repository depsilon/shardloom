# Substrait Report-Only Contract

Status: implemented report-only contract for `GAR-0022-A`.

## Decision

ShardLoom exposes an explicit Substrait import/export report-only contract without adding a
Substrait dependency, parser, exporter, imported-plan runtime, external engine bridge, filesystem or
network probing, or fallback execution.

The current contract is intentionally narrow:

- `shardloom plan-import substrait <source_label>` emits deterministic unsupported/report-only
  fields.
- `shardloom plan-export substrait` emits deterministic unsupported/report-only fields.
- `substrait-like` remains the canonical internal interop format label.
- `substrait` is accepted as a CLI alias for the same report-only posture.

## Current Support

The report fields use schema `shardloom.substrait_report_only_contract.v1`.

Required current values:

- `substrait_report_contract_support_status=report_only`
- `substrait_import_parser_status=not_implemented`
- `substrait_export_serializer_status=not_implemented`
- `substrait_imported_plan_execution_status=blocked`
- `substrait_dependency_status=not_added`
- `substrait_dependency_license_approved=false`
- `substrait_parser_executed=false`
- `substrait_payload_parsed=false`
- `substrait_export_serialization_performed=false`
- `substrait_imported_plan_execution_allowed=false`
- `substrait_runtime_execution=false`
- `substrait_external_engine_invoked=false`
- `substrait_fallback_attempted=false`
- `substrait_claim_gate_status=not_claim_grade`

## Evidence Required Before Promotion

Future Substrait import/export work must attach evidence for:

- dependency and license approval,
- parser schema/version support,
- construct coverage matrix,
- round-trip fixtures,
- imported-plan capability gate coverage,
- redaction and secret-safety policy,
- no-fallback and no-external-engine evidence.

## Non-Goals

- No Substrait dependency is added in this slice.
- No Substrait payload is parsed.
- No Substrait plan is exported.
- No imported plan is executed.
- No external engine is invoked.
- No filesystem, network, catalog, or adapter probe is performed.
- No runtime support, SQL/DataFrame support, or production interop claim is created.

## Claim Boundary

ShardLoom may claim only that Substrait import/export requests have deterministic, side-effect-free,
report-only diagnostics. It may not claim Substrait compatibility, plan round-tripping, imported-plan
execution, or production interoperability.

`fallback_attempted=false` and `external_engine_invoked=false` are mandatory for this contract.
