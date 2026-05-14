# Diagnostics Normalization Backlog

## Purpose

This document inventories diagnostic normalization work needed before or alongside future CG
implementation. Active status and queue placement live in
`docs/architecture/phased-execution-plan.md`.

It does not authorize runtime behavior, change diagnostic semantics by itself, or permit fallback
execution.

## Design Posture

- `shardloom-core::Diagnostic` is the canonical structured diagnostic record.
- `DiagnosticCode`, `DiagnosticSeverity`, `DiagnosticCategory`, and `FallbackStatus` exist
  centrally.
- `OutputEnvelope` carries diagnostics and fallback state.
- Plan/report surfaces should preserve `fallback_execution_allowed=false`.
- User-visible CLI/parser/error plumbing should move toward stable structured diagnostics over time.

## Promoted Checklist

- [x] P0 - Stable fallback/no-fallback diagnostics
  - Ensure unsupported execution paths report `fallback_attempted=false`.
  - Ensure output envelopes keep `fallback_execution_allowed=false`.
  - Ensure commands do not return success by masking fatal diagnostics.
  - Ensure external baseline references never become fallback diagnostics.
- [x] P1 - CLI parse/argument errors
  - Audit direct string errors for missing args.
  - Audit direct string errors for unknown flags/signals.
  - Normalize common invalid-input paths through helper constructors.
  - Leave broad command-by-command migration to targeted follow-up work.
- [x] P2 - `ShardLoomError` conversion normalization
  - Confirm invalid input maps to `DiagnosticCode::InvalidInput`.
  - Confirm configuration errors map to configuration diagnostics.
  - Confirm not-implemented maps to unsupported/not-implemented diagnostics.
  - Keep future helper candidates scoped to stable command context fields.
- [x] P3 - Diagnostic category consistency promoted to `GAR-0012-A`
  - Bad CLI argument -> invalid input.
  - Missing config -> configuration.
  - Planned but not implemented -> unsupported feature.
  - Object-store blocked -> object-store.
  - Materialization blocked -> materialization.
  - Fallback prohibited -> no-fallback policy.
- [x] P4 - Output envelope field normalization promoted to `GAR-0012-B`
  - Include `schema_version`.
  - Keep diagnostics arrays stable.
  - Keep fallback object stable.
  - Derive command status from severity-aware diagnostics.
  - Avoid requiring agents to scrape human text.
- [x] P5 - Planned diagnostic helpers promoted to `GAR-0012-A`
  - `cli_missing_arg_diagnostic(command, arg)`
  - `cli_unknown_signal_diagnostic(command, signal)`
  - `cli_unknown_command_diagnostic(command)`
  - `unsupported_feature_diagnostic(feature, reason)`
  - `object_store_blocked_diagnostic(target)`
  - `no_fallback_policy_diagnostic(feature)`
  - `report_status_to_command_status`
- [x] P6 - Planned diagnostic tests promoted to `GAR-0012-B`
  - Every JSON-capable command includes fallback attempted false.
  - Unknown command returns structured diagnostics once migration starts.
  - Unknown signal diagnostics use invalid input.
  - Unsupported feature diagnostics never imply fallback.
  - Output envelope status matches highest severity diagnostic.
  - Diagnostics remain stable across text and JSON.

## Completed Ledger

- [x] GAR-0012-A helper/category normalization
  - Added `Diagnostic::materialization_required` and `Diagnostic::object_store_blocked` helpers.
  - Routed `workflow-unsupported-plan` through helper-backed categories for invalid-input,
    unsupported-feature, materialization, object-store, and no-fallback diagnostics.
  - Added workflow blocker rows for object-store reads and fallback-engine requests without enabling
    runtime behavior.
  - Added CLI and Python tests proving normalized categories remain machine-readable.
- [x] GAR-0012-B distributed/object-store diagnostic propagation
  - Propagated `ObjectStoreRuntimeBlockerMatrixRow::to_diagnostic()` entries into
    `cg10-object-store-runtime-gate` as info-level JSON envelope diagnostics.
  - Added typed fields for blocker diagnostic propagation, count, code/category/severity order, and
    success envelope status.
  - Added text summary lines and Python client coverage proving the blocked distributed/object-store
    diagnostics survive JSON/text/Python boundaries.
  - Preserved report-only behavior, `fallback_attempted=false`, `external_engine_invoked=false`,
    and `execution=not_performed`.
- [x] R3.3a helper status
  - Added `cli_missing_arg_error(command, arg)`.
  - Added `cli_unknown_arg_error(command, value)`.
  - Added focused CLI tests for `ShardLoomError::to_diagnostic()` normalization.
  - Deferred broad command-by-command migration, unknown signal normalization, and output envelope
    redesign.
- [x] R3.3b helper status
  - Added `cli_unknown_signal_error(command, signal_family, token)`.
  - Migrated encoded-read boundary and metadata-probe signal parser unknown-token branches.
  - Deferred broad signal parser migration.
- [x] R3.3c audit result
  - Audited `OutputEnvelope::from_diagnostic`.
  - Audited `OutputEnvelope::from_error`.
  - Audited `OutputEnvelope::has_errors`.
  - Added focused tests.
  - Did not redesign output envelopes broadly.

## Guardrails

- Unsupported behavior must fail explicitly.
- Fallback execution must remain disabled and machine-readable.
- This backlog does not authorize new runtime behavior.
- Any future actionable item must be promoted into `phased-execution-plan.md` before implementation.
