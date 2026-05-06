# Diagnostics Normalization Backlog

## Purpose

- This document inventories diagnostic normalization work needed before or alongside future CG implementation.
- It does not authorize runtime behavior.
- It does not change diagnostic semantics by itself.
- Unsupported behavior must continue to fail explicitly.
- Fallback execution must remain disabled and machine-readable.

## Current diagnostic posture

- `shardloom-core::Diagnostic` is the canonical structured diagnostic record.
- `DiagnosticCode`, `DiagnosticSeverity`, `DiagnosticCategory`, and `FallbackStatus` exist centrally.
- `OutputEnvelope` carries diagnostics and fallback state.
- Many plan/report surfaces already preserve `fallback_execution_allowed=false`.
- Remaining work is mostly normalization of user-visible CLI/parser/error plumbing.

## P0 — Stable fallback/no-fallback diagnostics

- Ensure every unsupported execution path reports `fallback_attempted=false`.
- Ensure every output envelope keeps `fallback_execution_allowed=false`.
- Ensure no command returns success by masking an error/fatal diagnostic.
- Ensure external baseline references never become fallback diagnostics.

## P1 — CLI parse/argument errors

Audit scope: `shardloom-cli/src/main.rs`.

- Commands with direct string errors for missing args: many command branches still print usage text and return string-backed `ShardLoomError::InvalidOperation` for missing positional inputs.
- Commands with direct string errors for unknown flags/signals: signal parsers (for output payload, encoded-read boundary/probe, commit intent/protocol/marker, staged manifest, retry/cancellation gates, and related write/readiness command families) return plain `InvalidOperation("unknown ... token")` strings.
- Commands with direct `invalid input` style errors: parsing errors such as invalid primitive formats, invalid numeric args (`memory_gb`, `max_parallelism`, `estimated_bytes`), and invalid URI/identifier values often use inline formatted strings.
- Commands that already route through `Diagnostic::invalid_input`: paths using `OutputEnvelope::from_error(..., &ShardLoomError::InvalidOperation(...))` are normalized through `ShardLoomError::to_diagnostic()` and produce `DiagnosticCode::InvalidInput` + `DiagnosticCategory::InvalidInput`.
- Commands that need future migration: branches that only emit usage/unknown-token text to stderr before non-zero exit should migrate to shared CLI diagnostic helpers and consistent JSON envelope diagnostics.

No broad CLI migration is performed in this audit PR.

## P2 — `ShardLoomError` conversion normalization

Current conversion path audit:

- `ShardLoomError` is defined in `shardloom-core/src/lib.rs` (no `shardloom-core/src/error.rs` exists in this repository).
- Invalid input currently maps via `ShardLoomError::InvalidOperation` -> `Diagnostic::invalid_input("operation", ...)`.
- Configuration errors currently map via `ShardLoomError::Message` -> `Diagnostic::configuration_error("runtime", ...)`.
- Not-implemented currently maps via `ShardLoomError::NotImplemented` -> `Diagnostic::not_implemented(...)`.
- Raw strings remain in CLI command parsing/usage/error branches that do not always produce fully structured diagnostics for every user-visible failure path.
- Future helper candidates should centralize frequent parse/usage/unknown-token diagnostic creation and standardize command context fields.

## P3 — Diagnostic category consistency

Desired normalization mapping:

- bad CLI argument -> InvalidInput / `DiagnosticCategory::InvalidInput`
- missing config -> Configuration / `DiagnosticCategory::Configuration`
- planned but not implemented -> NotImplemented / `DiagnosticCategory::UnsupportedFeature`
- unsupported source/format -> Unsupported* / UnsupportedFeature or Translation
- object-store blocked -> ObjectStoreUnsupported / ObjectStore
- materialization blocked -> MaterializationRequired / Materialization
- fallback prohibited -> NoFallbackExecution / NoFallbackPolicy

## P4 — Output envelope field normalization

- all JSON output should include `schema_version`
- diagnostics array should be stable
- fallback object should be stable
- command status should be derived from severity-aware diagnostics
- future CLI commands should not require agents to scrape human text

## P5 — Future diagnostic helpers

Candidate helpers for follow-up PRs:

- `cli_missing_arg_diagnostic(command, arg)`
- `cli_unknown_signal_diagnostic(command, signal)`
- `cli_unknown_command_diagnostic(command)`
- `unsupported_feature_diagnostic(feature, reason)`
- `object_store_blocked_diagnostic(target)`
- `no_fallback_policy_diagnostic(feature)`
- `report_status_to_command_status` helper

Do not implement helpers in this PR.

## P6 — Tests to add later

- every command with `--format json` includes fallback attempted false
- unknown command returns structured diagnostic once migration starts
- unknown signal diagnostics use InvalidInput
- unsupported feature diagnostics never imply fallback
- output envelope status matches highest severity diagnostic
- diagnostics remain stable across text/json


### R3.3a helper status

- Added `cli_missing_arg_error(command, arg)` to build stable missing-required-argument invalid-input errors.
- Added `cli_unknown_arg_error(command, value)` to build stable unknown-command/unknown-argument invalid-input errors.
- Added focused CLI tests verifying `ShardLoomError::to_diagnostic()` normalization to:
  - `DiagnosticCode::InvalidInput`
  - `DiagnosticCategory::InvalidInput`
  - `fallback.attempted=false`
  - `fallback.allowed=false`
- Deferred work:
  - broad command-by-command CLI migration remains out of scope.
  - unknown signal normalization remains out of scope.
  - output envelope command-status derivation audit remains out of scope.
- Next recommended PR:
  - R3.3b unknown signal diagnostic normalization.


### R3.3b helper status

- Added `cli_unknown_signal_error(command, signal_family, token)` to normalize unknown-signal parse failures to InvalidInput diagnostics.
- Migrated `parse_vortex_encoded_read_boundary_signals` and `parse_vortex_encoded_read_metadata_probe_signals` unknown-token branches to the helper.
- Broad signal parser migration remains deferred.
- Next recommended PR:
  - R3.3c output envelope command-status derivation audit.


### R3.3c audit result

- `OutputEnvelope::from_diagnostic` status derivation audited.
- `OutputEnvelope::from_error` normalization audited.
- `OutputEnvelope::has_errors` severity/status behavior audited.
- Focused tests added.
- No broad output-envelope redesign performed.
- Next recommended cleanup:
  - R3.4 terminology consolidation backlog.
