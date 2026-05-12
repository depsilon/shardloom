# RFC Coverage Follow-Through

## Purpose

This document records the Priority 3.6 contract for carrying RFC 0010, RFC 0011, RFC 0020, RFC
0022, and RFC 0023 into the next user/runtime expansion lanes.

The corresponding code surface is:

```text
RfcCoverageFollowThroughReport
RfcCoverageFollowThroughEntry
RfcCoverageFollowThroughArea
RfcCoverageFollowThroughStatus
plan_rfc_coverage_followthrough()
```

This is a report-only coverage gate. It does not add parsers, execute imported plans, probe
catalogs, run extension code, publish packages, expand dependencies, invoke external engines, or
permit fallback execution.

## RFC rows

| RFC | Area | Required before runtime expansion |
| --- | --- | --- |
| RFC 0010 | Developer and agent usability | Deterministic machine-readable and human-readable CLI/Python/future REST/capability/diagnostic/benchmark/certificate surfaces; import, discovery, and dry-run safety before execution/write permissions. |
| RFC 0011 | Modular extensibility | Typed effect/materialization metadata for SQL, UDFs, unstructured/media, LLM/API calls, embeddings, vectors, and external effects; sandboxing, governance, correctness, and certificate evidence before effectful execution. |
| RFC 0020 | Schema, catalog, and table compatibility | Real snapshot/schema/partition/delete/catalog evidence before metadata promotion; metadata discovery separate from read/write/commit/update/delete/merge certification. |
| RFC 0022 | Native plan IR and interop | Native plan import/export and capability-gate evidence before imported plan execution; optional dependency-free Substrait-like posture; no interop fallback bridge. |
| RFC 0023 | Extension/plugin ABI and sandboxing | Manifest, lifecycle, permission, provenance, signing, sandbox, resource-limit, and agent-inspection evidence before plugin or UDF execution; manifest inspection without executing code. |

## CLI surface

`rfc-coverage-followthrough-plan --format json` exposes:

- `rfc_coverage_status=evidence_required`
- `rfc_coverage_entry_count=5`
- `rfc_order=rfc_0010,rfc_0011,rfc_0020,rfc_0022,rfc_0023`
- `deterministic_machine_readable_required=true`
- `import_discovery_dry_run_safety_required=true`
- `typed_effect_materialization_metadata_required=true`
- `metadata_discovery_separate_from_read_write_commit=true`
- `imported_plan_execution_blocked=true`
- `extension_manifest_inspection_only=true`
- `extension_code_execution_blocked=true`
- `all_entries_runtime_expansion_blocked=true`
- `all_entries_dependency_expansion_blocked=true`
- `all_entries_external_effects_blocked=true`
- `fallback_attempted=false`

## Runtime posture

This closes the Priority 3.6 coverage-follow-through surface only. Runtime expansion, parser
expansion, adapter expansion, dependency expansion, imported plan execution, extension execution,
table write/update/delete/merge claims, external effects, external engine invocation, and fallback
execution remain blocked until later lanes add the required evidence.
