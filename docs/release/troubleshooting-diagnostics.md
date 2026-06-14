<!-- SPDX-License-Identifier: Apache-2.0 -->

# Troubleshooting Diagnostics

Status: local v1 troubleshooting contract for ShardLoom JSON envelopes, deterministic diagnostic
codes, and support bundles.

Canonical diagnostic-code policy:
[`docs/release/diagnostic-code-stability.md`](diagnostic-code-stability.md).

Validate with:

```bash
python scripts/check_v1_observability_support.py
```

## First Checks

Use machine-readable output first:

```bash
shardloom doctor --format json
shardloom support-bundle --format json
```

For an individual failed command, inspect:

- `schema_version`
- `command`
- `status`
- `diagnostics[].code`
- `diagnostics[].category`
- `diagnostics[].suggested_next_step`
- `fallback_attempted=false`
- `external_engine_invoked=false`

If the command emitted an unsupported or blocked result, keep the JSON envelope attached to the
issue. Do not paste credentials, tokens, full private paths, private data rows, or unredacted query
text.

## Stable Codes

Common v1 diagnostic codes include:

- `SL_INVALID_INPUT`: the command shape, argument, signal, SQL fragment, or user-facing request is
  malformed or outside the admitted contract.
- `SL_UNSUPPORTED_SQL`: the requested SQL/planning surface is not currently supported by native
  ShardLoom execution.
- `SL_RESOURCE_BUDGET_EXCEEDED`: the local resource gate denied work before unsafe materialization
  or process OOM.
- `SL_NO_FALLBACK_EXECUTION`: ShardLoom refused to delegate work to an external fallback engine.

The complete stable v1 set is maintained in
[`docs/release/diagnostic-code-stability.md`](diagnostic-code-stability.md).

## Route And Benchmark Context

For benchmark or route issues, include:

- route id or `route_lane_id`
- timing surface such as `hot_runtime` or `publication_proof`
- evidence tier such as `metadata_sink` or `publication_full`
- `route_total_formula`
- certificate status fields
- whether sink timing is included in the selected route total

No performance claim is valid unless timing surface, evidence tier, and included stages are stated.

## Support Bundle Boundary

`support-bundle --format json` is local and redacted by default. It should report:

- `support_bundle_written=false`
- `redaction_status=redacted`
- `raw_secret_values_present=false`
- `filesystem_write_performed=false`
- `network_probe_performed=false`
- `runtime_execution=false`
- `fallback_attempted=false`
- `external_engine_invoked=false`

Remote support upload, telemetry export, live profiling, and production observability are not part
of the v1 support bundle.

