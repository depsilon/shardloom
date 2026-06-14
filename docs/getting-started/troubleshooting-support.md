<!-- SPDX-License-Identifier: Apache-2.0 -->

# Troubleshooting And Support Bundle

Start with machine-readable ShardLoom evidence. Do not infer fallback, package, production, or
performance posture from a human log line.

```powershell
shardloom doctor --format json
shardloom support-bundle --format json
```

If the CLI is source-built and not on `PATH`, use the resolved local binary:

```powershell
target\debug\shardloom doctor --format json
target\debug\shardloom support-bundle --format json
```

## What To Inspect

- `schema_version`
- `command`
- `status`
- `diagnostics[].code`
- `diagnostics[].suggested_next_step`
- `claim_gate_status`
- `fallback_attempted`
- `external_engine_invoked`

Expected v1 local support-bundle posture:

```text
support_bundle_written=false
redaction_status=redacted
raw_secret_values_present=false
filesystem_write_performed=false
network_probe_performed=false
runtime_execution=false
fallback_attempted=false
external_engine_invoked=false
```

## Support Boundary

Attach the JSON envelope and the relevant command when filing an issue. Do not paste credentials,
tokens, private paths, private data rows, or unredacted query text.

Remote support upload, production telemetry, live profiling collection, OpenTelemetry export, and
OpenLineage export are outside the current v1 support boundary. The detailed diagnostic-code policy
is [`docs/release/diagnostic-code-stability.md`](../release/diagnostic-code-stability.md), and the
support contract is [`docs/release/troubleshooting-diagnostics.md`](../release/troubleshooting-diagnostics.md).
