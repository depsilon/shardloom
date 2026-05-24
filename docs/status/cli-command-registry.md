# CLI Command Registry Status

Schema: `shardloom.command_registry.v1`

Source: `shardloom-cli/src/command_registry.rs`

Report id: `review-p1-1.command_registry`

Registered command count: 190

Support-state vocabulary: executable, feature_gated, diagnostic_only, report_only, blocked, future

Agent metadata command: `shardloom command-metadata [command] --format json`

Command-specific help command: `shardloom help [command] --format json`

Capability surface: `shardloom capabilities api-surfaces --format json`

Evidence fields: command, family, support_state, side_effect_level, usage_fragment,
feature_gate_status, input_contract, output_contract, owning_phase_item, claim_boundary,
fallback_boundary, fallback_attempted, external_engine_invoked

Claim boundary: command discoverability and metadata consolidation only. Runtime support and public
claims remain governed by `runs-today`, capability discovery, execution certificates, release gates,
and benchmark evidence.

No-fallback status: fallback_attempted=false and external_engine_invoked=false.

This page is a status snippet, not a separate hand-maintained command table. The full per-command
rows are generated from the registry through the CLI metadata and capability surfaces above.
