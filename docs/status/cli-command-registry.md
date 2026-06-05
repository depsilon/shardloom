# CLI Command Registry Status

Schema: `shardloom.command_registry.v1`

Source: `shardloom-cli/src/command_registry.rs`

Report id: `review-p1-1.command_registry`

Registered command count: 198

Support-state vocabulary: executable, feature_gated, diagnostic_only, report_only, blocked, future

User-surface graduation posture vocabulary: high_level_context, client_only, diagnostic_only,
feature_gated, not_user_facing

Agent metadata command: `shardloom command-metadata [command] --format json`

Command-specific help command: `shardloom help [command] --format json`

Public workflow facade commands: `shardloom route <sql|python|dataframe|cli> --format json`,
`shardloom run <sql|python|dataframe|cli> --format json`, and
`shardloom prepare <sql|python|dataframe|cli> --format json`

Scoped public helper coverage: Python lazy DataFrame bounded `collect()`, general `write(...)`,
`write_jsonl(...)`, `write_csv(...)`, structured write aliases, generated-source direct writes,
source-free SQL writes, admitted local/generated fanout helpers, and explicit native Vortex
primitive collect/local-execution helpers route through `shardloom run` and preserve typed report
views. Lower smoke/runtime/primitive commands remain executable `client_only` diagnostics and
benchmark/evidence surfaces, while `route`, `run`, and `prepare` are the high-level CLI context
commands. Future helper families and any future native Vortex write-helper payloads are deferred
until their owning runtime items define explicit facade payload contracts.

Help aliases: shardloom --help; shardloom -h; shardloom <command> --help

Capability surface: `shardloom capabilities api-surfaces --format json`

Evidence fields: command, family, support_state, user_surface_graduation_posture,
side_effect_level, usage_fragment, feature_gate_status, input_contract, output_contract,
owning_phase_item, claim_boundary, fallback_boundary, fallback_attempted,
external_engine_invoked

Claim boundary: command discoverability, metadata consolidation, and scoped public workflow
admission/execution envelopes only. Broad runtime support and public claims remain governed by
`runs-today`, capability discovery, execution certificates, release gates, and benchmark evidence.

No-fallback status: fallback_attempted=false and external_engine_invoked=false.

This page is a status snippet, not a separate hand-maintained command table. The full per-command
rows are generated from the registry through the CLI metadata and capability surfaces above.
