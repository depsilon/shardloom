//! Typed CLI command registry and generated command metadata.
//!
//! This registry is side-effect-free. It does not dispatch or authorize command
//! behavior; it gives help text, agent metadata, and drift tests one command
//! inventory instead of another hand-maintained usage string.

use std::process::ExitCode;

use shardloom_core::{CommandStatus, Diagnostic, OutputFormat, ShardLoomError};

use crate::{
    cli_output::{emit, emit_error},
    command_family::classify_command,
};

const REGISTRY_SCHEMA_VERSION: &str = "shardloom.command_registry.v1";
const SUPPORT_STATE_VOCABULARY: &[&str] = &[
    "executable",
    "feature_gated",
    "diagnostic_only",
    "report_only",
    "blocked",
    "future",
];
const CLAIM_BOUNDARY: &str = "command metadata only; runtime support and public claims remain governed by runs-today, capabilities, certificates, and release gates";
const FALLBACK_BOUNDARY: &str =
    "metadata rendering is side-effect-free and never invokes fallback or external engines";

pub(crate) const REGISTERED_COMMANDS: &[&str] = &[
    "command-metadata",
    "spill-lifecycle",
    "spill-reservation-plan",
    "spill-payload-roundtrip",
    "cleanup-synthetic-payload",
    "status",
    "runs-today",
    "release-plan",
    "package-plan",
    "api-compat-plan",
    "rest-api-contract-plan",
    "rest-api-plan-preview",
    "rest-api-local-lifecycle",
    "rest-api-event-stream",
    "rest-api-security-governance",
    "rest-api-data-plane",
    "serve",
    "agent-contract-pack",
    "python-wrapper-plan",
    "generated-source-user-rows-smoke",
    "generated-source-range-smoke",
    "generated-source-sequence-smoke",
    "generated-source-sql-smoke",
    "sql-local-source-smoke",
    "vortex-ingest-smoke",
    "workflow-unsupported-plan",
    "workload-certification-dossier",
    "claim-gate-closeout",
    "global-architecture-gate",
    "compute-capability-matrix",
    "semantic-conformance-suite",
    "input-adapters",
    "input-plan",
    "vortex-input-plan",
    "vortex-read-plan",
    "vortex-task-graph",
    "schema-plan",
    "catalog-plan",
    "table-compat-plan",
    "capabilities",
    "extension-registry",
    "extension-inspect",
    "udf-runtime-plan",
    "security-plan",
    "security-governance-evidence-gate",
    "effect-budget-plan",
    "agent-safety-plan",
    "redaction-plan",
    "plan-ir",
    "plan-import",
    "plan-export",
    "memory-plan",
    "operator-memory-spill-declarations",
    "cg14-memory-runtime-hardening-gate",
    "spill-plan",
    "correctness-plan",
    "correctness-harness-plan",
    "execution-certificate-plan",
    "kernel-registry",
    "recovery-plan",
    "commit-execution-promotion-gate",
    "fault-tolerance-promotion-gate",
    "cancellation-plan",
    "retry-plan",
    "retry-gate-plan",
    "cancellation-gate-plan",
    "observability-plan",
    "observability-schema-coverage",
    "runtime-report",
    "profile-plan",
    "feature-footprint",
    "doctor",
    "explain",
    "benchmark-plan",
    "benchmark-claim-evidence-plan",
    "manifest-plan",
    "layout-health-plan",
    "compaction-plan",
    "table-intelligence-plan",
    "cg9-catalog-metadata-gate",
    "local-table-metadata-read-smoke",
    "local-delete-tombstone-read-smoke",
    "local-append-only-cdc-overlay-smoke",
    "object-store-request-plan",
    "cg10-object-store-runtime-gate",
    "object-store-range-plan",
    "object-store-coalesce-plan",
    "object-store-schedule-plan",
    "object-store-checkpoint-retry-plan",
    "object-store-commit-plan",
    "object-store-read-smoke",
    "incremental-plan",
    "stateful-reuse-plan",
    "cg17-stateful-reuse-gate",
    "universal-harness-plan",
    "rfc-coverage-followthrough-plan",
    "native-io-envelope-plan",
    "world-class-sufficiency-plan",
    "cg20-user-capability-gate",
    "cg20-approx-sketch-gate",
    "vortex-write-intent-plan",
    "vortex-commit-intent-plan",
    "vortex-manifest-finalization-plan",
    "vortex-output-payload-plan",
    "vortex-finalized-manifest-artifact-write",
    "vortex-output-payload-artifact-write",
    "vortex-native-count-payload-write",
    "vortex-commit-marker-plan",
    "vortex-commit-marker-write",
    "vortex-commit-protocol-plan",
    "vortex-local-commit-execute",
    "vortex-local-commit-recovery-plan",
    "vortex-local-commit-rollback-execute",
    "vortex-staged-workspace-setup",
    "vortex-staged-marker-write",
    "vortex-staged-manifest-file-plan",
    "vortex-staged-manifest-file-write",
    "write-intent",
    "scan-plan",
    "engine-selection-plan",
    "engine-capability-matrix",
    "live-change-contract-plan",
    "live-fixture-run",
    "hybrid-overlay-run",
    "streaming-plan",
    "streaming-batch-plan",
    "backpressure-plan",
    "runtime-plan",
    "sizing-plan",
    "sizing-feedback-plan",
    "dynamic-work-shaping-plan",
    "cg8-runtime-promotion-gate",
    "task-plan",
    "vortex-adaptive-sizing",
    "vortex-memory-plan",
    "vortex-schedule-plan",
    "vortex-execution-readiness",
    "vortex-encoded-path-selection-plan",
    "vortex-generalized-encoded-primitive-gate",
    "vortex-encoded-read-api",
    "vortex-encoded-read-boundary",
    "vortex-encoded-read-metadata-probe",
    "vortex-encoded-read-readiness",
    "vortex-encoded-read-probe",
    "vortex-encoded-read-spike",
    "vortex-encoded-read-execute",
    "vortex-metadata-execute",
    "vortex-dry-run",
    "vortex-plan",
    "translation-plan",
    "vortex-output-plan",
    "vortex-readiness",
    "vortex-dtype-mapping",
    "vortex-encoding-layout-mapping",
    "vortex-statistics-mapping",
    "vortex-file-metadata-open",
    "vortex-metadata-summary",
    "vortex-query-primitive-plan",
    "vortex-metadata-physical-kernel-plan",
    "vortex-count-readiness-plan",
    "vortex-encoded-count-approval-plan",
    "vortex-layout-driver-approval-plan",
    "vortex-filtered-count-readiness-plan",
    "vortex-projection-readiness-plan",
    "traditional-analytics-run",
    "traditional-analytics-vortex-run",
    "traditional-analytics-vortex-batch-run",
    "vortex-count",
    "vortex-count-benchmark",
    "vortex-count-where",
    "vortex-project",
    "vortex-filter-project",
    "vortex-filter",
    "vortex-local-exec",
    "vortex-bounded-local-exec",
    "vortex-run",
    "vortex-query-trace",
    "vortex-metadata-plan",
    "vortex-pruning-plan",
    "vortex-metadata-probe",
    "vortex-api-inventory",
    "optimizer-plan",
    "optimizer-adaptive-memory-plan",
    "cpu-specialization-plan",
    "estimate",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommandDescriptor {
    pub command: &'static str,
}

impl CommandDescriptor {
    #[must_use]
    pub(crate) fn family(self) -> &'static str {
        classify_command(self.command).as_str()
    }

    #[must_use]
    pub(crate) fn support_state(self) -> &'static str {
        command_support_state(self.command)
    }

    #[must_use]
    pub(crate) fn side_effect_level(self) -> &'static str {
        command_side_effect_level(self.command)
    }

    #[must_use]
    pub(crate) fn usage_fragment(self) -> String {
        command_usage_fragment(self.command)
    }
}

pub(crate) fn registered_commands() -> impl Iterator<Item = CommandDescriptor> {
    REGISTERED_COMMANDS
        .iter()
        .copied()
        .map(|command| CommandDescriptor { command })
}

#[must_use]
pub(crate) fn lookup(command: &str) -> Option<CommandDescriptor> {
    REGISTERED_COMMANDS
        .iter()
        .copied()
        .find(|registered| *registered == command)
        .map(|command| CommandDescriptor { command })
}

#[must_use]
pub(crate) fn usage_line(command_name: &str) -> String {
    let fragments = registered_commands()
        .map(CommandDescriptor::usage_fragment)
        .collect::<Vec<_>>()
        .join("|");
    format!("usage: {command_name} <{fragments}> [--format text|json]")
}

pub(crate) fn handle_command_metadata(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let selected = args.next();
    if let Some(extra) = args.next() {
        return emit_error(
            "command-metadata",
            format,
            "unexpected command metadata argument",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected command-metadata argument: {extra}"
            )),
        );
    }
    let selected_descriptor = match selected.as_deref() {
        Some(command) => match lookup(command) {
            Some(descriptor) => Some(descriptor),
            None => {
                return emit_error(
                    "command-metadata",
                    format,
                    "unknown command metadata target",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown command-metadata target: {command}"
                    )),
                );
            }
        },
        None => None,
    };

    let fields = command_metadata_fields(selected_descriptor);
    let text = command_metadata_text(selected_descriptor);
    emit(
        "command-metadata",
        format,
        CommandStatus::Success,
        "command registry metadata rendered without side effects".to_string(),
        text,
        Vec::<Diagnostic>::new(),
        fields,
    );
    ExitCode::SUCCESS
}

fn command_metadata_fields(selected: Option<CommandDescriptor>) -> Vec<(String, String)> {
    let descriptors = registered_commands().collect::<Vec<_>>();
    let mut fields = vec![
        (
            "command_registry_schema_version".to_string(),
            REGISTRY_SCHEMA_VERSION.to_string(),
        ),
        (
            "command_registry_source".to_string(),
            "shardloom-cli/src/command_registry.rs".to_string(),
        ),
        (
            "registered_command_count".to_string(),
            descriptors.len().to_string(),
        ),
        (
            "command_registry_support_state_vocabulary".to_string(),
            SUPPORT_STATE_VOCABULARY.join(","),
        ),
        (
            "registered_commands".to_string(),
            descriptors
                .iter()
                .map(|descriptor| descriptor.command)
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "registered_command_families".to_string(),
            descriptors
                .iter()
                .map(|descriptor| format!("{}={}", descriptor.command, descriptor.family()))
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "registered_command_support_states".to_string(),
            descriptors
                .iter()
                .map(|descriptor| format!("{}={}", descriptor.command, descriptor.support_state()))
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "registered_command_side_effect_levels".to_string(),
            descriptors
                .iter()
                .map(|descriptor| {
                    format!("{}={}", descriptor.command, descriptor.side_effect_level())
                })
                .collect::<Vec<_>>()
                .join(","),
        ),
        ("claim_boundary".to_string(), CLAIM_BOUNDARY.to_string()),
        (
            "fallback_boundary".to_string(),
            FALLBACK_BOUNDARY.to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("external_engine_invoked".to_string(), "false".to_string()),
    ];
    if let Some(descriptor) = selected {
        fields.extend([
            (
                "selected_command".to_string(),
                descriptor.command.to_string(),
            ),
            (
                "selected_command_family".to_string(),
                descriptor.family().to_string(),
            ),
            (
                "selected_command_support_state".to_string(),
                descriptor.support_state().to_string(),
            ),
            (
                "selected_command_side_effect_level".to_string(),
                descriptor.side_effect_level().to_string(),
            ),
            (
                "selected_command_usage_fragment".to_string(),
                descriptor.usage_fragment(),
            ),
        ]);
    }
    fields
}

fn command_metadata_text(selected: Option<CommandDescriptor>) -> String {
    if let Some(descriptor) = selected {
        return format!(
            "{}\nfamily={}\nsupport_state={}\nside_effect_level={}\nusage={}\nclaim_boundary={CLAIM_BOUNDARY}\nfallback_boundary={FALLBACK_BOUNDARY}",
            descriptor.command,
            descriptor.family(),
            descriptor.support_state(),
            descriptor.side_effect_level(),
            descriptor.usage_fragment()
        );
    }

    let commands = registered_commands()
        .map(|descriptor| {
            format!(
                "{} [{}; {}; {}]",
                descriptor.command,
                descriptor.family(),
                descriptor.support_state(),
                descriptor.side_effect_level()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "command_registry_schema_version={REGISTRY_SCHEMA_VERSION}\nregistered_command_count={}\n{commands}\nclaim_boundary={CLAIM_BOUNDARY}\nfallback_boundary={FALLBACK_BOUNDARY}",
        REGISTERED_COMMANDS.len()
    )
}

fn command_usage_fragment(command: &str) -> String {
    match command {
        "capabilities" => format!("{command} [{}]", capability_scopes().join("|")),
        "rest-api-plan-preview" => {
            format!(
                "{command} [certified-local-batch|partial-hybrid-fixture|blocked-remote-object-store|invalid-input|unsupported-operator]"
            )
        }
        "rest-api-local-lifecycle" => {
            format!(
                "{command} [certified-local-batch|cancel-requested|retry-requested|blocked-uncertified]"
            )
        }
        "rest-api-event-stream" => {
            format!(
                "{command} [certified-live-fixture|certified-hybrid-fixture|blocked-production-workload|broker-requested]"
            )
        }
        "rest-api-security-governance" => {
            format!(
                "{command} [safe-local-default|destructive-policy-required|agent-mcp-discovery]"
            )
        }
        "rest-api-data-plane" => {
            format!(
                "{command} [artifact-reference-default|flight-ticket-requested|adbc-endpoint-requested|standards-matrix]"
            )
        }
        "serve" => "serve --mode discovery [--bind host:port]".to_string(),
        "generated-source-user-rows-smoke" => {
            format!("{command} <local-output-path> <schema> <rows>")
        }
        "generated-source-range-smoke" | "generated-source-sequence-smoke" => {
            format!("{command} <local-output-path> <start> <end>")
        }
        "generated-source-sql-smoke" => {
            format!("{command} <local-output-path> <sql-statement>")
        }
        "sql-local-source-smoke" => format!("{command} <sql-statement>"),
        "vortex-ingest-smoke" => format!("{command} <local-source-path> <target.vortex>"),
        "workflow-unsupported-plan" => {
            format!(
                "{command} [{}] [workflow] [target]",
                workflow_profiles().join("|")
            )
        }
        "workload-certification-dossier" => {
            format!(
                "{command} [local-vortex-count|planned-live-hybrid|blocked-remote-api|unsupported-sql]"
            )
        }
        "object-store-read-smoke" => {
            format!(
                "{command} <local-object-path> [--profile local-emulator] [--range offset:length]"
            )
        }
        "engine-selection-plan" => {
            format!(
                "{command} [auto|batch|live|hybrid] [bounded|unbounded|snapshot|unknown] [snapshot|append-only|upsert|delete|retract|tombstone|changelog] [snapshot|append|update|complete|changelog|continuous-view]"
            )
        }
        "live-fixture-run" | "hybrid-overlay-run" => {
            format!(
                "{command} [filter|project|count|count-where|group-count] [predicate|columns|group-column]"
            )
        }
        "dynamic-work-shaping-plan" => {
            format!("{command} [balanced|memory-pressure|object-store-throttled|small-tasks]")
        }
        "benchmark-claim-evidence-plan" => {
            format!("{command} [foundation|traditional-analytics]")
        }
        "table-compat-plan" => {
            format!("{command} [aggregate|partition-evolution|delete-semantics]")
        }
        "retry-gate-plan" | "cancellation-gate-plan" => format!("{command} <signals>"),
        _ => command.to_string(),
    }
}

fn capability_scopes() -> &'static [&'static str] {
    &[
        "sql",
        "functions",
        "operators",
        "adapters",
        "semantic-profiles",
        "migration",
        "certification",
        "data-etl",
        "python",
        "dataframe",
        "notebook",
        "udfs",
        "universal-adapters",
        "event-api-saas-adapters",
        "unstructured-media",
        "api-surfaces",
        "observability",
        "deployment",
        "extensions",
        "security-governance",
        "engines",
        "workflow",
        "remote-api",
        "cross-cg",
        "compatibility",
    ]
}

fn workflow_profiles() -> &'static [&'static str] {
    &[
        "profile",
        "collect",
        "from-pandas",
        "from-arrow-table",
        "from-arrow-ipc",
        "to-pandas",
        "to-arrow",
        "to-arrow-table",
        "to-arrow-ipc",
        "to-numpy",
        "to-python-objects",
        "with-column",
        "group-by",
        "agg",
        "sort",
        "limit",
        "write-vortex",
        "write-parquet",
        "sql",
        "sql-parse",
        "sql-bind",
        "sql-plan",
        "sql-execute",
        "sql-source-free-projection",
        "dataframe-source-free-projection",
        "dataframe-generated-with-column",
        "object-store-generated-output",
        "foundry-generated-output",
        "join",
        "aggregate",
        "window",
        "schema-contract",
        "schema",
        "describe-schema",
        "validate-schema",
        "data-quality",
        "data-quality-summary",
        "quarantine",
        "preview",
        "display",
        "object-store-read",
        "fallback-engine",
    ]
}

fn command_support_state(command: &str) -> &'static str {
    if classify_command(command).as_str() == "diagnostics" {
        "diagnostic_only"
    } else if matches!(
        command,
        "command-metadata" | "status" | "runs-today" | "capabilities"
    ) || command.ends_with("-smoke")
        || command.ends_with("-run")
        || matches!(
            command,
            "doctor"
                | "explain"
                | "estimate"
                | "spill-payload-roundtrip"
                | "cleanup-synthetic-payload"
                | "vortex-encoded-read-spike"
                | "vortex-count"
                | "vortex-count-where"
                | "vortex-project"
                | "vortex-filter"
                | "vortex-filter-project"
                | "vortex-local-exec"
                | "vortex-bounded-local-exec"
                | "vortex-run"
                | "vortex-query-trace"
        )
    {
        "executable"
    } else if command.contains("write") || command.contains("execute") {
        "feature_gated"
    } else {
        "report_only"
    }
}

fn command_side_effect_level(command: &str) -> &'static str {
    if matches!(
        command,
        "command-metadata" | "status" | "runs-today" | "capabilities"
    ) || command.ends_with("-plan")
        || command.ends_with("-gate")
        || command.ends_with("-matrix")
        || command.ends_with("-suite")
        || command.ends_with("-inventory")
        || command.ends_with("-mapping")
        || command.ends_with("-readiness")
    {
        "side_effect_free_metadata_or_report"
    } else if command.contains("write")
        || command.contains("execute")
        || command.ends_with("-smoke")
        || command.ends_with("-run")
        || matches!(
            command,
            "vortex-count"
                | "vortex-count-where"
                | "vortex-project"
                | "vortex-filter"
                | "vortex-filter-project"
                | "vortex-local-exec"
                | "vortex-bounded-local-exec"
                | "vortex-run"
                | "vortex-query-trace"
                | "vortex-encoded-read-spike"
                | "spill-payload-roundtrip"
                | "cleanup-synthetic-payload"
        )
    {
        "local_runtime_or_local_artifact_effect_possible"
    } else {
        "diagnostic_or_metadata_only"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn registry_has_unique_commands_and_metadata_invariants() {
        let mut seen = HashSet::new();
        for descriptor in registered_commands() {
            assert!(seen.insert(descriptor.command), "duplicate command");
            assert_ne!(
                descriptor.family(),
                "other",
                "unclassified command: {}",
                descriptor.command
            );
            assert!(
                SUPPORT_STATE_VOCABULARY.contains(&descriptor.support_state()),
                "unknown support state for command {}: {}",
                descriptor.command,
                descriptor.support_state()
            );
            assert!(!descriptor.usage_fragment().is_empty());
        }
        assert!(seen.contains("command-metadata"));
        assert!(seen.contains("capabilities"));
        assert!(seen.contains("vortex-ingest-smoke"));
        assert!(seen.contains("vortex-local-commit-execute"));
    }

    #[test]
    fn usage_line_is_registry_backed() {
        let usage = usage_line("shardloom");
        assert!(usage.starts_with("usage: shardloom <command-metadata|"));
        assert!(usage.contains("capabilities [sql|functions"));
        assert!(usage.contains("serve --mode discovery"));
        assert!(usage.contains("sql-execute"));
        assert_eq!(usage.matches("-execute").count(), 5);
    }

    #[test]
    fn selected_metadata_fields_are_agent_visible() {
        let descriptor = lookup("vortex-ingest-smoke").expect("registered command");
        let fields = command_metadata_fields(Some(descriptor));
        assert!(fields.contains(&(
            "command_registry_schema_version".to_string(),
            REGISTRY_SCHEMA_VERSION.to_string()
        )));
        assert!(fields.contains(&(
            "selected_command".to_string(),
            "vortex-ingest-smoke".to_string()
        )));
        assert!(fields.contains(&(
            "selected_command_family".to_string(),
            "prepared_source_backed_execution".to_string()
        )));
        assert!(fields.contains(&("fallback_attempted".to_string(), "false".to_string())));
        assert!(fields.contains(&("external_engine_invoked".to_string(), "false".to_string())));
    }

    #[test]
    fn registry_matches_cli_dispatch_table() {
        let source = include_str!("main.rs");
        let (_, after_match) = source
            .split_once("match args.next().as_deref() {")
            .expect("run dispatch match");
        let (dispatch_block, _) = after_match
            .split_once("Some(command) =>")
            .expect("unknown-command dispatch arm");

        let mut dispatch_commands = HashSet::new();
        let mut remaining = dispatch_block;
        while let Some((_, after_marker)) = remaining.split_once("Some(\"") {
            let (command, after_command) = after_marker
                .split_once("\")")
                .expect("command dispatch string terminator");
            assert!(
                dispatch_commands.insert(command),
                "duplicate dispatch command"
            );
            remaining = after_command;
        }

        let registry_commands = REGISTERED_COMMANDS.iter().copied().collect::<HashSet<_>>();
        for command in &dispatch_commands {
            assert!(
                registry_commands.contains(command),
                "dispatch command missing registry entry: {command}"
            );
        }
        for command in &registry_commands {
            assert!(
                dispatch_commands.contains(command),
                "registry command missing dispatch arm: {command}"
            );
        }
    }
}
