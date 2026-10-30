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

pub(crate) const REGISTRY_SCHEMA_VERSION: &str = "shardloom.command_registry.v1";
const SUPPORT_STATE_VOCABULARY: &[&str] = &[
    "executable",
    "feature_gated",
    "diagnostic_only",
    "report_only",
    "blocked",
    "future",
];
const USER_SURFACE_GRADUATION_POSTURE_VOCABULARY: &[&str] = &[
    "high_level_context",
    "public_runtime",
    "client_only",
    "diagnostic_only",
    "feature_gated",
    "not_user_facing",
];
const COMMAND_AUDIENCE_VOCABULARY: &[&str] = &["application", "maintainer"];
const APPLICATION_COMMANDS: &[&str] = &[
    "help",
    "command-metadata",
    "status",
    "run",
    "prepare",
    "route",
    "vortex-prepare",
    "input-adapters",
    "input-plan",
    "capabilities",
    "doctor",
    "support-bundle",
    "explain",
    "estimate",
];
const REGISTRY_REPORT_ID: &str = "review-p1-1.command_registry";
const REGISTRY_SOURCE: &str = "shardloom-cli/src/command_registry.rs";
const REGISTRY_DOCS_REF: &str = "docs/status/cli-command-registry.md";
const CLAIM_BOUNDARY: &str = "command metadata only; runtime support and public claims remain governed by runs-today, capabilities, certificates, and release gates";
const FALLBACK_BOUNDARY: &str =
    "metadata rendering is side-effect-free and never invokes fallback or external engines";
const COMMAND_EVIDENCE_FIELDS: &str = "command|family|support_state|user_surface_graduation_posture|side_effect_level|usage_fragment|feature_gate_status|input_contract|output_contract|owning_phase_item|claim_boundary|fallback_boundary|fallback_attempted|external_engine_invoked";
const HELP_ALIAS_HINT: &str = "shardloom --help; shardloom -h; shardloom <command> --help";

pub(crate) const REGISTERED_COMMANDS: &[&str] = &[
    "help",
    "command-metadata",
    "evidence-schema",
    "route",
    "run",
    "prepare",
    "spill-lifecycle",
    "spill-reservation-plan",
    "spill-payload-roundtrip",
    "cleanup-synthetic-payload",
    "status",
    "runs-today",
    "python-worker",
    "release-plan",
    "package-plan",
    "ci-work-shaping-plan",
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
    "vortex-prepare",
    "sqlite-local-import-export-smoke",
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
    "udf-registry",
    "udf-runtime-plan",
    "udf-local-scalar-fixture-smoke",
    "embedding-vector-local-fixture-smoke",
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
    "pre-oom-memory-guard-smoke",
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
    "support-bundle",
    "explain",
    "benchmark-plan",
    "benchmark-constitution",
    "benchmark-claim-evidence-plan",
    "manifest-plan",
    "layout-health-plan",
    "compaction-plan",
    "table-intelligence-plan",
    "cg9-catalog-metadata-gate",
    "local-table-metadata-read-smoke",
    "iceberg-metadata-read-smoke",
    "delta-log-metadata-read-smoke",
    "hudi-timeline-metadata-read-smoke",
    "local-delete-tombstone-read-smoke",
    "local-append-only-cdc-overlay-smoke",
    "local-table-append-commit-rehearsal-smoke",
    "local-table-commit-recovery-smoke",
    "object-store-request-plan",
    "cg10-object-store-runtime-gate",
    "object-store-range-plan",
    "object-store-coalesce-plan",
    "object-store-schedule-plan",
    "object-store-checkpoint-retry-plan",
    "object-store-commit-plan",
    "object-store-read-smoke",
    "object-store-write-smoke",
    "object-store-write-recovery-smoke",
    "object-store-partition-discovery-smoke",
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
    "live-hybrid-state-transition-smoke",
    "live-hybrid-durable-checkpoint-smoke",
    "distributed-local-fixture-run",
    "session-cache-smoke",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandAudience {
    Application,
    Maintainer,
    All,
}

impl CommandAudience {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Application => "application",
            Self::Maintainer => "maintainer",
            Self::All => "all",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "app" | "application" => Ok(Self::Application),
            "maintainer" => Ok(Self::Maintainer),
            "all" => Ok(Self::All),
            _ => Err(format!(
                "invalid command audience {value:?}; expected application, maintainer, or all"
            )),
        }
    }

    fn includes(self, descriptor: CommandDescriptor) -> bool {
        matches!(self, Self::All) || self.as_str() == descriptor.audience()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionShell {
    Bash,
    Zsh,
    Fish,
}

impl CompletionShell {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "bash" => Ok(Self::Bash),
            "zsh" => Ok(Self::Zsh),
            "fish" => Ok(Self::Fish),
            _ => Err(format!(
                "invalid completion shell {value:?}; expected bash, zsh, or fish"
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Bash => "bash",
            Self::Zsh => "zsh",
            Self::Fish => "fish",
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RegistryArguments {
    selected: Option<String>,
    audience: Option<CommandAudience>,
    completion: Option<CompletionShell>,
}

impl CommandDescriptor {
    #[must_use]
    pub(crate) fn audience(self) -> &'static str {
        if APPLICATION_COMMANDS.contains(&self.command) {
            "application"
        } else {
            "maintainer"
        }
    }

    #[must_use]
    pub(crate) fn family(self) -> &'static str {
        classify_command(self.command).as_str()
    }

    #[must_use]
    pub(crate) fn support_state(self) -> &'static str {
        command_support_state(self.command)
    }

    #[must_use]
    pub(crate) fn user_surface_graduation_posture(self) -> &'static str {
        command_user_surface_graduation_posture(self.command)
    }

    #[must_use]
    pub(crate) fn side_effect_level(self) -> &'static str {
        command_side_effect_level(self.command)
    }

    #[must_use]
    pub(crate) fn usage_fragment(self) -> String {
        command_usage_fragment(self.command)
    }

    #[must_use]
    pub(crate) fn field_id(self) -> String {
        command_field_id(self.command)
    }

    #[must_use]
    pub(crate) fn feature_gate_status(self) -> &'static str {
        command_feature_gate_status(self.command)
    }

    #[must_use]
    pub(crate) fn input_contract(self) -> &'static str {
        command_input_contract(self.command)
    }

    #[must_use]
    pub(crate) fn output_contract(self) -> &'static str {
        command_output_contract(self.command)
    }

    #[must_use]
    pub(crate) fn owning_phase_item(self) -> &'static str {
        command_owning_phase_item(self.command)
    }
}

pub(crate) fn registered_commands() -> impl Iterator<Item = CommandDescriptor> {
    REGISTERED_COMMANDS
        .iter()
        .copied()
        .map(|command| CommandDescriptor { command })
}

fn commands_for_audience(audience: CommandAudience) -> Vec<CommandDescriptor> {
    registered_commands()
        .filter(|descriptor| audience.includes(*descriptor))
        .collect()
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
    usage_line_for_audience(command_name, CommandAudience::All)
}

fn usage_line_for_audience(command_name: &str, audience: CommandAudience) -> String {
    let fragments = commands_for_audience(audience)
        .into_iter()
        .map(CommandDescriptor::usage_fragment)
        .collect::<Vec<_>>()
        .join("|");
    format!("usage: {command_name} <{fragments}> [--format text|json]")
}

fn parse_registry_arguments(
    args: impl Iterator<Item = String>,
    command: &str,
    allow_completion: bool,
) -> Result<RegistryArguments, String> {
    let mut parsed = RegistryArguments::default();
    let mut seen_audience = false;
    let mut seen_completion = false;
    let mut args = args;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--audience" => {
                if seen_audience {
                    return Err("duplicate --audience option".to_string());
                }
                seen_audience = true;
                let Some(value) = args.next() else {
                    return Err("missing value for --audience".to_string());
                };
                if value.starts_with("--") {
                    return Err("missing value for --audience".to_string());
                }
                parsed.audience = Some(CommandAudience::parse(&value)?);
            }
            "--completion" if allow_completion => {
                if seen_completion {
                    return Err("duplicate --completion option".to_string());
                }
                seen_completion = true;
                let Some(value) = args.next() else {
                    return Err("missing value for --completion".to_string());
                };
                if value.starts_with("--") {
                    return Err("missing value for --completion".to_string());
                }
                parsed.completion = Some(CompletionShell::parse(&value)?);
            }
            flag if flag.starts_with("--") => {
                return Err(format!("unknown {command} option: {flag}"));
            }
            _ if parsed.selected.is_some() => {
                return Err(format!("unexpected {command} argument: {argument}"));
            }
            _ => parsed.selected = Some(argument),
        }
    }
    Ok(parsed)
}

pub(crate) fn handle_command_help(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
    command_name: &str,
) -> ExitCode {
    let parsed = match parse_registry_arguments(args, "help", false) {
        Ok(parsed) => parsed,
        Err(message) => {
            return emit_error(
                "help",
                format,
                "invalid help options",
                &ShardLoomError::InvalidOperation(message),
            );
        }
    };
    let audience = parsed.audience.unwrap_or(match format {
        OutputFormat::Text => CommandAudience::Application,
        OutputFormat::Json => CommandAudience::All,
    });
    let selected_descriptor = match parsed.selected.as_deref() {
        Some(command) => match lookup(command) {
            Some(descriptor) => Some(descriptor),
            None => {
                return emit_error(
                    "help",
                    format,
                    "unknown help target",
                    &ShardLoomError::InvalidOperation(format!("unknown help target: {command}")),
                );
            }
        },
        None => None,
    };

    if let (Some(descriptor), Some(explicit_audience)) = (selected_descriptor, parsed.audience)
        && !explicit_audience.includes(descriptor)
    {
        return emit_error(
            "help",
            format,
            "command is outside the selected audience",
            &ShardLoomError::InvalidOperation(format!(
                "help target {:?} belongs to the {} audience, not {}",
                descriptor.command,
                descriptor.audience(),
                explicit_audience.as_str()
            )),
        );
    }

    let mut fields = if audience == CommandAudience::All {
        command_metadata_fields(selected_descriptor)
    } else {
        command_metadata_fields_for_audience(selected_descriptor, audience)
    };
    fields.push((
        "help_scope".to_string(),
        parsed
            .selected
            .unwrap_or_else(|| audience.as_str().to_string()),
    ));
    let usage = if format == OutputFormat::Json {
        usage_line(command_name)
    } else {
        usage_line_for_audience(command_name, audience)
    };
    fields.push(("usage_line".to_string(), usage));
    if let Some(descriptor) = selected_descriptor {
        fields.push((
            "selected_command_help_text".to_string(),
            command_help_text(command_name, descriptor),
        ));
    }
    emit(
        "help",
        format,
        CommandStatus::Success,
        "command help rendered from registry metadata without side effects".to_string(),
        command_help_text_for_selection(command_name, selected_descriptor, audience),
        Vec::<Diagnostic>::new(),
        fields,
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_command_metadata(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let parsed = match parse_registry_arguments(args, "command-metadata", true) {
        Ok(parsed) => parsed,
        Err(message) => {
            return emit_error(
                "command-metadata",
                format,
                "invalid command metadata options",
                &ShardLoomError::InvalidOperation(message),
            );
        }
    };
    let audience = parsed.audience.unwrap_or_else(|| {
        if parsed.completion.is_some() {
            CommandAudience::Application
        } else {
            CommandAudience::All
        }
    });
    let selected_descriptor = match parsed.selected.as_deref() {
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

    if let Some(descriptor) = selected_descriptor
        && !audience.includes(descriptor)
    {
        return emit_error(
            "command-metadata",
            format,
            "command is outside the selected audience",
            &ShardLoomError::InvalidOperation(format!(
                "command-metadata target {:?} belongs to the {} audience, not {}",
                descriptor.command,
                descriptor.audience(),
                audience.as_str()
            )),
        );
    }

    let completion_audience = parsed.audience.unwrap_or(CommandAudience::Application);
    let completion_script = parsed
        .completion
        .map(|shell| (shell, completion_script(shell, completion_audience)));
    let mut fields = if audience == CommandAudience::All {
        command_metadata_fields(selected_descriptor)
    } else {
        command_metadata_fields_for_audience(selected_descriptor, audience)
    };
    if let Some((shell, script)) = &completion_script {
        fields.extend([
            ("completion_shell".to_string(), shell.as_str().to_string()),
            (
                "completion_audience".to_string(),
                completion_audience.as_str().to_string(),
            ),
            ("completion_script".to_string(), script.clone()),
        ]);
    }
    let text = completion_script.map_or_else(
        || command_metadata_text_for_audience(selected_descriptor, audience),
        |(_, script)| script,
    );
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

#[allow(clippy::too_many_lines)]
fn command_metadata_fields(selected: Option<CommandDescriptor>) -> Vec<(String, String)> {
    command_metadata_fields_for_audience(selected, CommandAudience::All)
}

#[allow(clippy::too_many_lines)]
fn command_metadata_fields_for_audience(
    selected: Option<CommandDescriptor>,
    audience: CommandAudience,
) -> Vec<(String, String)> {
    let all_descriptors = registered_commands().collect::<Vec<_>>();
    let descriptors = all_descriptors
        .iter()
        .copied()
        .filter(|descriptor| audience.includes(*descriptor))
        .collect::<Vec<_>>();
    let mut fields = vec![
        (
            "command_registry_schema_version".to_string(),
            REGISTRY_SCHEMA_VERSION.to_string(),
        ),
        (
            "command_registry_report_id".to_string(),
            REGISTRY_REPORT_ID.to_string(),
        ),
        (
            "command_registry_source".to_string(),
            REGISTRY_SOURCE.to_string(),
        ),
        (
            "command_registry_docs_ref".to_string(),
            REGISTRY_DOCS_REF.to_string(),
        ),
        (
            "registered_command_count".to_string(),
            descriptors.len().to_string(),
        ),
        (
            "command_registry_registered_command_count".to_string(),
            all_descriptors.len().to_string(),
        ),
        (
            "command_registry_generated_command_count".to_string(),
            all_descriptors.len().to_string(),
        ),
        (
            "command_registry_filtered_command_count".to_string(),
            descriptors.len().to_string(),
        ),
        (
            "command_registry_selected_audience".to_string(),
            audience.as_str().to_string(),
        ),
        (
            "command_registry_audience_vocabulary".to_string(),
            COMMAND_AUDIENCE_VOCABULARY.join(","),
        ),
        (
            "command_registry_audience_map".to_string(),
            all_descriptors
                .iter()
                .map(|descriptor| format!("{}={}", descriptor.command, descriptor.audience()))
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "command_registry_support_state_vocabulary".to_string(),
            SUPPORT_STATE_VOCABULARY.join(","),
        ),
        (
            "command_registry_user_surface_graduation_posture_vocabulary".to_string(),
            USER_SURFACE_GRADUATION_POSTURE_VOCABULARY.join(","),
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
            "registered_command_user_surface_graduation_postures".to_string(),
            descriptors
                .iter()
                .map(|descriptor| {
                    format!(
                        "{}={}",
                        descriptor.command,
                        descriptor.user_surface_graduation_posture()
                    )
                })
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
        (
            "registered_command_feature_gate_statuses".to_string(),
            descriptors
                .iter()
                .map(|descriptor| {
                    format!(
                        "{}={}",
                        descriptor.command,
                        descriptor.feature_gate_status()
                    )
                })
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "registered_command_input_contracts".to_string(),
            descriptors
                .iter()
                .map(|descriptor| format!("{}={}", descriptor.command, descriptor.input_contract()))
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "registered_command_output_contracts".to_string(),
            descriptors
                .iter()
                .map(|descriptor| {
                    format!("{}={}", descriptor.command, descriptor.output_contract())
                })
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "registered_command_owning_phase_items".to_string(),
            descriptors
                .iter()
                .map(|descriptor| {
                    format!("{}={}", descriptor.command, descriptor.owning_phase_item())
                })
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "command_registry_evidence_fields".to_string(),
            COMMAND_EVIDENCE_FIELDS.to_string(),
        ),
        (
            "command_registry_help_command".to_string(),
            "shardloom help [command] --format json".to_string(),
        ),
        (
            "command_registry_help_aliases".to_string(),
            HELP_ALIAS_HINT.to_string(),
        ),
        (
            "command_registry_metadata_command".to_string(),
            "shardloom command-metadata [command] --format json".to_string(),
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
                "selected_command_audience".to_string(),
                descriptor.audience().to_string(),
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
                "selected_command_user_surface_graduation_posture".to_string(),
                descriptor.user_surface_graduation_posture().to_string(),
            ),
            (
                "selected_command_side_effect_level".to_string(),
                descriptor.side_effect_level().to_string(),
            ),
            (
                "selected_command_usage_fragment".to_string(),
                descriptor.usage_fragment(),
            ),
            (
                "selected_command_feature_gate_status".to_string(),
                descriptor.feature_gate_status().to_string(),
            ),
            (
                "selected_command_input_contract".to_string(),
                descriptor.input_contract().to_string(),
            ),
            (
                "selected_command_output_contract".to_string(),
                descriptor.output_contract().to_string(),
            ),
            (
                "selected_command_evidence_fields".to_string(),
                COMMAND_EVIDENCE_FIELDS.to_string(),
            ),
            (
                "selected_command_owning_phase_item".to_string(),
                descriptor.owning_phase_item().to_string(),
            ),
        ]);
    }
    fields
}

fn command_metadata_text_for_audience(
    selected: Option<CommandDescriptor>,
    audience: CommandAudience,
) -> String {
    if let Some(descriptor) = selected {
        return format!(
            "{}\naudience={}\nfamily={}\nsupport_state={}\nuser_surface_graduation_posture={}\nside_effect_level={}\nusage={}\nfeature_gate_status={}\ninput_contract={}\noutput_contract={}\nowning_phase_item={}\nevidence_fields={COMMAND_EVIDENCE_FIELDS}\nclaim_boundary={CLAIM_BOUNDARY}\nfallback_boundary={FALLBACK_BOUNDARY}",
            descriptor.command,
            descriptor.audience(),
            descriptor.family(),
            descriptor.support_state(),
            descriptor.user_surface_graduation_posture(),
            descriptor.side_effect_level(),
            descriptor.usage_fragment(),
            descriptor.feature_gate_status(),
            descriptor.input_contract(),
            descriptor.output_contract(),
            descriptor.owning_phase_item()
        );
    }

    let descriptors = registered_commands()
        .filter(|descriptor| audience.includes(*descriptor))
        .collect::<Vec<_>>();
    let commands = descriptors
        .iter()
        .copied()
        .map(|descriptor| {
            format!(
                "{} [{}; {}; {}; {}; audience={}]",
                descriptor.command,
                descriptor.family(),
                descriptor.support_state(),
                descriptor.user_surface_graduation_posture(),
                descriptor.side_effect_level(),
                descriptor.audience()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "command_registry_schema_version={REGISTRY_SCHEMA_VERSION}\nregistered_command_count={}\ncommand_registry_registered_command_count={}\ncommand_registry_selected_audience={}\n{commands}\nclaim_boundary={CLAIM_BOUNDARY}\nfallback_boundary={FALLBACK_BOUNDARY}",
        descriptors.len(),
        REGISTERED_COMMANDS.len(),
        audience.as_str()
    )
}

fn completion_script(shell: CompletionShell, audience: CommandAudience) -> String {
    let commands = commands_for_audience(audience)
        .into_iter()
        .map(|descriptor| descriptor.command)
        .collect::<Vec<_>>();
    match shell {
        CompletionShell::Bash => format!(
            "# Generated by shardloom command-metadata; static registry completion.\n_shardloom_commands='{}'\n_shardloom_complete() {{\n  COMPREPLY=( $(compgen -W \"$_shardloom_commands\" -- \"${{COMP_WORDS[COMP_CWORD]}}\") )\n}}\ncomplete -F _shardloom_complete shardloom\n",
            commands.join(" ")
        ),
        CompletionShell::Zsh => format!(
            "#compdef shardloom\n# Generated by shardloom command-metadata; static registry completion.\n_arguments '1:command:({})'\n",
            commands.join(" ")
        ),
        CompletionShell::Fish => {
            commands
                .iter()
                .map(|command| {
                    format!("complete -c shardloom -f -n '__fish_use_subcommand' -a '{command}'")
                })
                .collect::<Vec<_>>()
                .join("\n")
                + "\n"
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn append_command_registry_capability_fields(fields: &mut Vec<(String, String)>) {
    let descriptors = registered_commands().collect::<Vec<_>>();
    fields.extend([
        (
            "command_registry_schema_version".to_string(),
            REGISTRY_SCHEMA_VERSION.to_string(),
        ),
        (
            "command_registry_report_id".to_string(),
            REGISTRY_REPORT_ID.to_string(),
        ),
        (
            "command_registry_docs_ref".to_string(),
            REGISTRY_DOCS_REF.to_string(),
        ),
        (
            "command_registry_source".to_string(),
            REGISTRY_SOURCE.to_string(),
        ),
        (
            "command_registry_metadata_command".to_string(),
            "shardloom command-metadata [command] --format json".to_string(),
        ),
        (
            "command_registry_help_command".to_string(),
            "shardloom help [command] --format json".to_string(),
        ),
        (
            "command_registry_help_aliases".to_string(),
            HELP_ALIAS_HINT.to_string(),
        ),
        (
            "command_registry_registered_command_count".to_string(),
            descriptors.len().to_string(),
        ),
        (
            "command_registry_support_state_vocabulary".to_string(),
            SUPPORT_STATE_VOCABULARY.join(","),
        ),
        (
            "command_registry_user_surface_graduation_posture_vocabulary".to_string(),
            USER_SURFACE_GRADUATION_POSTURE_VOCABULARY.join(","),
        ),
        (
            "command_registry_row_order".to_string(),
            descriptors
                .iter()
                .map(|descriptor| descriptor.command)
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "command_registry_family_order".to_string(),
            unique_descriptor_values(&descriptors, CommandDescriptor::family).join(","),
        ),
        (
            "command_registry_executable_count".to_string(),
            support_state_count(&descriptors, "executable").to_string(),
        ),
        (
            "command_registry_feature_gated_count".to_string(),
            support_state_count(&descriptors, "feature_gated").to_string(),
        ),
        (
            "command_registry_diagnostic_only_count".to_string(),
            support_state_count(&descriptors, "diagnostic_only").to_string(),
        ),
        (
            "command_registry_report_only_count".to_string(),
            support_state_count(&descriptors, "report_only").to_string(),
        ),
        (
            "command_registry_blocked_count".to_string(),
            support_state_count(&descriptors, "blocked").to_string(),
        ),
        (
            "command_registry_future_count".to_string(),
            support_state_count(&descriptors, "future").to_string(),
        ),
        (
            "command_registry_high_level_context_count".to_string(),
            user_surface_graduation_posture_count(&descriptors, "high_level_context").to_string(),
        ),
        (
            "command_registry_public_runtime_count".to_string(),
            user_surface_graduation_posture_count(&descriptors, "public_runtime").to_string(),
        ),
        (
            "command_registry_client_only_count".to_string(),
            user_surface_graduation_posture_count(&descriptors, "client_only").to_string(),
        ),
        (
            "command_registry_diagnostic_graduation_count".to_string(),
            user_surface_graduation_posture_count(&descriptors, "diagnostic_only").to_string(),
        ),
        (
            "command_registry_feature_gated_graduation_count".to_string(),
            user_surface_graduation_posture_count(&descriptors, "feature_gated").to_string(),
        ),
        (
            "command_registry_not_user_facing_count".to_string(),
            user_surface_graduation_posture_count(&descriptors, "not_user_facing").to_string(),
        ),
        (
            "command_registry_evidence_fields".to_string(),
            COMMAND_EVIDENCE_FIELDS.to_string(),
        ),
        (
            "command_registry_claim_boundary".to_string(),
            CLAIM_BOUNDARY.to_string(),
        ),
        (
            "command_registry_fallback_boundary".to_string(),
            FALLBACK_BOUNDARY.to_string(),
        ),
        (
            "command_registry_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "command_registry_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
        (
            "command_registry_all_commands_have_usage_fragment".to_string(),
            "true".to_string(),
        ),
        (
            "command_registry_all_commands_classified".to_string(),
            "true".to_string(),
        ),
        (
            "command_registry_claim_gate_status".to_string(),
            "metadata_only_not_claim_grade".to_string(),
        ),
    ]);
    for descriptor in descriptors {
        let prefix = format!("command_registry_row_{}", descriptor.field_id());
        fields.extend([
            (format!("{prefix}_command"), descriptor.command.to_string()),
            (format!("{prefix}_family"), descriptor.family().to_string()),
            (
                format!("{prefix}_support_state"),
                descriptor.support_state().to_string(),
            ),
            (
                format!("{prefix}_user_surface_graduation_posture"),
                descriptor.user_surface_graduation_posture().to_string(),
            ),
            (
                format!("{prefix}_side_effect_level"),
                descriptor.side_effect_level().to_string(),
            ),
            (
                format!("{prefix}_usage_fragment"),
                descriptor.usage_fragment(),
            ),
            (
                format!("{prefix}_feature_gate_status"),
                descriptor.feature_gate_status().to_string(),
            ),
            (
                format!("{prefix}_input_contract"),
                descriptor.input_contract().to_string(),
            ),
            (
                format!("{prefix}_output_contract"),
                descriptor.output_contract().to_string(),
            ),
            (
                format!("{prefix}_evidence_fields"),
                COMMAND_EVIDENCE_FIELDS.to_string(),
            ),
            (
                format!("{prefix}_owning_phase_item"),
                descriptor.owning_phase_item().to_string(),
            ),
            (format!("{prefix}_fallback_attempted"), "false".to_string()),
            (
                format!("{prefix}_external_engine_invoked"),
                "false".to_string(),
            ),
        ]);
    }
}

#[allow(clippy::too_many_lines)]
fn command_usage_fragment(command: &str) -> String {
    let mut usage = match command {
        "help" => "help [--audience application|maintainer|all] [command]".to_string(),
        "command-metadata" => "command-metadata [--audience application|maintainer|all] [--completion bash|zsh|fish] [command]".to_string(),
        "evidence-schema" => "evidence-schema [surface]".to_string(),
        "route" => "route <sql|python|dataframe|cli> [--input <uri>] [--input-format <format>] [--sql <statement>] [--plan <summary>] [--request <collect|write_vortex|write_parquet|write_arrow_ipc|write_avro|write_orc|write_csv|write_json|write_jsonl|explain|route|evidence>] [--recursion-limit <positive-integer>]".to_string(),
        "run" => "run <sql|python|dataframe|cli> [--input <uri>] [--input-format <format>] [--sql <statement>] [--plan <summary>] [--request <collect|write_vortex|write_parquet|write_arrow_ipc|write_avro|write_orc|write_csv|write_json|write_jsonl>] [--output <ref>] [--recursion-limit <positive-integer>]".to_string(),
        "prepare" => "prepare <sql|python|dataframe|cli> --input <uri> [--input-format <format>] --output <target.vortex>".to_string(),
        "python-worker" => "python-worker".to_string(),
        "capabilities" => format!("{command} [{}]", capability_scopes().join("|")),
        "support-bundle" => format!("{command} [--note <redacted-text>] [--include-defaults]"),
        "rest-api-plan-preview" => format!("{command} [certified-local-batch|partial-hybrid-fixture|blocked-remote-object-store|invalid-input|unsupported-operator]"),
        "rest-api-local-lifecycle" => format!("{command} [certified-local-batch|certified-live-fixture|certified-hybrid-fixture|cancel-requested|retry-requested|blocked-uncertified]"),
        "rest-api-event-stream" => format!("{command} [certified-live-fixture|certified-hybrid-fixture|blocked-production-workload|broker-requested]"),
        "rest-api-security-governance" => format!("{command} [safe-local-default|destructive-policy-required|agent-mcp-discovery]"),
        "rest-api-data-plane" => format!("{command} [artifact-reference-default|flight-ticket-requested|adbc-endpoint-requested|standards-matrix]"),
        "serve" => "serve --mode discovery [--bind host:port]".to_string(),
        "vortex-prepare" => {
            format!("{command} <local-source-path> <target.vortex> [--input-format csv|json|jsonl|parquet|arrow-ipc|avro|orc|vortex]")
        }
        "sqlite-local-import-export-smoke" => {
            format!(
                "{command} <db.sqlite> --table <table> --export-jsonl <path> --roundtrip-db <path> [--order-by <column>] [--allow-overwrite]"
            )
        }
        "udf-local-scalar-fixture-smoke" => format!("{command} <comma-separated-int64-or-null>"),
        "embedding-vector-local-fixture-smoke" => {
            format!("{command} <semicolon-separated-texts> [--query <text>]")
        }
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
        "object-store-write-smoke" => {
            format!(
                "{command} <local-source-path> <local-object-path> [--profile local-emulator] [--idempotency-key key] [--allow-overwrite] [--rollback-after-commit]"
            )
        }
        "object-store-write-recovery-smoke" => {
            format!(
                "{command} <local-object-path> [--profile local-emulator] [--idempotency-key key]"
            )
        }
        "object-store-partition-discovery-smoke" => {
            format!(
                "{command} <local-partition-root> [--profile local-emulator] [--partition-columns a,b]"
            )
        }
        "local-table-append-commit-rehearsal-smoke" => {
            format!(
                "{command} <local-committed-manifest-path> [--profile local-manifest] [--idempotency-key key] [--expected-current-manifest-digest digest] [--allow-overwrite] [--rollback-after-commit]"
            )
        }
        "local-table-commit-recovery-smoke" => {
            format!(
                "{command} <local-committed-manifest-path> [--profile local-manifest] [--idempotency-key key]"
            )
        }
        "iceberg-metadata-read-smoke" => {
            format!(
                "{command} <metadata-json-path> [--snapshot-id id|--as-of-timestamp-ms ms] [--manifest-list local.avro] [--manifest local.avro] [--execute-data-file-scan]"
            )
        }
        "delta-log-metadata-read-smoke" => format!("{command} <delta-log-json-path>"),
        "hudi-timeline-metadata-read-smoke" => {
            format!("{command} <timeline-dir> [--metadata-json local.json]")
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
        "live-hybrid-durable-checkpoint-smoke" => {
            format!("{command} <checkpoint-dir>")
        }
        "distributed-local-fixture-run" => {
            format!("{command} [worker-count] [none|fault-injection]")
        }
        "dynamic-work-shaping-plan" => {
            format!(
                "{command} [balanced|memory-pressure|object-store-throttled|small-tasks|repeated-independent-shards]"
            )
        }
        "ci-work-shaping-plan" => {
            format!(
                "{command} [--mode pull_request|merge|release] [--changed-path <path>...] [--changed-paths-file <file>]"
            )
        }
        "benchmark-constitution" | "benchmark-claim-evidence-plan" => {
            format!("{command} [foundation|traditional-analytics]")
        }
        "table-compat-plan" => {
            format!("{command} [aggregate|partition-evolution|delete-semantics]")
        }
        "retry-gate-plan" | "cancellation-gate-plan" => format!("{command} <signals>"),
        _ => command.to_string(),
    };
    if matches!(
        command,
        "run"
            | "prepare"
            | "vortex-prepare"
            | "vortex-run"
            | "vortex-count"
            | "vortex-file-metadata-open"
            | "vortex-bounded-local-exec"
            | "sqlite-local-import-export-smoke"
            | "udf-local-scalar-fixture-smoke"
            | "embedding-vector-local-fixture-smoke"
            | "object-store-read-smoke"
            | "object-store-write-smoke"
            | "object-store-write-recovery-smoke"
            | "object-store-partition-discovery-smoke"
            | "local-table-metadata-read-smoke"
            | "local-table-append-commit-rehearsal-smoke"
            | "local-table-commit-recovery-smoke"
            | "iceberg-metadata-read-smoke"
            | "delta-log-metadata-read-smoke"
            | "hudi-timeline-metadata-read-smoke"
            | "local-delete-tombstone-read-smoke"
            | "local-append-only-cdc-overlay-smoke"
            | "live-fixture-run"
            | "hybrid-overlay-run"
            | "live-hybrid-state-transition-smoke"
            | "live-hybrid-durable-checkpoint-smoke"
            | "distributed-local-fixture-run"
            | "spill-payload-roundtrip"
    ) {
        usage.push_str(" (--resources-from-env | (--memory-gb <GiB> | --memory-bytes <bytes>) --max-parallelism <n>)");
    }
    usage
}

fn command_field_id(command: &str) -> String {
    command.replace('-', "_")
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
    if classify_command(command).as_str() == "diagnostics"
        || matches!(
            command,
            "help"
                | "command-metadata"
                | "evidence-schema"
                | "status"
                | "runs-today"
                | "capabilities"
        )
    {
        "diagnostic_only"
    } else if matches!(
        command,
        "doctor"
            | "support-bundle"
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
            | "vortex-prepare"
            | "python-worker"
            | "run"
            | "prepare"
    ) || command.ends_with("-smoke")
        || command.ends_with("-run")
    {
        "executable"
    } else if command.contains("write") || command.contains("execute") {
        "feature_gated"
    } else {
        "report_only"
    }
}

fn command_user_surface_graduation_posture(command: &str) -> &'static str {
    if command == "python-worker" {
        return "not_user_facing";
    }
    if command == "vortex-prepare" {
        return "public_runtime";
    }
    if is_high_level_context_command(command) {
        return "high_level_context";
    }
    match command_support_state(command) {
        "feature_gated" => "feature_gated",
        "diagnostic_only" | "report_only" | "blocked" => "diagnostic_only",
        "future" => "not_user_facing",
        _ => "client_only",
    }
}

fn is_high_level_context_command(command: &str) -> bool {
    matches!(command, "route" | "run" | "prepare")
}

fn command_side_effect_level(command: &str) -> &'static str {
    if matches!(
        command,
        "help"
            | "command-metadata"
            | "evidence-schema"
            | "route"
            | "status"
            | "runs-today"
            | "capabilities"
            | "doctor"
            | "support-bundle"
            | "benchmark-constitution"
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
            "vortex-prepare"
                | "python-worker"
                | "vortex-count"
                | "vortex-count-where"
                | "vortex-project"
                | "vortex-filter"
                | "vortex-filter-project"
                | "vortex-local-exec"
                | "vortex-bounded-local-exec"
                | "vortex-run"
                | "vortex-query-trace"
                | "run"
                | "prepare"
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

fn command_feature_gate_status(command: &str) -> &'static str {
    match command_support_state(command) {
        "feature_gated" => "explicit_feature_gate_or_runtime_gate_required",
        "blocked" => "blocked",
        "future" => "future",
        _ => "not_required_for_metadata",
    }
}

fn command_input_contract(command: &str) -> &'static str {
    if command == "python-worker" {
        return "newline_delimited_json_transport_requests_with_args_array";
    }
    if command == "iceberg-metadata-read-smoke" {
        return "local_iceberg_table_metadata_json_path_with_optional_snapshot_selector";
    }
    if command == "delta-log-metadata-read-smoke" {
        return "local_delta_transaction_log_json_path";
    }
    if command == "hudi-timeline-metadata-read-smoke" {
        return "local_hudi_timeline_directory_with_optional_metadata_table_summary";
    }
    if command == "session-cache-smoke" {
        return "scoped_cli_session_cache_lifecycle_smoke";
    }
    if command == "ci-work-shaping-plan" {
        return "newline_changed_path_manifest_or_repeated_changed_path_args";
    }
    if command == "live-hybrid-durable-checkpoint-smoke" {
        return "explicit_local_filesystem_checkpoint_directory";
    }
    if command == "route" {
        return "declared_public_workflow_route_request";
    }
    if matches!(command, "run" | "prepare") {
        return "declared_public_workflow_request_with_route_admission";
    }
    match classify_command(command).as_str() {
        "status_capabilities" => "registry_or_capability_scope_args",
        "prepared_source_backed_execution" => "local_source_or_vortex_artifact_args",
        "vortex_primitive_execution" => "local_vortex_artifact_and_operator_args",
        "vortex_output_commit" => "local_workspace_commit_or_write_signals",
        "object_store_planning" => "declared_object_store_request_shape",
        "rest_api_planning" => "declared_remote_api_contract_or_lifecycle_signals",
        "workflow_planning" => "workflow_or_local_artifact_args",
        "input_planning" => "declared_input_source_or_adapter_shape",
        "engine_runtime_planning" => "declared_runtime_or_engine_mode_signals",
        "vortex_runtime_planning" => "declared_vortex_runtime_signals",
        "vortex_planning" => "declared_vortex_file_or_planning_signals",
        "benchmarks" => "declared_benchmark_or_local_runtime_args",
        "packaging_deployment" => "release_or_package_readiness_scope",
        "operational_hardening" => "declared_safety_memory_spill_or_recovery_signals",
        "diagnostics" => "diagnostic_or_estimate_args",
        "evidence_certificates" => "evidence_or_certificate_scope_args",
        "optimizer_planning" => "declared_optimizer_or_kernel_scope",
        "extension_planning" => "extension_manifest_or_udf_scope_args",
        _ => "command_specific_args",
    }
}

fn command_output_contract(command: &str) -> &'static str {
    if command == "python-worker" {
        return "one_typed_json_envelope_per_request_no_runtime_route_changes";
    }
    if command == "iceberg-metadata-read-smoke" {
        return "typed_envelope_plus_scoped_iceberg_metadata_snapshot_selection_and_no_fallback_evidence";
    }
    if command == "delta-log-metadata-read-smoke" {
        return "typed_envelope_plus_scoped_delta_log_metadata_action_summary_and_no_fallback_evidence";
    }
    if command == "hudi-timeline-metadata-read-smoke" {
        return "typed_envelope_plus_scoped_hudi_timeline_metadata_summary_and_no_fallback_evidence";
    }
    if command == "session-cache-smoke" {
        return "typed_envelope_plus_session_cache_reuse_invalidation_and_cleanup_evidence";
    }
    if command == "ci-work-shaping-plan" {
        return "typed_envelope_plus_metadata_first_ci_work_shaping_plan_and_no_fallback_evidence";
    }
    if command == "live-hybrid-durable-checkpoint-smoke" {
        return "typed_envelope_plus_local_checkpoint_changelog_restore_and_no_fallback_evidence";
    }
    if command == "route" {
        return "typed_public_workflow_route_envelope_no_execution";
    }
    if matches!(command, "run" | "prepare") {
        return "typed_runtime_envelope_with_attached_public_workflow_route";
    }
    if command.ends_with("-smoke")
        || command.ends_with("-run")
        || command.contains("write")
        || command.contains("execute")
        || matches!(
            command,
            "vortex-prepare"
                | "python-worker"
                | "vortex-count"
                | "vortex-count-where"
                | "vortex-project"
                | "vortex-filter"
                | "vortex-filter-project"
                | "vortex-local-exec"
                | "vortex-bounded-local-exec"
                | "vortex-run"
                | "vortex-query-trace"
                | "spill-payload-roundtrip"
                | "cleanup-synthetic-payload"
        )
    {
        "typed_envelope_plus_local_runtime_or_artifact_evidence"
    } else {
        "typed_envelope_metadata_report_only"
    }
}

fn command_owning_phase_item(command: &str) -> &'static str {
    if command == "iceberg-metadata-read-smoke" {
        return "PROD-READY-1C";
    }
    if matches!(
        command,
        "delta-log-metadata-read-smoke" | "hudi-timeline-metadata-read-smoke"
    ) {
        return "PROD-READY-1C";
    }
    if command == "evidence-schema" {
        return "REVIEW-P1-2";
    }
    if command == "python-worker" {
        return "PY-RUNTIME-OVERHEAD-1";
    }
    if command == "route" {
        return "GAR-RUNTIME-IMPL-6D:public_workflow_route_facade";
    }
    if matches!(command, "run" | "prepare") {
        return "GAR-RUNTIME-IMPL-6D:public_workflow_route_facade";
    }
    if command == "session-cache-smoke" {
        return "GAR-RUNTIME-IMPL-4L/GAR-RUNTIME-IMPL-5I";
    }
    if command == "ci-work-shaping-plan" {
        return "CI-WORK-SHAPING-1";
    }
    if matches!(
        command,
        "sqlite-local-import-export-smoke"
            | "udf-registry"
            | "udf-local-scalar-fixture-smoke"
            | "embedding-vector-local-fixture-smoke"
    ) {
        return "GAR-RUNTIME-IMPL-4R/GAR-RUNTIME-IMPL-5O";
    }
    match classify_command(command).as_str() {
        "status_capabilities" => "REVIEW-P1-1",
        "prepared_source_backed_execution" | "vortex_primitive_execution" | "vortex_planning" => {
            "GAR-RUNTIME-IMPL-4"
        }
        "vortex_output_commit" => "CG-3",
        "object_store_planning" => "CG-10",
        "rest_api_planning" => "CG-23",
        "workflow_planning" => "CG-21",
        "input_planning" => "CG-20",
        "engine_runtime_planning" => "CG-8-CG-22",
        "vortex_runtime_planning" => "CG-8",
        "benchmarks" => "CG-6",
        "packaging_deployment" => "REVIEW-P0-4",
        "operational_hardening" => "CG-14-CG-17",
        "diagnostics" => "RFC-0012",
        "evidence_certificates" => "CG-5-CG-16",
        "optimizer_planning" => "CG-7-CG-8",
        "extension_planning" => "RFC-0023",
        _ => "unassigned",
    }
}

fn unique_descriptor_values(
    descriptors: &[CommandDescriptor],
    accessor: fn(CommandDescriptor) -> &'static str,
) -> Vec<&'static str> {
    let mut values = Vec::new();
    for descriptor in descriptors {
        let value = accessor(*descriptor);
        if !values.contains(&value) {
            values.push(value);
        }
    }
    values
}

fn support_state_count(descriptors: &[CommandDescriptor], support_state: &str) -> usize {
    descriptors
        .iter()
        .filter(|descriptor| descriptor.support_state() == support_state)
        .count()
}

fn user_surface_graduation_posture_count(
    descriptors: &[CommandDescriptor],
    posture: &str,
) -> usize {
    descriptors
        .iter()
        .filter(|descriptor| descriptor.user_surface_graduation_posture() == posture)
        .count()
}

fn command_help_text_for_selection(
    command_name: &str,
    selected: Option<CommandDescriptor>,
    audience: CommandAudience,
) -> String {
    selected.map_or_else(
        || {
            let commands = commands_for_audience(audience)
                .iter()
                .map(|descriptor| {
                    format!(
                        "  {} — {}",
                        descriptor.command,
                        command_description(descriptor.command)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                "usage: {command_name} <command> [options] [--format text|json]\n\n{commands}\n\nUse '{command_name} help <command>' or '{command_name} <command> --help' for command-specific metadata. Use '{command_name} command-metadata --audience all' for the complete registry, or '{command_name} command-metadata [command] --format json' for agent-readable registry output."
            )
        },
        |descriptor| command_help_text(command_name, descriptor),
    )
}

fn command_description(command: &str) -> &'static str {
    match command {
        "help" => "Show command help",
        "command-metadata" => "Inspect command metadata or generate completion",
        "status" => "Show engine status",
        "run" => "Execute a query or data workflow",
        "prepare" => "Load or reuse source data in a Vortex file",
        "route" => "Inspect how a query or data workflow would run",
        "vortex-prepare" => "Load a local input into a Vortex file",
        "input-adapters" => "Inspect input formats and reader availability",
        "input-plan" => "Inspect input requirements",
        "capabilities" => "Inspect installed capabilities",
        "doctor" => "Check installation/configuration",
        "support-bundle" => "Collect redacted support details",
        "explain" => "Explain an operation",
        "estimate" => "Estimate an operation",
        _ => classify_command(command).as_str(),
    }
}

fn command_help_text(command_name: &str, descriptor: CommandDescriptor) -> String {
    format!(
        "{}\n\nusage: {command_name} {} [--format text|json]\n\nCommand details:\ncommand: {}\naudience: {}\nfamily: {}\nsupport_state: {}\nside_effect_level: {}\nfeature_gate_status: {}\ninput_contract: {}\noutput_contract: {}\nowning_phase_item: {}\nevidence_fields: {COMMAND_EVIDENCE_FIELDS}\nclaim_boundary: {CLAIM_BOUNDARY}\nfallback_boundary: {FALLBACK_BOUNDARY}",
        command_description(descriptor.command),
        descriptor.usage_fragment(),
        descriptor.command,
        descriptor.audience(),
        descriptor.family(),
        descriptor.support_state(),
        descriptor.side_effect_level(),
        descriptor.feature_gate_status(),
        descriptor.input_contract(),
        descriptor.output_contract(),
        descriptor.owning_phase_item(),
    )
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
            assert_ne!(descriptor.usage_fragment(), "");
        }
        assert!(seen.contains("help"));
        assert!(seen.contains("command-metadata"));
        assert!(seen.contains("evidence-schema"));
        assert!(seen.contains("route"));
        assert!(seen.contains("capabilities"));
        assert!(seen.contains("support-bundle"));
        assert!(seen.contains("vortex-prepare"));
        assert!(seen.contains("vortex-local-commit-execute"));
    }

    #[test]
    fn command_audiences_partition_registry_without_losing_commands() {
        let application = commands_for_audience(CommandAudience::Application);
        let maintainer = commands_for_audience(CommandAudience::Maintainer);
        let all = commands_for_audience(CommandAudience::All);
        assert_eq!(application.len() + maintainer.len(), all.len());
        assert_eq!(all.len(), REGISTERED_COMMANDS.len());
        assert!(
            application
                .iter()
                .all(|command| command.audience() == "application")
        );
        assert!(
            maintainer
                .iter()
                .all(|command| command.audience() == "maintainer")
        );
        let partitioned = application
            .iter()
            .chain(&maintainer)
            .map(|command| command.command)
            .collect::<HashSet<_>>();
        assert_eq!(
            partitioned,
            all.iter().map(|command| command.command).collect()
        );
        assert_eq!(
            CommandAudience::parse("app"),
            Ok(CommandAudience::Application)
        );
        let mut allowlisted = HashSet::new();
        for command in APPLICATION_COMMANDS {
            assert!(
                allowlisted.insert(command),
                "duplicate application command: {command}"
            );
            assert!(
                lookup(command).is_some(),
                "unregistered application command: {command}"
            );
        }
    }

    #[test]
    fn registry_argument_parser_rejects_duplicate_unknown_and_missing_options() {
        let duplicate = parse_registry_arguments(
            ["--audience", "application", "--audience", "all"]
                .map(str::to_string)
                .into_iter(),
            "help",
            false,
        );
        assert!(duplicate.unwrap_err().contains("duplicate --audience"));

        let missing = parse_registry_arguments(
            ["--audience"].map(str::to_string).into_iter(),
            "help",
            false,
        );
        assert!(
            missing
                .unwrap_err()
                .contains("missing value for --audience")
        );

        let unknown = parse_registry_arguments(
            ["--bogus"].map(str::to_string).into_iter(),
            "command-metadata",
            true,
        );
        assert!(
            unknown
                .unwrap_err()
                .contains("unknown command-metadata option")
        );

        let invalid = parse_registry_arguments(
            ["--audience", "secret"].map(str::to_string).into_iter(),
            "help",
            false,
        );
        assert!(invalid.unwrap_err().contains("invalid command audience"));

        let completion_on_help = parse_registry_arguments(
            ["--completion", "bash"].map(str::to_string).into_iter(),
            "help",
            false,
        );
        assert!(
            completion_on_help
                .unwrap_err()
                .contains("unknown help option")
        );

        let duplicate_completion = parse_registry_arguments(
            ["--completion", "fish", "--completion", "zsh"]
                .map(str::to_string)
                .into_iter(),
            "command-metadata",
            true,
        );
        assert!(
            duplicate_completion
                .unwrap_err()
                .contains("duplicate --completion")
        );

        let missing_completion = parse_registry_arguments(
            ["--completion"].map(str::to_string).into_iter(),
            "command-metadata",
            true,
        );
        assert!(
            missing_completion
                .unwrap_err()
                .contains("missing value for --completion")
        );

        let invalid_completion = parse_registry_arguments(
            ["--completion", "powershell"]
                .map(str::to_string)
                .into_iter(),
            "command-metadata",
            true,
        );
        assert!(
            invalid_completion
                .unwrap_err()
                .contains("invalid completion shell")
        );

        let extra_argument = parse_registry_arguments(
            ["help", "status"].map(str::to_string).into_iter(),
            "help",
            false,
        );
        assert!(
            extra_argument
                .unwrap_err()
                .contains("unexpected help argument")
        );
    }

    #[test]
    fn completion_defaults_to_application_and_is_static_and_side_effect_free() {
        let parsed = parse_registry_arguments(
            ["--completion", "bash"].map(str::to_string).into_iter(),
            "command-metadata",
            true,
        )
        .expect("valid completion args");
        let audience = parsed.audience.unwrap_or_else(|| {
            if parsed.completion.is_some() {
                CommandAudience::Application
            } else {
                CommandAudience::All
            }
        });
        assert_eq!(audience, CommandAudience::Application);
        let fields = command_metadata_fields_for_audience(None, audience);
        assert!(fields.contains(&("fallback_attempted".into(), "false".into())));
        assert!(fields.contains(&("external_engine_invoked".into(), "false".into())));
        assert!(!fields.iter().any(|(name, _)| name == "completion_audience"));
        let generated = completion_script(CompletionShell::Bash, audience);
        let vocabulary = generated
            .lines()
            .find_map(|line| line.strip_prefix("_shardloom_commands='"))
            .and_then(|line| line.strip_suffix('\''))
            .expect("static Bash completion vocabulary");
        for command in ["help", "command-metadata", "status"] {
            assert!(vocabulary.split_whitespace().any(|word| word == command));
        }
        assert!(!generated.contains("spill-lifecycle"));
    }

    #[test]
    fn unfiltered_metadata_keeps_all_existing_commands_and_adds_audience_evidence() {
        let fields = command_metadata_fields(None);
        assert!(fields.contains(&(
            "registered_command_count".to_string(),
            REGISTERED_COMMANDS.len().to_string()
        )));
        assert!(fields.contains(&(
            "registered_commands".to_string(),
            REGISTERED_COMMANDS.join(",")
        )));
        assert!(fields.contains(&(
            "command_registry_selected_audience".to_string(),
            "all".to_string()
        )));
    }

    #[test]
    fn selected_command_metadata_reports_audience_and_filtering_is_enforced() {
        let descriptor = lookup("vortex-prepare").expect("registered application command");
        let fields =
            command_metadata_fields_for_audience(Some(descriptor), CommandAudience::Application);
        assert!(fields.contains(&("selected_command_audience".into(), "application".into())));
        assert!(fields.contains(&("selected_command".into(), "vortex-prepare".into())));
        assert!(
            !CommandAudience::Application
                .includes(lookup("spill-lifecycle").expect("registered maintainer command"))
        );
    }

    #[test]
    fn completion_scripts_cover_bash_zsh_and_fish() {
        let bash = completion_script(CompletionShell::Bash, CommandAudience::Application);
        let zsh = completion_script(CompletionShell::Zsh, CommandAudience::Maintainer);
        let fish = completion_script(CompletionShell::Fish, CommandAudience::All);
        assert!(bash.contains("complete -F _shardloom_complete shardloom"));
        assert!(bash.contains("vortex-prepare"));
        assert!(!bash.contains("spill-lifecycle"));
        assert!(zsh.starts_with("#compdef shardloom"));
        assert!(zsh.contains("spill-lifecycle"));
        assert!(!zsh.contains(" vortex-prepare"));
        assert!(
            fish.contains(
                "complete -c shardloom -f -n '__fish_use_subcommand' -a 'vortex-prepare'"
            )
        );
        assert!(
            fish.contains(
                "complete -c shardloom -f -n '__fish_use_subcommand' -a 'spill-lifecycle'"
            )
        );
    }

    #[test]
    fn usage_line_is_registry_backed() {
        let usage = usage_line("shardloom");
        assert!(usage.starts_with("usage: shardloom <help [--audience application|maintainer|all] [command]|command-metadata [--audience application|maintainer|all]"));
        assert!(usage.contains("route <sql|python|dataframe|cli>"));
        assert!(usage.contains("--recursion-limit <positive-integer>"));
        assert!(usage.contains("evidence-schema [surface]"));
        assert!(usage.contains("capabilities [sql|functions"));
        assert!(usage.contains("serve --mode discovery"));
        assert!(usage.contains("sql-execute"));
        let execute_command_count = usage
            .trim_start_matches("usage: shardloom <")
            .trim_end_matches('>')
            .split('|')
            .filter_map(|fragment| fragment.split_whitespace().next())
            .filter(|command| command.ends_with("-execute"))
            .count();
        assert_eq!(execute_command_count, 5);
    }

    #[test]
    fn help_honors_explicit_audience_for_a_selected_command() {
        let arguments = ["--audience", "application", "spill-lifecycle"];
        assert_eq!(
            handle_command_help(
                arguments.map(str::to_string).into_iter(),
                OutputFormat::Json,
                "shardloom"
            ),
            ExitCode::from(2)
        );
        assert_eq!(
            handle_command_help(
                ["spill-lifecycle"].map(str::to_string).into_iter(),
                OutputFormat::Text,
                "shardloom"
            ),
            ExitCode::SUCCESS,
            "explicit command lookup remains compatible without an audience filter"
        );
    }

    #[test]
    fn selected_metadata_fields_are_agent_visible() {
        let descriptor = lookup("vortex-prepare").expect("registered command");
        let fields = command_metadata_fields(Some(descriptor));
        assert!(fields.contains(&(
            "command_registry_schema_version".to_string(),
            REGISTRY_SCHEMA_VERSION.to_string()
        )));
        assert!(fields.contains(&("selected_command".to_string(), "vortex-prepare".to_string())));
        assert!(fields.contains(&(
            "selected_command_family".to_string(),
            "prepared_source_backed_execution".to_string()
        )));
        assert!(fields.contains(&(
            "selected_command_feature_gate_status".to_string(),
            "not_required_for_metadata".to_string()
        )));
        assert!(fields.contains(&(
            "selected_command_input_contract".to_string(),
            "local_source_or_vortex_artifact_args".to_string()
        )));
        assert!(fields.contains(&(
            "selected_command_output_contract".to_string(),
            "typed_envelope_plus_local_runtime_or_artifact_evidence".to_string()
        )));
        assert!(fields.contains(&(
            "selected_command_owning_phase_item".to_string(),
            "GAR-RUNTIME-IMPL-4".to_string()
        )));
        assert!(fields.contains(&("fallback_attempted".to_string(), "false".to_string())));
        assert!(fields.contains(&("external_engine_invoked".to_string(), "false".to_string())));
    }

    #[test]
    fn help_text_is_registry_backed_and_command_specific() {
        let descriptor = lookup("vortex-prepare").expect("registered command");
        let help = command_help_text("shardloom", descriptor);
        assert!(help.starts_with("Load a local input into a Vortex file\n\nusage:"));
        assert!(
            help.contains("usage: shardloom vortex-prepare <local-source-path> <target.vortex>")
        );
        assert!(help.contains("csv|json|jsonl|parquet|arrow-ipc|avro|orc|vortex"));
        assert!(help.contains("support_state: executable"));
        assert!(help.contains("owning_phase_item: GAR-RUNTIME-IMPL-4"));
        assert!(help.contains("fallback_boundary: metadata rendering is side-effect-free"));
    }

    #[test]
    fn default_human_help_lists_application_commands_as_readable_lines() {
        let help = command_help_text_for_selection("shardloom", None, CommandAudience::Application);
        for command in APPLICATION_COMMANDS {
            let line_prefix = format!("  {command} — ");
            assert_eq!(
                help.lines()
                    .filter(|line| line.starts_with(&line_prefix))
                    .count(),
                1,
                "expected one concise help line for {command}"
            );
        }
        assert!(!help.contains("  spill-lifecycle — "));
        assert!(!help.contains("--input"));
        assert!(!help.contains("--sql"));

        let selected = command_help_text_for_selection(
            "shardloom",
            lookup("vortex-prepare"),
            CommandAudience::Application,
        );
        assert!(
            selected
                .contains("usage: shardloom vortex-prepare <local-source-path> <target.vortex>")
        );
    }

    #[test]
    fn metadata_commands_are_diagnostic_only() {
        for command in [
            "help",
            "command-metadata",
            "evidence-schema",
            "status",
            "runs-today",
            "capabilities",
            "support-bundle",
        ] {
            assert_eq!(
                command_support_state(command),
                "diagnostic_only",
                "{command}"
            );
            assert_eq!(
                command_user_surface_graduation_posture(command),
                "diagnostic_only",
                "{command}"
            );
        }
    }

    #[test]
    fn capability_fields_are_registry_generated() {
        let mut fields = Vec::new();
        append_command_registry_capability_fields(&mut fields);
        assert!(fields.contains(&(
            "command_registry_registered_command_count".to_string(),
            REGISTERED_COMMANDS.len().to_string()
        )));
        assert!(fields.contains(&(
            "command_registry_row_vortex_prepare_command".to_string(),
            "vortex-prepare".to_string()
        )));
        assert!(fields.contains(&(
            "command_registry_row_vortex_prepare_owning_phase_item".to_string(),
            "GAR-RUNTIME-IMPL-4".to_string()
        )));
        assert!(fields.contains(&(
            "command_registry_fallback_attempted".to_string(),
            "false".to_string()
        )));
        assert!(fields.contains(&(
            "command_registry_external_engine_invoked".to_string(),
            "false".to_string()
        )));
    }

    #[test]
    fn docs_status_snippet_tracks_registry_summary() {
        let docs = include_str!("../../docs/status/cli-command-registry.md");
        assert!(docs.contains(REGISTRY_SCHEMA_VERSION));
        assert!(docs.contains(REGISTRY_SOURCE));
        assert!(docs.contains("shardloom command-metadata [command] --format json"));
        assert!(docs.contains("shardloom help [command] --format json"));
        assert!(docs.contains(HELP_ALIAS_HINT));
        assert!(docs.contains(&format!(
            "Registered command count: {}",
            REGISTERED_COMMANDS.len()
        )));
        assert!(docs.contains("Support-state vocabulary: executable, feature_gated, diagnostic_only, report_only, blocked, future"));
        assert!(docs.contains("fallback_attempted=false"));
        assert!(docs.contains("external_engine_invoked=false"));
    }

    #[test]
    fn registry_matches_cli_dispatch_table() {
        let source = include_str!("lib.rs");
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
