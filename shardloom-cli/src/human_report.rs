//! Human labels for existing public workflow observations.
//!
//! This renderer performs no runtime work. Missing, contradictory, or planned
//! observations cannot establish completed preparation, execution, or output.

use shardloom_core::{CommandStatus, Diagnostic, DiagnosticSeverity};

pub(crate) fn workflow_text(
    command: &str,
    status: CommandStatus,
    summary: &str,
    details: String,
    diagnostics: &[Diagnostic],
    fields: &[(String, String)],
) -> String {
    if field(fields, "public_workflow_route_schema_version").is_none() {
        return details;
    }
    let primary = diagnostics
        .iter()
        .position(|item| item.severity == DiagnosticSeverity::Fatal)
        .or_else(|| {
            diagnostics
                .iter()
                .position(|item| item.severity == DiagnosticSeverity::Error)
        });
    let failed = status.is_error() || primary.is_some();
    let plan_only = command == "route" || field(fields, "plan_only") == Some("true");
    let mut sections = Vec::new();
    let details_are_problem = failed && primary.is_none() && !details.trim().is_empty();
    if let Some(index) = primary {
        sections.push(diagnostics[index].to_human_text());
    } else if failed {
        sections.push(if details_are_problem {
            details.clone()
        } else {
            summary.to_string()
        });
    }
    let input = first_field(
        fields,
        &[
            "public_workflow_preparation_source_format",
            "public_workflow_source_format",
            "source_format",
            "input_format",
        ],
    )
    .map_or("Not reported", format_label);
    sections.push(format!(
        "Input: {input}\nSource preparation: {}\nExecution: {}\n{}",
        preparation_label(fields, plan_only),
        execution_label(command, fields, plan_only, failed),
        output_label(command, fields, plan_only, failed),
    ));
    for (index, diagnostic) in diagnostics.iter().enumerate() {
        if Some(index) != primary {
            sections.push(diagnostic.to_human_text());
        }
    }
    if !details.trim().is_empty() && !details_are_problem {
        // Preserve handler-specific values and explanations. Raw identifiers
        // also remain in the unchanged structured fields.
        sections.push(format!("Execution details:\n{details}"));
    }
    sections.join("\n\n")
}

fn preparation_label(fields: &[(String, String)], plan_only: bool) -> &'static str {
    if plan_only {
        return match consistent_field(
            fields,
            &[
                "public_workflow_preparation_included",
                "preparation_included",
            ],
        ) {
            Some("true") => "Planned; not performed",
            Some("false") => "Not planned",
            _ => "Not reported",
        };
    }
    let reused = consistent_field(
        fields,
        &[
            "public_workflow_preparation_prepared_state_reused",
            "prepared_state_reused",
        ],
    ) == Some("true");
    let created = consistent_field(
        fields,
        &[
            "public_workflow_preparation_prepared_state_created",
            "prepared_state_created",
        ],
    ) == Some("true");
    if reused && created {
        return "Not reported";
    }
    if reused {
        return "Reused";
    }
    if created {
        return "Performed";
    }
    // preparation_included is only a plan property. In particular, a false
    // reuse flag does not establish that no source preparation occurred.
    "Not reported"
}

fn execution_label(
    command: &str,
    fields: &[(String, String)],
    plan_only: bool,
    failed: bool,
) -> &'static str {
    if plan_only {
        return "Not performed (query plan)";
    }
    if field(fields, "execution") == Some("blocked_before_execution") {
        return "Not performed (unsupported request)";
    }
    if failed {
        return "Not completed";
    }
    if execution_not_performed(fields) {
        return "Not performed";
    }
    if field(fields, "runtime_execution") == Some("true") {
        return if command == "prepare" {
            "ShardLoom (source preparation)"
        } else {
            "ShardLoom"
        };
    }
    if result_reported(fields) {
        // Metadata can supply a result without reading column data. Do not
        // infer whether a query executed from data_read alone.
        return "ShardLoom";
    }
    "Not reported"
}

fn output_label(
    command: &str,
    fields: &[(String, String)],
    plan_only: bool,
    failed: bool,
) -> String {
    if !plan_only && !failed && !execution_not_performed(fields) {
        if field(fields, "native_vortex_result_export_all_targets_committed") == Some("true")
            && let Some(formats) = first_field(
                fields,
                &[
                    "native_vortex_result_export_target_formats",
                    "native_vortex_result_export_format",
                ],
            )
        {
            return format!(
                "Output: {}",
                formats
                    .split(',')
                    .map(str::trim)
                    .map(format_label)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if command == "prepare"
            && field(fields, "runtime_execution") == Some("true")
            && field(fields, "public_workflow_prepared_vortex_path").is_some()
        {
            return "Output: Reusable Vortex data".to_string();
        }
        if result_reported(fields) {
            return "Output: Result".to_string();
        }
    }
    match first_field(
        fields,
        &["public_workflow_requested_output", "requested_output"],
    ) {
        Some(requested) => {
            let label = match requested {
                "collect" => "Collected result",
                "prepare" => "Reusable Vortex data",
                value => format_label(value.strip_prefix("write_").unwrap_or(value)),
            };
            format!("Requested output: {label}")
        }
        None => "Output: Not reported".to_string(),
    }
}

fn result_reported(fields: &[(String, String)]) -> bool {
    field(fields, "result_known") == Some("true")
        || field(fields, "result_payload_complete") == Some("true")
}

fn execution_not_performed(fields: &[(String, String)]) -> bool {
    // An explicit nonexecution observation takes precedence over a result
    // flag, including when duplicate observations contradict each other.
    fields.iter().any(|(key, value)| {
        (key == "runtime_execution" && value == "false")
            || (key == "execution"
                && matches!(value.as_str(), "not_performed" | "blocked_before_execution"))
    })
}

fn format_label(format: &str) -> &str {
    match format {
        "vortex" | "vtx" => "Vortex",
        "parquet" => "Parquet",
        "arrow-ipc" | "arrow_ipc" | "ipc" | "feather" => "Arrow IPC",
        "avro" => "Avro",
        "orc" => "ORC",
        "csv" => "CSV",
        "json" => "JSON",
        "jsonl" | "ndjson" => "JSON Lines",
        value => value,
    }
}

fn first_field<'a>(fields: &'a [(String, String)], names: &[&str]) -> Option<&'a str> {
    names.iter().find_map(|name| field(fields, name))
}

fn field<'a>(fields: &'a [(String, String)], name: &str) -> Option<&'a str> {
    consistent_field(fields, &[name])
}

fn consistent_field<'a>(fields: &'a [(String, String)], names: &[&str]) -> Option<&'a str> {
    let mut values = fields
        .iter()
        .filter(|(key, _)| names.contains(&key.as_str()))
        .map(|(_, value)| value.as_str());
    let first = values.next()?;
    if !values.all(|value| value == first)
        || matches!(
            first,
            "" | "none"
                | "unknown"
                | "not_declared"
                | "not_reported"
                | "not_observed"
                | "not_instrumented"
                | "unavailable"
                | "not_applicable"
        )
    {
        return None;
    }
    Some(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::DiagnosticCode;

    fn fields(values: &[(&str, &str)]) -> Vec<(String, String)> {
        std::iter::once((
            "public_workflow_route_schema_version",
            "shardloom.public_workflow_route.v1",
        ))
        .chain(values.iter().copied())
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
    }

    fn render(command: &str, values: &[(&str, &str)]) -> String {
        workflow_text(
            command,
            CommandStatus::Success,
            "complete",
            String::new(),
            &[],
            &fields(values),
        )
    }

    #[test]
    fn planned_preparation_and_output_are_not_reported_as_performed() {
        let text = render(
            "route",
            &[
                ("source_format", "parquet"),
                ("preparation_included", "true"),
                ("requested_output", "write_orc"),
                ("runtime_execution", "false"),
                ("execution", "not_performed"),
            ],
        );
        assert_eq!(
            text,
            "Input: Parquet\nSource preparation: Planned; not performed\nExecution: Not performed (query plan)\nRequested output: ORC"
        );
        let unknown = render(
            "run",
            &[
                ("public_workflow_preparation_included", "true"),
                ("prepared_state_reused", "false"),
                ("data_read", "false"),
            ],
        );
        assert_eq!(
            unknown,
            "Input: Not reported\nSource preparation: Not reported\nExecution: Not reported\nOutput: Not reported"
        );
    }

    #[test]
    fn metadata_result_is_execution_even_without_column_reads() {
        let text = render(
            "run",
            &[
                ("public_workflow_source_format", "vortex"),
                ("execution", "metadata_only_or_not_performed"),
                ("data_read", "false"),
                ("result_known", "true"),
            ],
        );
        assert!(text.contains("Input: Vortex"));
        assert!(text.contains("Execution: ShardLoom\nOutput: Result"));
    }

    #[test]
    fn explicit_nonexecution_overrides_contradictory_completion_flags() {
        for observation in [
            ("runtime_execution", "false"),
            ("execution", "not_performed"),
        ] {
            let text = render(
                "run",
                &[
                    observation,
                    ("runtime_execution", "true"),
                    ("result_known", "true"),
                    ("result_payload_complete", "true"),
                    ("native_vortex_result_export_all_targets_committed", "true"),
                    ("native_vortex_result_export_format", "orc"),
                    ("public_workflow_requested_output", "write_orc"),
                ],
            );
            assert!(text.contains("Execution: Not performed\nRequested output: ORC"));
        }
        let planned = render(
            "route",
            &[
                ("runtime_execution", "true"),
                ("prepared_state_reused", "true"),
                ("result_known", "true"),
                ("native_vortex_result_export_all_targets_committed", "true"),
                ("native_vortex_result_export_format", "orc"),
                ("public_workflow_requested_output", "write_orc"),
            ],
        );
        assert!(planned.contains("Source preparation: Not reported\nExecution: Not performed (query plan)\nRequested output: ORC"));
    }

    #[test]
    fn reused_and_created_source_preparation_are_distinct() {
        for (key, label) in [
            ("prepared_state_reused", "Reused"),
            ("prepared_state_created", "Performed"),
        ] {
            let values = fields(&[
                ("public_workflow_source_format", "not_declared"),
                ("public_workflow_preparation_source_format", "csv"),
                ("runtime_execution", "true"),
                (
                    "public_workflow_prepared_vortex_path",
                    "/data/loaded.vortex",
                ),
            ]);
            let mut values = values;
            values.push((
                format!("public_workflow_preparation_{key}"),
                "true".to_string(),
            ));
            let original = values.clone();
            let text = workflow_text(
                "prepare",
                CommandStatus::Success,
                "complete",
                String::new(),
                &[],
                &values,
            );
            assert!(text.contains(&format!("Input: CSV\nSource preparation: {label}")));
            assert!(text.contains(
                "Execution: ShardLoom (source preparation)\nOutput: Reusable Vortex data"
            ));
            assert_eq!(values, original);
        }
    }

    #[test]
    fn requested_write_becomes_output_only_with_commit_evidence() {
        let mut values = fields(&[
            ("public_workflow_requested_output", "write_orc"),
            ("runtime_execution", "true"),
            ("native_vortex_result_export_format", "orc"),
        ]);
        let before = workflow_text(
            "run",
            CommandStatus::Success,
            "complete",
            String::new(),
            &[],
            &values,
        );
        assert!(before.ends_with("Requested output: ORC"));
        values.push((
            "native_vortex_result_export_all_targets_committed".into(),
            "true".into(),
        ));
        values.push((
            "native_vortex_result_export_target_formats".into(),
            "orc,parquet".into(),
        ));
        let after = workflow_text(
            "run",
            CommandStatus::Success,
            "complete",
            String::new(),
            &[],
            &values,
        );
        assert!(after.ends_with("Output: ORC, Parquet"));
        let failed = workflow_text(
            "run",
            CommandStatus::Error,
            "Write did not complete.",
            String::new(),
            &[],
            &values,
        );
        assert!(failed.starts_with("Write did not complete."));
        assert!(failed.ends_with("Requested output: ORC"));
        assert!(failed.contains("Execution: Not completed"));
    }

    #[test]
    fn error_precedes_warning_and_keeps_values_and_classifications() {
        let warning = Diagnostic::metadata_loss_warning(
            "writer",
            "Metadata may be lost.",
            "Inspect the output schema.",
        );
        let error = Diagnostic::unsupported(
            DiagnosticCode::UnsupportedOutputFormat,
            "writer",
            "This writer does not support the requested type.",
            Some("Choose a writer that supports the type.".into()),
        );
        let diagnostics = vec![warning, error];
        let values = fields(&[
            ("execution", "blocked_before_execution"),
            ("public_workflow_source_format", "parquet"),
            ("public_workflow_requested_output", "write_orc"),
        ]);
        let original = (values.clone(), diagnostics.clone());
        let text = workflow_text(
            "run",
            CommandStatus::Unsupported,
            "blocked",
            "result_count: 0".into(),
            &diagnostics,
            &values,
        );
        assert!(text.starts_with("This writer does not support the requested type."));
        assert!(
            text.find("Choose a writer").unwrap()
                < text.find("SL_UNSUPPORTED_OUTPUT_FORMAT").unwrap()
        );
        assert!(text.contains("Execution: Not performed (unsupported request)"));
        assert!(text.contains("Metadata may be lost."));
        assert!(text.ends_with("Execution details:\nresult_count: 0"));
        assert_eq!((values, diagnostics), original);
    }

    #[test]
    fn unscoped_reports_and_conflicting_observations_are_not_reinterpreted() {
        assert_eq!(
            workflow_text(
                "status",
                CommandStatus::Success,
                "ready",
                "original body".into(),
                &[],
                &[]
            ),
            "original body"
        );
        let text = render(
            "run",
            &[
                ("result_known", "true"),
                ("result_known", "false"),
                ("public_workflow_source_format", "not_declared"),
            ],
        );
        assert!(text.contains("Input: Not reported"));
        assert!(text.contains("Execution: Not reported"));
        let preparation = render(
            "prepare",
            &[
                ("prepared_state_reused", "true"),
                ("prepared_state_created", "true"),
            ],
        );
        assert!(preparation.contains("Source preparation: Not reported"));
        let conflicting_alias = render(
            "prepare",
            &[
                ("public_workflow_preparation_prepared_state_reused", "true"),
                ("prepared_state_reused", "false"),
            ],
        );
        assert!(conflicting_alias.contains("Source preparation: Not reported"));
    }
}
