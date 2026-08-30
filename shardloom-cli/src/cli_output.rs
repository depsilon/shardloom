//! Shared CLI output rendering for typed command/result envelopes.
//!
//! This module centralizes the renderer used by command handlers. It attaches
//! command-family lifecycle metadata and routes command fields through the
//! typed-envelope field/ref classifier without changing command behavior,
//! executing runtime work, probing datasets, or weakening no-fallback policy.

use std::{
    io::{self, ErrorKind, Write},
    process::ExitCode,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use shardloom_core::{CommandStatus, Diagnostic, OutputEnvelope, OutputFormat, ShardLoomError};

use crate::{command_family::classify_command, typed_envelope::apply_typed_envelope_fields};

static OUTPUT_EMISSION_COUNT: AtomicU64 = AtomicU64::new(0);
const EMIT_TIMING_PLACEHOLDER: &str = "__shardloom_json_envelope_emit_micros__";
const ENVELOPE_BUILD_TIMING_PLACEHOLDER: &str = "__shardloom_json_envelope_build_micros__";
const TYPED_FIELD_CLASSIFICATION_TIMING_PLACEHOLDER: &str =
    "__shardloom_json_envelope_typed_field_classification_micros__";
const RENDER_TIMING_PLACEHOLDER: &str = "__shardloom_json_envelope_render_micros__";
const BUILD_RENDER_TIMING_PLACEHOLDER: &str = "__shardloom_json_envelope_build_render_micros__";
const PLACEHOLDER_REPLACE_TIMING_PLACEHOLDER: &str =
    "__shardloom_json_envelope_placeholder_replace_micros__";

pub(crate) fn output_emission_count() -> u64 {
    OUTPUT_EMISSION_COUNT.load(Ordering::Relaxed)
}

fn base_envelope_from_fields(
    command: &str,
    status: CommandStatus,
    summary: String,
    text: String,
    diagnostics: Vec<Diagnostic>,
) -> OutputEnvelope {
    let mut envelope = OutputEnvelope::new(command, status, summary, text)
        .with_lifecycle_field("command_family", classify_command(command).as_str());
    for diagnostic in diagnostics {
        envelope.add_diagnostic(diagnostic);
    }
    envelope
}

fn envelope_from_fields(
    command: &str,
    status: CommandStatus,
    summary: String,
    text: String,
    diagnostics: Vec<Diagnostic>,
    fields: Vec<(String, String)>,
) -> OutputEnvelope {
    let envelope = base_envelope_from_fields(command, status, summary, text, diagnostics);
    apply_typed_envelope_fields(envelope, command, fields)
}

fn envelope_from_fields_timed(
    command: &str,
    status: CommandStatus,
    summary: String,
    text: String,
    diagnostics: Vec<Diagnostic>,
    fields: Vec<(String, String)>,
) -> (OutputEnvelope, u128, u128) {
    let build_start = Instant::now();
    let envelope = base_envelope_from_fields(command, status, summary, text, diagnostics);
    let envelope_build_micros = build_start.elapsed().as_micros();
    let classification_start = Instant::now();
    let envelope = apply_typed_envelope_fields(envelope, command, fields);
    let typed_field_classification_micros = classification_start.elapsed().as_micros();
    (
        envelope,
        envelope_build_micros,
        typed_field_classification_micros,
    )
}

pub(crate) fn emit(
    command: &str,
    format: OutputFormat,
    status: CommandStatus,
    summary: String,
    text: String,
    diagnostics: Vec<Diagnostic>,
    fields: Vec<(String, String)>,
) {
    let envelope = envelope_from_fields(command, status, summary, text, diagnostics, fields);
    write_stdout_line(&envelope.render(format));
}

pub(crate) fn emit_timed(
    command: &str,
    format: OutputFormat,
    status: CommandStatus,
    summary: String,
    text: String,
    diagnostics: Vec<Diagnostic>,
    mut fields: Vec<(String, String)>,
) {
    fields.push((
        "json_envelope_emit_micros".to_string(),
        EMIT_TIMING_PLACEHOLDER.to_string(),
    ));
    fields.push((
        "json_envelope_build_micros".to_string(),
        ENVELOPE_BUILD_TIMING_PLACEHOLDER.to_string(),
    ));
    fields.push((
        "json_envelope_typed_field_classification_micros".to_string(),
        TYPED_FIELD_CLASSIFICATION_TIMING_PLACEHOLDER.to_string(),
    ));
    fields.push((
        "json_envelope_render_micros".to_string(),
        RENDER_TIMING_PLACEHOLDER.to_string(),
    ));
    fields.push((
        "json_envelope_build_render_micros".to_string(),
        BUILD_RENDER_TIMING_PLACEHOLDER.to_string(),
    ));
    fields.push((
        "json_envelope_placeholder_replace_micros".to_string(),
        PLACEHOLDER_REPLACE_TIMING_PLACEHOLDER.to_string(),
    ));
    fields.push((
        "json_envelope_emit_timing_status".to_string(),
        "build_render_placeholder_split_excludes_stdout_write".to_string(),
    ));
    fields.push((
        "json_envelope_emit_timing_scope".to_string(),
        "base_envelope_build_typed_field_classification_render_plus_first_placeholder_substitution_excludes_field_vector_push_and_stdout_write"
            .to_string(),
    ));
    fields.push((
        "json_envelope_stdout_write_timing_status".to_string(),
        "not_measured_in_same_envelope_to_avoid_second_output_or_hot_path_perturbation".to_string(),
    ));
    let (envelope, envelope_build_micros, typed_field_classification_micros) =
        envelope_from_fields_timed(command, status, summary, text, diagnostics, fields);
    let render_start = Instant::now();
    let rendered = envelope.render(format);
    let render_micros = render_start.elapsed().as_micros();
    let build_render_micros = envelope_build_micros
        .saturating_add(typed_field_classification_micros)
        .saturating_add(render_micros);
    let build_render_micros_text = build_render_micros.to_string();
    let replace_start = Instant::now();
    let rendered = rendered.replace(
        BUILD_RENDER_TIMING_PLACEHOLDER,
        build_render_micros_text.as_str(),
    );
    let placeholder_replace_micros = replace_start.elapsed().as_micros();
    let emit_micros = build_render_micros.saturating_add(placeholder_replace_micros);
    let values = EmitTimingReplacementValues {
        emit: emit_micros.to_string(),
        envelope_build: envelope_build_micros.to_string(),
        typed_field_classification: typed_field_classification_micros.to_string(),
        render: render_micros.to_string(),
        build_render: build_render_micros_text,
        placeholder_replace: placeholder_replace_micros.to_string(),
    };
    let rendered = replace_emit_timing_placeholders(&rendered, &values);
    write_stdout_line(&rendered);
}

struct EmitTimingReplacementValues {
    emit: String,
    envelope_build: String,
    typed_field_classification: String,
    render: String,
    build_render: String,
    placeholder_replace: String,
}

fn replace_emit_timing_placeholders(
    rendered: &str,
    values: &EmitTimingReplacementValues,
) -> String {
    rendered
        .replace(EMIT_TIMING_PLACEHOLDER, values.emit.as_str())
        .replace(
            ENVELOPE_BUILD_TIMING_PLACEHOLDER,
            values.envelope_build.as_str(),
        )
        .replace(
            TYPED_FIELD_CLASSIFICATION_TIMING_PLACEHOLDER,
            values.typed_field_classification.as_str(),
        )
        .replace(RENDER_TIMING_PLACEHOLDER, values.render.as_str())
        .replace(
            BUILD_RENDER_TIMING_PLACEHOLDER,
            values.build_render.as_str(),
        )
        .replace(
            PLACEHOLDER_REPLACE_TIMING_PLACEHOLDER,
            values.placeholder_replace.as_str(),
        )
}

pub(crate) fn emit_error(
    command: &str,
    format: OutputFormat,
    summary: &str,
    error: &ShardLoomError,
) -> ExitCode {
    let envelope = OutputEnvelope::from_error(command, summary, error)
        .with_lifecycle_field("command_family", classify_command(command).as_str());
    match format {
        OutputFormat::Text => eprintln!("{}", envelope.to_text()),
        OutputFormat::Json => write_stdout_line(&envelope.to_json()),
    }
    ExitCode::from(2)
}

pub(crate) fn emit_error_with_fields(
    command: &str,
    format: OutputFormat,
    summary: &str,
    error: &ShardLoomError,
    fields: Vec<(String, String)>,
) -> ExitCode {
    let envelope = OutputEnvelope::from_error(command, summary, error)
        .with_lifecycle_field("command_family", classify_command(command).as_str());
    let envelope = apply_typed_envelope_fields(envelope, command, fields);
    match format {
        OutputFormat::Text => eprintln!("{}", envelope.to_text()),
        OutputFormat::Json => write_stdout_line(&envelope.to_json()),
    }
    ExitCode::from(2)
}

fn write_stdout_line(rendered: &str) {
    let mut stdout = io::stdout().lock();
    if let Err(error) = writeln!(stdout, "{rendered}") {
        if error.kind() == ErrorKind::BrokenPipe {
            return;
        }
        eprintln!("failed writing ShardLoom CLI output: {error}");
        std::process::exit(1);
    }
    OUTPUT_EMISSION_COUNT.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::{
        BUILD_RENDER_TIMING_PLACEHOLDER, EMIT_TIMING_PLACEHOLDER,
        ENVELOPE_BUILD_TIMING_PLACEHOLDER, EmitTimingReplacementValues,
        PLACEHOLDER_REPLACE_TIMING_PLACEHOLDER, RENDER_TIMING_PLACEHOLDER,
        TYPED_FIELD_CLASSIFICATION_TIMING_PLACEHOLDER, replace_emit_timing_placeholders,
    };

    fn timing_values() -> EmitTimingReplacementValues {
        EmitTimingReplacementValues {
            emit: "49".to_string(),
            envelope_build: "4".to_string(),
            typed_field_classification: "5".to_string(),
            render: "40".to_string(),
            build_render: "45".to_string(),
            placeholder_replace: "4".to_string(),
        }
    }

    #[test]
    fn json_emit_timing_placeholders_are_filled_after_render() {
        let rendered = format!(
            r#"{{"fields":[{{"key":"json_envelope_emit_micros","value":"{EMIT_TIMING_PLACEHOLDER}"}},{{"key":"json_envelope_build_micros","value":"{ENVELOPE_BUILD_TIMING_PLACEHOLDER}"}},{{"key":"json_envelope_typed_field_classification_micros","value":"{TYPED_FIELD_CLASSIFICATION_TIMING_PLACEHOLDER}"}},{{"key":"json_envelope_render_micros","value":"{RENDER_TIMING_PLACEHOLDER}"}},{{"key":"json_envelope_build_render_micros","value":"{BUILD_RENDER_TIMING_PLACEHOLDER}"}},{{"key":"json_envelope_placeholder_replace_micros","value":"{PLACEHOLDER_REPLACE_TIMING_PLACEHOLDER}"}}]}}"#
        );

        let values = timing_values();
        let updated = replace_emit_timing_placeholders(&rendered, &values);

        assert!(updated.contains(r#""value":"49""#));
        assert!(updated.contains(r#""value":"4""#));
        assert!(updated.contains(r#""value":"5""#));
        assert!(updated.contains(r#""value":"40""#));
        assert!(updated.contains(r#""value":"45""#));
        assert!(!updated.contains(EMIT_TIMING_PLACEHOLDER));
        assert!(!updated.contains(ENVELOPE_BUILD_TIMING_PLACEHOLDER));
        assert!(!updated.contains(TYPED_FIELD_CLASSIFICATION_TIMING_PLACEHOLDER));
        assert!(!updated.contains(RENDER_TIMING_PLACEHOLDER));
        assert!(!updated.contains(BUILD_RENDER_TIMING_PLACEHOLDER));
        assert!(!updated.contains(PLACEHOLDER_REPLACE_TIMING_PLACEHOLDER));
    }

    #[test]
    fn text_emit_timing_placeholders_are_not_exposed() {
        let rendered = format!(
            "json_envelope_emit_micros={EMIT_TIMING_PLACEHOLDER}\njson_envelope_build_micros={ENVELOPE_BUILD_TIMING_PLACEHOLDER}\njson_envelope_typed_field_classification_micros={TYPED_FIELD_CLASSIFICATION_TIMING_PLACEHOLDER}\njson_envelope_render_micros={RENDER_TIMING_PLACEHOLDER}\njson_envelope_build_render_micros={BUILD_RENDER_TIMING_PLACEHOLDER}\njson_envelope_placeholder_replace_micros={PLACEHOLDER_REPLACE_TIMING_PLACEHOLDER}",
        );

        let values = timing_values();
        let updated = replace_emit_timing_placeholders(&rendered, &values);

        assert!(updated.contains("json_envelope_emit_micros=49"));
        assert!(updated.contains("json_envelope_build_micros=4"));
        assert!(updated.contains("json_envelope_typed_field_classification_micros=5"));
        assert!(updated.contains("json_envelope_render_micros=40"));
        assert!(updated.contains("json_envelope_build_render_micros=45"));
        assert!(updated.contains("json_envelope_placeholder_replace_micros=4"));
        assert!(!updated.contains(EMIT_TIMING_PLACEHOLDER));
        assert!(!updated.contains(ENVELOPE_BUILD_TIMING_PLACEHOLDER));
        assert!(!updated.contains(TYPED_FIELD_CLASSIFICATION_TIMING_PLACEHOLDER));
        assert!(!updated.contains(RENDER_TIMING_PLACEHOLDER));
        assert!(!updated.contains(BUILD_RENDER_TIMING_PLACEHOLDER));
        assert!(!updated.contains(PLACEHOLDER_REPLACE_TIMING_PLACEHOLDER));
    }
}
