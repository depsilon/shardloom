//! Shared CLI output rendering for typed command/result envelopes.
//!
//! This module centralizes the renderer used by command handlers. It attaches
//! command-family lifecycle metadata and routes command fields through the
//! typed-envelope field/ref classifier without changing command behavior,
//! executing runtime work, probing datasets, or weakening no-fallback policy.

use std::{
    io::{self, ErrorKind, Write},
    process::ExitCode,
    time::Instant,
};

use shardloom_core::{CommandStatus, Diagnostic, OutputEnvelope, OutputFormat, ShardLoomError};

use crate::{command_family::classify_command, typed_envelope::apply_typed_envelope_fields};

fn envelope_from_fields(
    command: &str,
    status: CommandStatus,
    summary: String,
    text: String,
    diagnostics: Vec<Diagnostic>,
    fields: Vec<(String, String)>,
) -> OutputEnvelope {
    let mut envelope = OutputEnvelope::new(command, status, summary, text)
        .with_lifecycle_field("command_family", classify_command(command).as_str());
    for diagnostic in diagnostics {
        envelope.add_diagnostic(diagnostic);
    }
    apply_typed_envelope_fields(envelope, command, fields)
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
    let timing_start = Instant::now();
    let probe_envelope = envelope_from_fields(
        command,
        status,
        summary.clone(),
        text.clone(),
        diagnostics.clone(),
        fields.clone(),
    );
    let _rendered_probe = probe_envelope.render(format);
    let micros = timing_start.elapsed().as_micros().to_string();
    fields.push(("json_envelope_emit_micros".to_string(), micros));
    fields.push((
        "json_envelope_emit_timing_status".to_string(),
        "measured_by_probe_render_before_final_emit".to_string(),
    ));
    let envelope = envelope_from_fields(command, status, summary, text, diagnostics, fields);
    write_stdout_line(&envelope.render(format));
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

fn write_stdout_line(rendered: &str) {
    let mut stdout = io::stdout().lock();
    if let Err(error) = writeln!(stdout, "{rendered}") {
        if error.kind() == ErrorKind::BrokenPipe {
            return;
        }
        eprintln!("failed writing ShardLoom CLI output: {error}");
        std::process::exit(1);
    }
}
