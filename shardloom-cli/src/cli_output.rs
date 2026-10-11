//! Shared CLI output rendering for typed command/result envelopes.
//!
//! This module centralizes the renderer used by command handlers. It attaches
//! command-family lifecycle metadata and routes command fields through the
//! typed-envelope field/ref classifier without changing command behavior,
//! executing runtime work, probing datasets, or weakening no-fallback policy.

use std::{
    cell::RefCell,
    io::{self, ErrorKind, Write},
    process::ExitCode,
    sync::atomic::{AtomicU64, Ordering},
};

use shardloom_core::{CommandStatus, Diagnostic, OutputEnvelope, OutputFormat, ShardLoomError};

use crate::{command_family::classify_command, typed_envelope::apply_typed_envelope_fields};

static OUTPUT_EMISSION_COUNT: AtomicU64 = AtomicU64::new(0);

enum PendingLine {
    Stdout { rendered: String },
    Stderr { message: String },
}

thread_local! {
    // Native public routes emit on their caller thread. A nested scope releases
    // into its parent, so no envelope escapes an outer generation check.
    static PENDING_OUTPUT: RefCell<Vec<Vec<PendingLine>>> = const { RefCell::new(Vec::new()) };
}

struct PendingOutput {
    scope_index: usize,
}

impl PendingOutput {
    fn new() -> Self {
        let scope_index = PENDING_OUTPUT.with_borrow_mut(|pending| {
            let index = pending.len();
            pending.push(Vec::new());
            index
        });
        Self { scope_index }
    }

    fn take(&self) -> Vec<PendingLine> {
        PENDING_OUTPUT.with_borrow_mut(|pending| std::mem::take(&mut pending[self.scope_index]))
    }
}

impl Drop for PendingOutput {
    fn drop(&mut self) {
        // Discard unvalidated output on both ordinary errors and unwinding.
        PENDING_OUTPUT.with_borrow_mut(|pending| pending.truncate(self.scope_index));
    }
}

/// Hold response envelopes until the input generations survive execution.
/// This governs CLI evidence, not rollback of a sink already published by a
/// native route. Such output is not certified when final validation fails.
pub(crate) fn with_validated_output<T>(
    validate: impl Fn() -> Result<(), ShardLoomError>,
    execute: impl FnOnce() -> T,
) -> Result<T, ShardLoomError> {
    validate()?;
    let pending = PendingOutput::new();
    let result = execute();
    validate()?;
    let lines = pending.take();
    drop(pending);
    for line in lines {
        write_output_line(line);
    }
    Ok(result)
}

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
    write_stdout_line(envelope.render(format));
}

pub(crate) fn emit_error(
    command: &str,
    format: OutputFormat,
    summary: &str,
    error: &ShardLoomError,
) -> ExitCode {
    emit_error_with_fields(command, format, summary, error, Vec::new())
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
        OutputFormat::Text => write_output_line(PendingLine::Stderr {
            message: error.to_string(),
        }),
        OutputFormat::Json => write_stdout_line(envelope.to_json()),
    }
    ExitCode::from(2)
}

fn write_stdout_line(rendered: String) {
    write_output_line(PendingLine::Stdout { rendered });
}

fn write_output_line(line: PendingLine) {
    let line = PENDING_OUTPUT.with_borrow_mut(|pending| {
        if let Some(lines) = pending.last_mut() {
            lines.push(line);
            None
        } else {
            Some(line)
        }
    });
    // Keep response payloads and human diagnostics in separate variants/fields;
    // a buffered JSON response must never be routed to the diagnostic channel.
    let rendered = match line {
        Some(PendingLine::Stdout { rendered }) => rendered,
        Some(PendingLine::Stderr { message }) => {
            eprintln!("{message}");
            return;
        }
        None => return,
    };
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
    use std::cell::Cell;

    use shardloom_core::{CommandStatus, OutputFormat, ShardLoomError};

    #[test]
    fn generation_failure_discards_every_response_style() {
        let outer = super::PendingOutput::new();
        for format in [OutputFormat::Json, OutputFormat::Text] {
            let changed = Cell::new(false);
            let result = super::with_validated_output(
                || {
                    if changed.get() {
                        Err(ShardLoomError::InvalidOperation(
                            "changed generation".into(),
                        ))
                    } else {
                        Ok(())
                    }
                },
                || {
                    super::emit(
                        "run",
                        format,
                        CommandStatus::Success,
                        "stale".into(),
                        "stale".into(),
                        Vec::new(),
                        Vec::new(),
                    );
                    let error = ShardLoomError::InvalidOperation("stale diagnostic".into());
                    super::emit_error("run", format, "stale", &error);
                    changed.set(true);
                },
            );
            assert!(result.is_err());
            assert!(outer.take().is_empty(), "unvalidated response escaped");
        }
    }

    #[test]
    fn generation_failure_before_execution_skips_effects() {
        let ran = Cell::new(false);
        let result = super::with_validated_output(
            || {
                Err(ShardLoomError::InvalidOperation(
                    "changed generation".into(),
                ))
            },
            || ran.set(true),
        );
        assert!(result.is_err());
        assert!(!ran.get());
    }

    #[test]
    fn validated_output_preserves_nested_order_and_discards_panics() {
        let outer = super::PendingOutput::new();
        super::with_validated_output(
            || Ok(()),
            || {
                super::write_stdout_line("first".into());
                super::with_validated_output(
                    || Ok(()),
                    || {
                        super::write_stdout_line("second".into());
                    },
                )
                .unwrap();
            },
        )
        .unwrap();
        assert_eq!(
            outer
                .take()
                .into_iter()
                .map(stdout_text)
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert!(
            std::panic::catch_unwind(|| {
                let _ = super::with_validated_output(
                    || Ok(()),
                    || {
                        super::write_stdout_line("unvalidated".into());
                        panic!("execution interrupted");
                    },
                );
            })
            .is_err()
        );
        assert!(outer.take().is_empty());
        super::write_stdout_line("after unwind".into());
        assert_eq!(stdout_text(outer.take().pop().unwrap()), "after unwind");
    }

    fn stdout_text(line: super::PendingLine) -> String {
        match line {
            super::PendingLine::Stdout { rendered } => rendered,
            super::PendingLine::Stderr { .. } => panic!("expected stdout response"),
        }
    }

    #[test]
    fn validated_output_preserves_text_error_and_json_response_channels() {
        let outer = super::PendingOutput::new();
        super::with_validated_output(
            || Ok(()),
            || {
                let error = ShardLoomError::InvalidOperation("expected diagnostic".into());
                super::emit_error("run", OutputFormat::Text, "failed", &error);
                super::emit_error("run", OutputFormat::Json, "failed", &error);
            },
        )
        .unwrap();
        let mut lines = outer.take().into_iter();
        assert!(
            matches!(lines.next(), Some(super::PendingLine::Stderr { message })
            if message.contains("expected diagnostic"))
        );
        assert!(
            matches!(lines.next(), Some(super::PendingLine::Stdout { rendered })
            if serde_json::from_str::<serde_json::Value>(&rendered).is_ok())
        );
        assert!(lines.next().is_none());
    }
}
