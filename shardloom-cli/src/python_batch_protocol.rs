//! A single bounded adapter transaction. The shared native planner/executor owns
//! all data work; the control reader only frames bytes and signals cancellation.

use crate::{
    native_memory_input::{MemoryInput, MemoryRow, bounded_vec},
    python_worker_protocol::read_bounded_frame,
};
use shardloom_core::ShardLoomError;
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::{
    local_primitives::collect::SerializedVortexResultBatch,
    resident_memory_source::{MemoryBatchSourceBuilder, ResidentMemorySource},
    resident_session::ResidentVortexSession,
};
use std::{
    io::{self, Write as _},
    process::ExitCode,
    sync::{Arc, mpsc},
};

const MAX_FRAME: usize = 8 * 1024 * 1024;

#[path = "python_batch_frames.rs"]
mod frames;

type Result<T> = std::result::Result<T, ShardLoomError>;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(deserialize_with = "bounded_vec::<_, _, 4096>")]
    args: Vec<String>,
    stream_results: bool,
    batch_rows: usize,
}

enum Incoming {
    Rows {
        uri: String,
        index: u64,
        rows: Vec<MemoryRow>,
    },
    End {
        uri: String,
        index: u64,
    },
    Ack {
        index: u64,
    },
    Cancel,
}

pub(crate) struct Transport {
    frames: mpsc::Receiver<Result<frames::Frame>>,
    reader: Option<mpsc::SyncSender<Result<frames::Frame>>>,
    scratch_stats: Arc<frames::ScratchStats>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) stream_results: bool,
    pub(crate) batch_rows: usize,
    pub(crate) input_batches: u64,
    pub(crate) input_sources: u64,
    pub(crate) input_rows: u64,
    pub(crate) output_batches: u64,
}

pub(crate) fn run() -> ExitCode {
    let result = start();
    match result {
        Ok(code) => code,
        Err(error) => {
            let code = crate::emit_error(
                "run",
                shardloom_core::OutputFormat::Json,
                "batch transaction failed",
                &error,
            );
            let _ = io::stdout().flush();
            code
        }
    }
}

fn start() -> Result<ExitCode> {
    let first = read_bounded_frame(
        &mut io::stdin().lock(),
        crate::python_worker_protocol::MAX_REQUEST_BYTES,
    )
    .map_err(failed)?
    .ok_or_else(|| failed("batch transaction declaration is absent"))?;
    let request: Request = serde_json::from_str(&first).map_err(failed)?;
    if request.batch_rows == 0
        || request.batch_rows > 2048
        || request.args.first().map(String::as_str) != Some("run")
        || request.args.iter().any(|arg| arg == "--format")
    {
        return Err(failed(
            "batch transaction requires run arguments, 1..=2048 batch rows and native JSON framing",
        ));
    }
    let token = CancellationToken::default();
    let (sender, receiver) = mpsc::sync_channel(0);
    let mut transport = Transport {
        frames: receiver,
        reader: Some(sender),
        scratch_stats: Arc::new(frames::ScratchStats::default()),
        cancellation: token,
        stream_results: request.stream_results,
        batch_rows: request.batch_rows,
        input_batches: 0,
        input_sources: 0,
        input_rows: 0,
        output_batches: 0,
    };
    let code = crate::public_workflow_route::handle_batch_workflow(
        request.args.into_iter().skip(1),
        &mut transport,
    );
    let _ = io::stdout().flush();
    Ok(code)
}

fn read_control_frames(
    input: &mut impl io::BufRead,
    sender: &mpsc::SyncSender<Result<frames::Frame>>,
    cancellation: &CancellationToken,
    mut scratch: frames::Scratch,
) {
    loop {
        let mut frame = match frames::read(input, &mut scratch, cancellation) {
            Ok(Some(frame)) => frame,
            result => {
                cancellation.cancel();
                let _ = sender.send(Err(failed(match result {
                    Err(error) => error.to_string(),
                    _ => "batch transaction input closed before completion".into(),
                })));
                break;
            }
        };
        match frame.is_cancel() {
            Ok(false) => {}
            result => {
                cancellation.cancel();
                let error = result
                    .err()
                    .unwrap_or_else(|| failed("batch transaction cancelled by consumer"));
                let _ = sender.send(Err(error));
                break;
            }
        }
        if sender.send(Ok(frame)).is_err() {
            break;
        }
    }
}

impl Transport {
    pub(crate) fn start_reader(
        &mut self,
        preparation: &shardloom_vortex::local_primitives::prepared_relational::VortexRelationalPreparation<'_>,
    ) -> Result<()> {
        if self.reader.is_none() {
            return Err(failed("batch control reader already started"));
        }
        // Account for stdin's fixed buffer before transferring the reader into
        // this execution. Each variable-sized frame then acquires its own lease.
        let scratch = frames::Scratch::new(
            preparation.reserve_input_scratch(8192)?,
            Arc::clone(&self.scratch_stats),
        );
        let sender = self
            .reader
            .take()
            .ok_or_else(|| failed("batch control reader is absent"))?;
        let cancellation = self.cancellation.clone();
        std::thread::Builder::new()
            .name("shardloom-batch-control".into())
            .spawn(move || {
                let stdin = io::stdin();
                let mut input = stdin.lock();
                read_control_frames(&mut input, &sender, &cancellation, scratch);
            })
            .map_err(failed)?;
        Ok(())
    }

    pub(crate) fn input_scratch_peak_bytes(&self) -> u64 {
        self.scratch_stats.peak()
    }

    fn incoming(&self, width: Option<usize>) -> Result<frames::Decoded> {
        self.cancellation.check()?;
        if self.reader.is_some() {
            return Err(failed("batch control reader lacks native admission"));
        }
        let frame = self.frames.recv().map_err(failed)??;
        frame.decode(width)
    }

    pub(crate) fn build_source(
        &mut self,
        uri: &str,
        input: &MemoryInput,
        session: &ResidentVortexSession,
    ) -> Result<ResidentMemorySource> {
        let MemoryInput::Batches { schema, streaming } = input else {
            return input.build(session);
        };
        if *streaming {
            return crate::native_memory_rows::build_batch(schema, &[], session);
        }
        let mut builder = MemoryBatchSourceBuilder::new(session, self.cancellation.clone())?;
        add_count(&mut self.input_sources, 1)?;
        let mut index = 0;
        loop {
            send(
                &serde_json::json!({"kind":"input", "uri":uri, "index":index, "max_rows":2048, "max_bytes":MAX_FRAME}),
            )?;
            let mut decoded = self.incoming(Some(schema.len()))?;
            let conversion_bytes = match &decoded.value {
                Incoming::Rows { rows, .. } => {
                    crate::native_memory_rows::conversion_bytes(schema.len(), rows.len())?
                }
                _ => 0,
            };
            let scratch = decoded.conversion_scratch(conversion_bytes)?;
            match &decoded.value {
                Incoming::Rows {
                    uri: actual,
                    index: sequence,
                    rows,
                } if actual == uri && *sequence == index => {
                    crate::native_memory_rows::append_admitted_batch(
                        schema,
                        rows,
                        &mut builder,
                        scratch.lease(),
                    )?;
                    add_count(&mut self.input_batches, 1)?;
                    add_count(&mut self.input_rows, rows.len() as u64)?;
                    add_count(&mut index, 1)?;
                }
                Incoming::End {
                    uri: actual,
                    index: sequence,
                } if actual == uri && *sequence == index => {
                    if index == 0 {
                        crate::native_memory_rows::append_batch(schema, &[], &mut builder)?;
                    }
                    break;
                }
                _ => {
                    return Err(failed(
                        "input batch kind, source or sequence does not match native demand",
                    ));
                }
            }
        }
        builder.finish()
    }

    /// Demand one batch only after the engine released its previous input. The
    /// native executor owns cumulative bounds and end-of-input certification.
    pub(crate) fn next_source_batch(
        &mut self,
        uri: &str,
        input: &MemoryInput,
        session: &ResidentVortexSession,
    ) -> Result<Option<ResidentMemorySource>> {
        let MemoryInput::Batches {
            schema,
            streaming: true,
        } = input
        else {
            return Err(failed(
                "streaming demand requires an explicit streaming declaration",
            ));
        };
        self.cancellation.check()?;
        let index = self.input_batches;
        send(
            &serde_json::json!({"kind":"input", "uri":uri, "index":index, "max_rows":2048, "max_bytes":MAX_FRAME}),
        )?;
        self.input_sources = 1;
        let mut decoded = self.incoming(Some(schema.len()))?;
        let conversion_bytes = match &decoded.value {
            Incoming::Rows { rows, .. } => {
                crate::native_memory_rows::conversion_bytes(schema.len(), rows.len())?
            }
            _ => 0,
        };
        let scratch = decoded.conversion_scratch(conversion_bytes)?;
        match &decoded.value {
            Incoming::Rows {
                uri: actual,
                index: sequence,
                rows,
            } if actual == uri && *sequence == index => {
                let source = crate::native_memory_rows::build_admitted_batch(
                    schema,
                    rows,
                    session,
                    scratch.lease(),
                )?;
                self.cancellation.check()?;
                add_count(&mut self.input_batches, 1)?;
                add_count(&mut self.input_rows, rows.len() as u64)?;
                Ok(Some(source))
            }
            Incoming::End {
                uri: actual,
                index: sequence,
            } if actual == uri && *sequence == index => Ok(None),
            _ => Err(failed(
                "input batch kind, source or sequence does not match native demand",
            )),
        }
    }

    pub(crate) fn consume(&mut self, batch: &SerializedVortexResultBatch) -> Result<()> {
        self.cancellation.check()?;
        {
            // Values and schema are already validated JSON owned by the native
            // bounded serializer. Insert them directly instead of escaping and
            // duplicating either payload into another complete JSON string.
            let mut out = io::stdout().lock();
            write!(
                out,
                "{{\"kind\":\"batch\",\"index\":{},\"row_count\":{},\"rows\":",
                self.output_batches, batch.rows
            )
            .map_err(failed)?;
            out.write_all(batch.values_json.value().as_bytes())
                .map_err(failed)?;
            out.write_all(b",\"schema\":").map_err(failed)?;
            out.write_all(batch.result_schema_json.value().as_bytes())
                .map_err(failed)?;
            out.write_all(b"}\n").map_err(failed)?;
            out.flush().map_err(failed)?;
        }
        match self.incoming(None)?.value {
            Incoming::Ack { index } if index == self.output_batches => {
                add_count(&mut self.output_batches, 1)?;
                self.cancellation.check()
            }
            _ => Err(failed("result batch requires its matching acknowledgement")),
        }
    }
}

fn add_count(value: &mut u64, additional: u64) -> Result<()> {
    *value = value
        .checked_add(additional)
        .ok_or_else(|| failed("batch transaction counter overflow"))?;
    Ok(())
}

fn send(value: &serde_json::Value) -> Result<()> {
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, value).map_err(failed)?;
    out.write_all(b"\n").map_err(failed)?;
    out.flush().map_err(failed)
}

fn failed(reason: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "SL-NATIVE-BATCH: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scratch() -> frames::Scratch {
        let pool = shardloom_exec::live_memory::LiveMemoryPool::new(32 << 20).unwrap();
        frames::Scratch::new(
            pool.reserve(8192).unwrap(),
            Arc::new(frames::ScratchStats::default()),
        )
    }

    fn decode(text: &str, width: Option<usize>) -> Result<frames::Decoded> {
        frames::read(
            &mut io::Cursor::new(text.as_bytes()),
            &mut scratch(),
            &CancellationToken::default(),
        )?
        .ok_or_else(|| failed("test frame missing"))?
        .decode(width)
    }

    #[test]
    fn batch_frames_reject_bad_sequences_types_widths_and_unknown_fields() {
        for frame in [
            json!({"kind":"rows","uri":"memory://x","index":0,"rows":[vec![None::<String>;129]]}),
            json!({"kind":"rows","uri":"memory://x","index":0,"rows":vec![vec![None::<String>];2049]}),
            json!({"kind":"ack","index":true}),
            json!({"kind":"ack","index":-1}),
            json!({"kind":"ack","index":0,"extra":1}),
            json!({"kind":"missing","index":0}),
        ] {
            assert!(decode(&frame.to_string(), Some(128)).is_err());
        }
        assert!(decode(r#"{"kind":"ack","index":0,"index":1}"#, None).is_err());
        assert!(
            decode(
                &json!({
                    "kind":"rows","uri":"memory://x","index":0,"rows":[vec![None::<String>;128]],
                })
                .to_string(),
                Some(128)
            )
            .is_ok()
        );
        assert!(
            serde_json::from_value::<Request>(json!({
                "args":vec!["x";4097],"stream_results":true,"batch_rows":2048,
            }))
            .is_err()
        );
    }

    #[test]
    fn control_reader_cancels_on_eof_cancel_and_oversized_frames() {
        for input in [
            Vec::new(),
            b"{\"kind\":\"cancel\"}\n".to_vec(),
            vec![b'x'; MAX_FRAME + 1],
        ] {
            let token = CancellationToken::default();
            let (sender, receiver) = mpsc::sync_channel(0);
            let control = token.clone();
            let worker = std::thread::spawn(move || {
                read_control_frames(&mut io::Cursor::new(input), &sender, &control, scratch());
            });
            assert!(receiver.recv().unwrap().is_err());
            assert!(token.is_cancelled());
            worker.join().unwrap();
        }
    }

    #[test]
    fn control_reader_preserves_framing_before_cancellation() {
        let token = CancellationToken::default();
        let (sender, receiver) = mpsc::sync_channel(0);
        let control = token.clone();
        let worker = std::thread::spawn(move || {
            read_control_frames(
                &mut io::Cursor::new(b"{\"kind\":\"ack\",\"index\":0}\n{\"kind\":\"cancel\"}\n"),
                &sender,
                &control,
                scratch(),
            );
        });
        let frame = receiver.recv().unwrap().unwrap();
        assert!(matches!(
            frame.decode(None).unwrap().value,
            Incoming::Ack { index: 0 }
        ));
        assert!(receiver.recv().unwrap().is_err());
        worker.join().unwrap();
        assert!(token.is_cancelled());
    }
}
