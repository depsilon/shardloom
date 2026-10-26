//! Explicit schema inspection reads format metadata, never row pages or IPC
//! dictionaries. The ordinary query binder consumes the resulting schema.

use super::{
    Arc, CancellationToken, File, Path, Result, SchemaRef, ShardLoomError, SourceIdentity,
    parquet_metadata_first_reader_options,
};
use shardloom_exec::live_memory::Budgeted;
use std::io::{Read as _, Seek as _, SeekFrom};

const MAX_FOOTER_BYTES: usize = 16 << 20;

/// Read a self-describing local input's schema without constructing a batch
/// reader or preparing compatibility data. Footer size is checked before
/// allocation; provider metadata workspace and retained schema use the action
/// grant. This is not an accounting claim for upstream allocator internals.
///
/// # Errors
/// Rejects unsupported formats, malformed/oversized footers, changing source
/// generations, cancellation and insufficient shared capacity.
pub fn inspect_local_columnar_schema(
    path: &Path,
    format: &str,
    session: &crate::resident_session::ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<Budgeted<SchemaRef>> {
    cancellation.check()?;
    let source = SourceIdentity::capture(path)?;
    let mut file = source.open_checked()?;
    let (length, trailer) = match format {
        "parquet" => (footer_length(&mut file, b"PAR1", 4)?, 8),
        "arrow-ipc" => (footer_length(&mut file, b"ARROW1", 6)?, 10),
        _ => return Err(failed("source has no admitted metadata-only schema reader")),
    };
    let allowance = u64::try_from(length)
        .ok()
        .and_then(|bytes| bytes.checked_mul(32))
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(|| failed("schema inspection workspace overflow"))?;
    let mut workspace = session.reserve_input_scratch(allowance)?;
    cancellation.check()?;
    let schema = if format == "parquet" {
        let metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
            &file,
            parquet_metadata_first_reader_options(),
        )
        .map_err(failed)?;
        Arc::clone(metadata.schema())
    } else {
        file.seek(SeekFrom::End(
            -(i64::try_from(length).map_err(failed)? + trailer),
        ))
        .map_err(failed)?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes).map_err(failed)?;
        let footer = arrow_ipc::root_as_footer(&bytes).map_err(failed)?;
        let schema = footer
            .schema()
            .ok_or_else(|| failed("IPC footer has no schema"))?;
        if !schema.endianness().equals_to_target_endianness() {
            return Err(failed(
                "IPC metadata endianness differs from the native provider",
            ));
        }
        Arc::new(
            std::panic::catch_unwind(|| arrow_ipc::convert::fb_to_schema(schema))
                .map_err(|_| failed("IPC footer contains an invalid logical schema"))?,
        )
    };
    cancellation.check()?;
    source.validate()?;
    let mut bytes = crate::native_payload_schema::names_bytes(schema.fields().len(), |index| {
        schema.field(index).name().as_str()
    })?;
    for field in schema.fields() {
        cancellation.check()?;
        bytes = bytes
            .checked_add(crate::native_payload_schema::arrow_metadata_bytes(
                field.data_type(),
            )?)
            .ok_or_else(|| failed("schema metadata size overflow"))?;
    }
    workspace.resize(bytes)?;
    Ok(Budgeted::new(schema, workspace))
}

fn footer_length(file: &mut File, magic: &[u8], header_bytes: u64) -> Result<usize> {
    let size = file.metadata().map_err(failed)?.len();
    let trailer = magic.len() + 4;
    if size < header_bytes + u64::try_from(trailer).map_err(failed)? {
        return Err(failed("input is too short for its schema footer"));
    }
    let mut header = vec![0; magic.len()];
    file.read_exact(&mut header).map_err(failed)?;
    file.seek(SeekFrom::End(-i64::try_from(trailer).map_err(failed)?))
        .map_err(failed)?;
    let mut tail = vec![0; trailer];
    file.read_exact(&mut tail).map_err(failed)?;
    let length = u32::from_le_bytes(tail[..4].try_into().map_err(failed)?) as usize;
    if header != magic
        || &tail[4..] != magic
        || length == 0
        || length > MAX_FOOTER_BYTES
        || u64::try_from(length + trailer).map_err(failed)? > size - header_bytes
    {
        return Err(failed(
            "input schema footer is malformed or exceeds admission",
        ));
    }
    Ok(length)
}

fn failed(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "metadata schema inspection: {error}; no fallback execution was attempted"
    ))
}
