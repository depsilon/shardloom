//! Admit a native object's directory without touching its raw payload segments.

use super::{
    Backing, CHUNK_BYTES, Chunk, ObjectId, Payload, content_id, digest, error,
    layout::{DIRECTORY_MAGIC, Header, Objects},
    reserved_vec,
};
use crate::{
    resident_session::{LocalReadMetrics, NativeExecutionContext},
    source_identity::SourceIdentity,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::Budgeted;
use std::sync::Arc;
use vortex::{
    buffer::ByteBuffer,
    file::{DeserializeStep, EOF_SIZE, Footer, MAX_POSTSCRIPT_SIZE, SegmentSpec, VortexFile},
    layout::Layout,
};

/// The provider's ordinary initial tail read spans up to 64 KiB and can include
/// object bytes in a small file. Read the actual EOF/postscript first, then let
/// the native footer parser request exactly the remaining metadata. No custom
/// layout/array decoder or alternate execution path is involved.
pub(crate) fn read_footer(
    identity: Arc<SourceIdentity>,
    context: &NativeExecutionContext<'_>,
) -> Result<Footer> {
    let reader = context.local_source_reader(identity, Arc::new(LocalReadMetrics::default()))?;
    read_footer_ranges(reader.len(), context.native_session(), |start, length| {
        context.check_cancelled()?;
        reader.bytes(start, length)
    })
}

pub(crate) fn read_footer_ranges(
    file_length: u64,
    session: &vortex::session::VortexSession,
    mut read: impl FnMut(u64, usize) -> Result<bytes::Bytes>,
) -> Result<Footer> {
    let eof_start = file_length
        .checked_sub(EOF_SIZE as u64)
        .ok_or_else(|| error("DIRECTORY", "native file is shorter than its EOF marker"))?;
    let eof = read(eof_start, EOF_SIZE)?;
    let postscript = u16::from_le_bytes([eof[2], eof[3]]);
    if postscript > MAX_POSTSCRIPT_SIZE {
        return Err(error(
            "DIRECTORY",
            "native postscript exceeds its format bound",
        ));
    }
    let length = usize::from(postscript) + EOF_SIZE;
    let start = file_length
        .checked_sub(length as u64)
        .ok_or_else(|| error("DIRECTORY", "native postscript exceeds the file"))?;
    let mut parser = Footer::deserializer(ByteBuffer::from(read(start, length)?), session.clone())
        .with_size(file_length);
    loop {
        match parser
            .deserialize()
            .map_err(|cause| error("DIRECTORY", cause))?
        {
            DeserializeStep::Done(footer) => return Ok(footer),
            DeserializeStep::NeedFileSize => {
                return Err(error("DIRECTORY", "native footer lost its file size"));
            }
            DeserializeStep::NeedMoreData { offset, len } => {
                let expected_end = file_length.checked_sub(parser.buffer().len() as u64);
                if offset.checked_add(len as u64) != expected_end || len == 0 {
                    return Err(error("DIRECTORY", "invalid native footer metadata range"));
                }
                parser
                    .try_prefix_data(ByteBuffer::from(read(offset, len)?))
                    .map_err(|cause| error("DIRECTORY", cause))?;
            }
        }
    }
}

/// Unpublished, fully validated metadata. The caller checks its source and
/// cancellation before publishing the complete batch, and owns the final I/O
/// drain. Internal opens keep that scope live for the following table scan.
pub(crate) fn prepare(
    file: &VortexFile,
    identity: &Arc<SourceIdentity>,
    context: &NativeExecutionContext<'_>,
) -> Result<Budgeted<Vec<Arc<Payload>>>> {
    let root = file.footer().layout();
    let Some(layout) = root.as_any().downcast_ref::<Layout<Objects>>() else {
        if crate::native_binary_type::contains(file.dtype()) {
            return Err(error(
                "MISSING",
                "binary-object columns require a complete native object layout",
            ));
        }
        let (objects, credit) = reserved_vec(context.memory(), 0)?;
        return Ok(Budgeted::new(objects, credit));
    };
    let header = layout.header;
    let map = file.footer().segment_map();
    let first = validate_segments(layout, map, identity.generation.len)?;
    let catalog_segment = map
        .last()
        .ok_or_else(|| error("DIRECTORY", "missing object directory"))?;
    let raw = read_directory(&header, catalog_segment, identity, context)?;
    let mut cursor = Cursor(&raw[DIRECTORY_MAGIC.len()..]);
    let count = usize::try_from(header.objects).map_err(|cause| error("RESOURCE", cause))?;
    let (mut objects, credit) = reserved_vec(context.memory(), count)?;
    let mut next_segment = first;
    let mut previous_id = None;
    for _ in 0..count {
        context.check_cancelled()?;
        let id: ObjectId = cursor.take(32)?.try_into().expect("checked width");
        if previous_id.is_some_and(|previous| previous >= id) {
            return Err(error(
                "DIRECTORY",
                "object identities must be distinct and sorted",
            ));
        }
        previous_id = Some(id);
        let length = cursor.u64()?;
        let chunks = cursor.u64()?;
        if chunks != length.div_ceil(CHUNK_BYTES as u64)
            || chunks > header.chunks
            || chunks > (map.len() - 1 - next_segment) as u64
        {
            return Err(error(
                "DIRECTORY",
                "object chunks do not cover its declared length",
            ));
        }
        let (mut entries, chunk_credit) = reserved_vec(
            context.memory(),
            usize::try_from(chunks).map_err(|cause| error("RESOURCE", cause))?,
        )?;
        for index in 0..chunks {
            context.check_cancelled()?;
            let segment = &map[next_segment];
            let expected = (length - index * CHUNK_BYTES as u64).min(CHUNK_BYTES as u64);
            if u64::from(segment.length) != expected {
                return Err(error(
                    "DIRECTORY",
                    "object chunk has a different native segment length",
                ));
            }
            entries.push(Chunk {
                offset: segment.offset,
                length: segment.length,
                digest: cursor.take(32)?.try_into().expect("checked width"),
            });
            next_segment += 1;
        }
        if content_id(length, &entries) != id {
            return Err(error(
                "INTEGRITY",
                "object identity does not match its chunk directory",
            ));
        }
        let metadata = context.memory().reserve(512)?;
        objects.push(Arc::new(Payload {
            id: Some(id),
            length,
            chunks: Budgeted::new(entries, chunk_credit),
            backing: Backing::File {
                identity: Arc::clone(identity),
                offset: 0,
                _retention: None,
            },
            _metadata: metadata,
        }));
    }
    if !cursor.0.is_empty() || next_segment != map.len() - 1 {
        return Err(error(
            "DIRECTORY",
            "object directory contains trailing bytes or unused segments",
        ));
    }
    context.check_cancelled()?;
    identity.validate()?;
    Ok(Budgeted::new(objects, credit))
}

fn read_directory(
    header: &Header,
    segment: &SegmentSpec,
    identity: &Arc<SourceIdentity>,
    context: &NativeExecutionContext<'_>,
) -> Result<bytes::Bytes> {
    if u64::from(segment.length) != header.directory_bytes {
        return Err(error(
            "DIRECTORY",
            "object directory segment has a different length",
        ));
    }
    let reader =
        context.local_source_reader(Arc::clone(identity), Arc::new(LocalReadMetrics::default()))?;
    let raw = reader.bytes(segment.offset, segment.length as usize)?;
    if digest(&raw) != header.digest || !raw.starts_with(DIRECTORY_MAGIC) {
        return Err(error(
            "INTEGRITY",
            "object directory checksum or version does not match",
        ));
    }
    Ok(raw)
}

fn validate_segments(
    layout: &Layout<Objects>,
    map: &[SegmentSpec],
    file_length: u64,
) -> Result<usize> {
    let ids = layout.segment_ids();
    let first = map.len().checked_sub(ids.len()).ok_or_else(|| {
        error(
            "DIRECTORY",
            "object directory declares unavailable segments",
        )
    })?;
    let mut previous_end = map[..first].iter().try_fold(4_u64, |end, segment| {
        segment
            .offset
            .checked_add(u64::from(segment.length))
            .filter(|end| *end <= file_length)
            .map(|stop| end.max(stop))
            .ok_or_else(|| error("DIRECTORY", "table segment exceeds its file"))
    })?;
    // Native object segments are the ordered suffix after the table child.
    // Enforce both identifier and physical geometry before binding any bytes.
    for (index, id) in ids.iter().enumerate() {
        if **id as usize != first + index {
            return Err(error(
                "DIRECTORY",
                "object segments are not the native suffix",
            ));
        }
        let segment = &map[first + index];
        let end = segment
            .offset
            .checked_add(u64::from(segment.length))
            .filter(|end| *end <= file_length)
            .ok_or_else(|| error("DIRECTORY", "object segment exceeds its file"))?;
        if segment.offset < previous_end {
            return Err(error(
                "DIRECTORY",
                "object segments overlap or precede the file header",
            ));
        }
        previous_end = end;
    }
    // The child is materialized only for schema/row validation, not scanned.
    let child = layout
        .children()
        .child(0, layout.dtype())
        .map_err(|cause| error("DIRECTORY", cause))?;
    if child.dyn_row_count() != layout.row_count() {
        return Err(error(
            "DIRECTORY",
            "object table child has a different row count",
        ));
    }
    Ok(first)
}

struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let (head, rest) = self
            .0
            .split_at_checked(count)
            .ok_or_else(|| error("DIRECTORY", "truncated object directory"))?;
        self.0 = rest;
        Ok(head)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("checked width"),
        ))
    }
}
