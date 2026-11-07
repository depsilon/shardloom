//! Capture selected descriptors on the admitted producer; stream their owned
//! chunks after the ordinary table child, before the native footer is published.

use super::{
    Arena, Backing, CHUNK_BYTES, MemoryView, OwnedBytes, Payload,
    descriptor::{self, Descriptor},
    digest, error,
    layout::{self, DIRECTORY_MAGIC, Header},
    reserved_vec,
};
use crate::resident_session::{NativeExecutionContext, OwnedLocalRead};
use bytes::Bytes;
use futures::future::BoxFuture;
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, LiveMemoryPool},
};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, VortexSessionExecute as _,
        arrays::{
            ExtensionArray, FixedSizeListArray, ListViewArray, StructArray, VarBinViewArray,
            extension::ExtensionArrayExt as _, fixed_size_list::FixedSizeListArrayExt as _,
            listview::ListViewArrayExt as _, struct_::StructArrayExt as _,
        },
        dtype::DType,
    },
    buffer::ByteBuffer,
    error::{VortexResult, vortex_err},
    layout::{
        LayoutRef, LayoutStrategy, LayoutWriterContext,
        segments::SegmentSinkRef,
        sequence::{SendableSequentialStream, SequencePointer},
    },
    session::VortexSession,
};

struct Entry {
    payload: Arc<Payload>,
    reader: Option<OwnedLocalRead>,
}

pub(crate) struct Objects {
    arena: Arena,
    memory: LiveMemoryPool,
    cancellation: CancellationToken,
    entries: Mutex<Option<Budgeted<Vec<Entry>>>>,
    complete: AtomicBool,
    layout_bytes: AtomicU64,
    sink_failed: OnceLock<Arc<AtomicBool>>,
}

type StrategyOwner = (Arc<dyn LayoutStrategy>, Option<Arc<Objects>>);

pub(crate) fn strategy(
    base: Arc<dyn LayoutStrategy>,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<StrategyOwner> {
    if !crate::native_binary_type::contains(dtype) {
        return Ok((base, None));
    }
    let (entries, credit) = reserved_vec(context.memory(), 0)?;
    let objects = Arc::new(Objects {
        arena: Arena::get(context.native_session())?,
        memory: context.memory().clone(),
        cancellation: context.cancellation().clone(),
        entries: Mutex::new(Some(Budgeted::new(entries, credit))),
        complete: AtomicBool::new(false),
        layout_bytes: AtomicU64::new(0),
        sink_failed: OnceLock::new(),
    });
    Ok((
        Arc::new(Strategy {
            base,
            objects: Arc::clone(&objects),
        }),
        Some(objects),
    ))
}

impl Objects {
    pub(crate) fn watch_sink_failure(&self, signal: Arc<AtomicBool>) -> Result<()> {
        self.sink_failed
            .set(signal)
            .map_err(|_| error("OWNER", "object writer already has a sink owner"))
    }

    fn check(&self) -> Result<()> {
        self.cancellation.check()?;
        if self
            .sink_failed
            .get()
            .is_some_and(|signal| signal.load(Ordering::Acquire))
        {
            return Err(error(
                "OUTPUT",
                "object payload copy stopped after output failure",
            ));
        }
        Ok(())
    }

    pub(crate) fn observe(
        &self,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let mut ctx = context.native_session().create_execution_ctx();
        visit(array, &mut ctx, &self.cancellation, &mut |bytes| {
            match descriptor::parse(bytes)? {
                Descriptor::External { .. } => {
                    // Validate the declared wire shape without opening externally
                    // owned files or converting them into managed payloads.
                    Ok(())
                }
                Descriptor::Managed { id, length } => {
                    let mut state = self
                        .entries
                        .lock()
                        .map_err(|_| error("OWNER", "object writer is poisoned"))?;
                    let entries = state
                        .as_mut()
                        .ok_or_else(|| error("OWNER", "object writer is already closed"))?;
                    match entries.value().binary_search_by_key(&id, |entry| {
                        entry.payload.id.expect("managed writer")
                    }) {
                        Ok(index) => {
                            if entries.value()[index].payload.length != length {
                                return Err(error(
                                    "DESCRIPTOR",
                                    "selected object length differs from its owner",
                                ));
                            }
                        }
                        Err(index) => {
                            let payload = self.arena.lookup(&id, length)?;
                            payload.validate()?;
                            let reader = match &payload.backing {
                                Backing::Memory(_) => None,
                                Backing::File { identity, .. } => {
                                    Some(context.local_source_reader(
                                        Arc::clone(identity),
                                        Arc::clone(&self.arena.0.metrics),
                                    )?)
                                }
                            };
                            let (mut replacement, credit) =
                                reserved_vec(&self.memory, entries.value().len() + 1)?;
                            // Move entries only after all new allocation/read admission
                            // has succeeded. Dropping a failed producer aborts payload IO.
                            let (old, old_credit) =
                                state.take().expect("checked writer state").into_parts();
                            replacement.extend(old);
                            drop(old_credit);
                            replacement.insert(index, Entry { payload, reader });
                            *state = Some(Budgeted::new(replacement, credit));
                        }
                    }
                    Ok(())
                }
            }
        })
    }

    pub(crate) fn complete_input(&self) {
        self.complete.store(true, Ordering::Release);
    }

    pub(crate) fn metadata_bytes(&self) -> u64 {
        self.layout_bytes.load(Ordering::Relaxed)
    }

    fn take(&self) -> Result<Budgeted<Vec<Entry>>> {
        self.check()?;
        if !self.complete.load(Ordering::Acquire) {
            return Err(error(
                "CANCELLED",
                "object output was abandoned before input completion",
            ));
        }
        self.entries
            .lock()
            .map_err(|_| error("OWNER", "object writer is poisoned"))?
            .take()
            .ok_or_else(|| error("OWNER", "object writer is already closed"))
    }
}

struct Strategy {
    base: Arc<dyn LayoutStrategy>,
    objects: Arc<Objects>,
}

impl LayoutStrategy for Strategy {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        ctx: LayoutWriterContext,
        sink: SegmentSinkRef,
        input: SendableSequentialStream,
        eof: SequencePointer,
        session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            // The first EOF belongs wholly to the child. No earlier pointer is
            // retained while writing raw segments at the later EOF.
            let (child_eof, mut payload_eof) = eof.split();
            let child = self
                .base
                .write_stream(ctx, Arc::clone(&sink), input, child_eof, session)
                .await?;
            let entries = self.objects.take().map_err(native_error)?;
            let chunks = entries.value().iter().try_fold(0_usize, |count, entry| {
                count
                    .checked_add(entry.payload.chunks.value().len())
                    .ok_or_else(|| vortex_err!("SL-BINARY-OBJECT-RESOURCE: chunk count overflows"))
            })?;
            let directory_bytes =
                Header::directory_size(entries.value().len() as u64, chunks as u64)?;
            let layout_credit = layout::reserve(session, chunks)?;
            self.objects
                .layout_bytes
                .store(layout::reserved_bytes(chunks)? as u64, Ordering::Relaxed);
            let mut ids = Vec::new();
            ids.try_reserve_exact(chunks + 1)
                .map_err(|cause| vortex_err!("{cause}"))?;
            if ids.capacity() > chunks + 1 {
                return Err(vortex_err!(
                    "SL-BINARY-OBJECT-RESOURCE: segment allocation exceeds admission"
                ));
            }
            let (mut directory, directory_credit) =
                reserved_vec(&self.objects.memory, usize::try_from(directory_bytes)?)
                    .map_err(native_error)?;
            directory.extend_from_slice(DIRECTORY_MAGIC);
            for entry in entries.value() {
                self.objects.check().map_err(native_error)?;
                let payload = &entry.payload;
                payload.validate().map_err(native_error)?;
                directory.extend_from_slice(&payload.id.expect("managed writer"));
                directory.extend_from_slice(&payload.length.to_le_bytes());
                directory.extend_from_slice(&(payload.chunks.value().len() as u64).to_le_bytes());
                for (index, chunk) in payload.chunks.value().iter().enumerate() {
                    self.objects.check().map_err(native_error)?;
                    let bytes = match &payload.backing {
                        Backing::Memory(data) => Bytes::from_owner(MemoryView {
                            data: Arc::clone(data),
                            range: index * CHUNK_BYTES..index * CHUNK_BYTES + chunk.length as usize,
                        }),
                        Backing::File { .. } => entry
                            .reader
                            .as_ref()
                            .expect("held file reader")
                            .bytes(chunk.offset, chunk.length as usize)
                            .map_err(native_error)?,
                    };
                    if digest(&bytes) != chunk.digest {
                        return Err(vortex_err!(
                            "SL-BINARY-OBJECT-INTEGRITY: output object chunk checksum does not match"
                        ));
                    }
                    ids.push(
                        sink.write(payload_eof.advance(), vec![ByteBuffer::from(bytes)])
                            .await?,
                    );
                    directory.extend_from_slice(&chunk.digest);
                }
                payload.validate().map_err(native_error)?;
            }
            let header = Header {
                objects: entries.value().len() as u64,
                chunks: chunks as u64,
                directory_bytes,
                digest: digest(&directory),
            };
            self.objects.check().map_err(native_error)?;
            let bytes = Bytes::from_owner(OwnedBytes(Budgeted::new(directory, directory_credit)));
            ids.push(
                sink.write(payload_eof.advance(), vec![ByteBuffer::from(bytes)])
                    .await?,
            );
            self.objects.check().map_err(native_error)?;
            Ok(layout::wrap(child, ids, header, layout_credit))
        })
    }
}

fn native_error(cause: impl std::fmt::Display) -> vortex::error::VortexError {
    vortex_err!("{cause}")
}

/// Visit only logical nonnull descriptor cells. A null parent and unreachable
/// list elements cannot cause an otherwise unselected payload to be exported.
pub(crate) fn visit(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    cancellation: &CancellationToken,
    callback: &mut impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    cancellation.check()?;
    if !crate::native_binary_type::contains(array.dtype()) {
        return Ok(());
    }
    let map = |cause| error("DESCRIPTOR", cause);
    match array.dtype() {
        DType::Extension(_) => {
            let extension = array.clone().execute::<ExtensionArray>(ctx).map_err(map)?;
            let storage = extension
                .storage_array()
                .clone()
                .execute::<VarBinViewArray>(ctx)
                .map_err(map)?;
            for index in 0..storage.len() {
                cancellation.check()?;
                if storage.is_valid(index, ctx).map_err(map)? {
                    callback(&storage.bytes_at(index))?;
                }
            }
        }
        DType::Struct(_, _) => {
            let structure = array.clone().execute::<StructArray>(ctx).map_err(map)?;
            for child in structure.iter_unmasked_fields() {
                if !crate::native_binary_type::contains(child.dtype()) {
                    continue;
                }
                if array.dtype().is_nullable() {
                    for index in 0..array.len() {
                        cancellation.check()?;
                        if structure.is_valid(index, ctx).map_err(map)? {
                            visit(
                                &child.slice(index..index + 1).map_err(map)?,
                                ctx,
                                cancellation,
                                callback,
                            )?;
                        }
                    }
                } else {
                    visit(child, ctx, cancellation, callback)?;
                }
            }
        }
        DType::List(_, _) => {
            let list = array.clone().execute::<ListViewArray>(ctx).map_err(map)?;
            for index in 0..array.len() {
                cancellation.check()?;
                if list.is_valid(index, ctx).map_err(map)? {
                    visit(
                        &list.list_elements_at(index, ctx).map_err(map)?,
                        ctx,
                        cancellation,
                        callback,
                    )?;
                }
            }
        }
        DType::FixedSizeList(_, _, _) => {
            let list = array
                .clone()
                .execute::<FixedSizeListArray>(ctx)
                .map_err(map)?;
            for index in 0..array.len() {
                cancellation.check()?;
                if list.is_valid(index, ctx).map_err(map)? {
                    visit(
                        &list.fixed_size_list_elements_at(index).map_err(map)?,
                        ctx,
                        cancellation,
                        callback,
                    )?;
                }
            }
        }
        _ => return Err(error("DESCRIPTOR", "unsupported object column container")),
    }
    Ok(())
}
