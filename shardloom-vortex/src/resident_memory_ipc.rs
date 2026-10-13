//! Bounded intake of pinned native IPC, validated against independent producers.

use std::cell::{Cell, RefCell};

use super::{encoded_transfer, memory_error, native_error};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::{Result, ShardLoomError};
use vortex::{
    array::{
        ArrayId, ArrayRef, IntoArray as _,
        arrays::dict::DictArraySlotsExt as _,
        arrays::{Dict, DictArray},
        buffer::BufferHandle,
        dtype::DType,
        memory::MemorySessionExt as _,
        serde::ArrayChildren,
        session::ArraySessionExt as _,
    },
    buffer::{Alignment, ByteBuffer},
    error::{VortexResult, vortex_err},
    flatbuffers::{array as fa, dtype as fd, message as fm},
};

#[path = "resident_memory_ipc_schema.rs"]
mod schema;

#[path = "resident_memory_ipc_validation.rs"]
mod validation;

#[cfg(test)]
#[path = "resident_memory_ipc_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "resident_memory_ipc_corruption_tests.rs"]
mod corruption_tests;

const MAX_TRANSPORT: usize = 64 << 20;
const MAX_METADATA: usize = 8 << 20;
const MAX_NODES: usize = 4096;
const MAX_DEPTH: usize = 24;

impl super::ResidentMemorySource {
    /// Copy one Vortex 0.85 native IPC record into the receiving resource owner.
    /// Both inputs are complete framed messages from the pinned public encoder;
    /// their combined transport is bounded to 64 MiB. Encodings are checked
    /// against a closed provider registry, and wire statistics are discarded.
    /// # Errors
    /// Rejects unsupported or malformed messages, schema/encoding mismatch,
    /// memory denial and cancellation without publishing partial source state.
    pub fn from_vortex_ipc(
        session: &crate::resident_session::ResidentVortexSession,
        dtype_message: &[u8],
        array_message: &[u8],
        cancellation: &shardloom_exec::compute_pool::CancellationToken,
    ) -> Result<Self> {
        let mut builder = super::MemoryBatchSourceBuilder::new(session, cancellation.clone())?;
        session.with_native_execution_context(cancellation, |context| {
            builder.push_transferred(receive(dtype_message, array_message, context)?)
        })?;
        builder.finish()
    }
}

fn failed(message: &str) -> ShardLoomError {
    memory_error(&format!("SL-NATIVE-IPC: {message}"))
}

fn verification() -> flatbuffers::VerifierOptions {
    flatbuffers::VerifierOptions {
        max_depth: 64,
        max_tables: MAX_NODES * 8,
        max_apparent_size: MAX_TRANSPORT,
        ignore_missing_null_terminator: false,
    }
}

fn frame(bytes: &[u8], kind: fm::MessageHeader) -> Result<(fm::Message<'_>, &[u8])> {
    if bytes.len() > MAX_TRANSPORT || bytes.len() < 4 {
        return Err(failed(
            "message length is outside the admitted transport bound",
        ));
    }
    let length = u32::from_le_bytes(bytes[..4].try_into().map_err(native_error)?) as usize;
    if length > MAX_METADATA {
        return Err(failed("message header exceeds the metadata bound"));
    }
    let header_end = 4usize
        .checked_add(length)
        .ok_or_else(|| failed("header overflow"))?;
    let header = bytes
        .get(4..header_end)
        .ok_or_else(|| failed("truncated message header"))?;
    let message = fm::root_as_message_with_opts(&verification(), header).map_err(native_error)?;
    if message.version() != fm::MessageVersion::V0 || message.header_type() != kind {
        return Err(failed("unsupported message version or kind"));
    }
    let body = &bytes[header_end..];
    if u64::try_from(body.len()).map_err(native_error)? != message.body_size() {
        return Err(failed("message body length mismatch or trailing bytes"));
    }
    Ok((message, body))
}

// Resolve only literal IDs. No untrusted registry name reaches the global
// interner, including names present in an otherwise unused header entry.
fn encoding(name: &str) -> Option<ArrayId> {
    const IDS: &[&str] = &[
        "vortex.null",
        "vortex.bool",
        "vortex.primitive",
        "vortex.decimal",
        "vortex.varbin",
        "vortex.varbinview",
        "vortex.list",
        "vortex.listview",
        "vortex.fixed_size_list",
        "vortex.struct",
        "vortex.ext",
        "vortex.constant",
        "vortex.dict",
        "vortex.chunked",
        "vortex.runend",
        "fastlanes.bitpacked",
    ];
    IDS.iter()
        .find(|id| **id == name)
        .map(|id| ArrayId::new_static(id))
}

struct Inspected<'a> {
    dtype: &'a [u8],
    body: &'a [u8],
    array: fa::Array<'a>,
    message: fm::ArrayMessage<'a>,
    workspace_bytes: u64,
}

fn inspect<'a>(dtype_message: &'a [u8], array_message: &'a [u8]) -> Result<Inspected<'a>> {
    if dtype_message
        .len()
        .checked_add(array_message.len())
        .is_none_or(|n| n > MAX_TRANSPORT)
    {
        return Err(failed("combined messages exceed the transport bound"));
    }
    let (_, dtype) = frame(dtype_message, fm::MessageHeader::DTypeMessage)?;
    if dtype.len() > MAX_METADATA {
        return Err(failed("dtype exceeds the metadata bound"));
    }
    let dtype_view = fd::root_as_dtype_with_opts(&verification(), dtype).map_err(native_error)?;
    schema::validate(dtype_view)?;
    let (header, body) = frame(array_message, fm::MessageHeader::ArrayMessage)?;
    let message = header
        .header_as_array_message()
        .ok_or_else(|| failed("missing array header"))?;
    let names = message
        .encodings()
        .ok_or_else(|| failed("missing encoding registry"))?;
    if names.len() > 64 || names.is_empty() {
        return Err(failed(
            "encoding registry size is outside the admitted bound",
        ));
    }
    for (index, name) in names.iter().enumerate() {
        if encoding(name).is_none() || names.iter().take(index).any(|prior| prior == name) {
            return Err(failed(
                "unknown, unsupported or duplicate encoding registry name",
            ));
        }
    }
    let suffix = body
        .len()
        .checked_sub(4)
        .ok_or_else(|| failed("missing array suffix"))?;
    let metadata_len =
        u32::from_le_bytes(body[suffix..].try_into().map_err(native_error)?) as usize;
    let metadata_start = suffix
        .checked_sub(metadata_len)
        .ok_or_else(|| failed("array metadata length mismatch"))?;
    if metadata_len > MAX_METADATA {
        return Err(failed("array metadata exceeds the admitted bound"));
    }
    let array = fa::root_as_array_with_opts(&verification(), &body[metadata_start..suffix])
        .map_err(native_error)?;
    let buffers = array.buffers().unwrap_or_default();
    if buffers.len() > MAX_NODES {
        return Err(failed("array buffer count exceeds the admitted bound"));
    }
    let mut end = 0usize;
    for buffer in buffers {
        if buffer.compression() != fa::Compression::None || buffer.alignment_exponent() > 16 {
            return Err(failed("unsupported buffer compression or alignment"));
        }
        end = end
            .checked_add(buffer.padding() as usize)
            .and_then(|n| n.checked_add(buffer.length() as usize))
            .filter(|n| *n <= metadata_start)
            .ok_or_else(|| failed("buffer extends into metadata or outside the message"))?;
    }
    if metadata_start - end > 7 {
        return Err(failed("unexpected bytes between buffers and metadata"));
    }
    let root = array.root().ok_or_else(|| failed("missing array root"))?;
    let mut nodes = 0usize;
    // Host view validation may normalize null slots by cloning a data buffer.
    // This transient copy is distinct from the receiving allocator's buffers.
    let mut workspace_bytes =
        4096 + (dtype.len() as u64) * 64 + (metadata_len as u64) * 64 + (end as u64) * 2;
    let mut inspected = Inspected {
        dtype,
        body,
        array,
        message,
        workspace_bytes: 0,
    };
    inspect_node(root, &inspected, 0, &mut nodes, &mut workspace_bytes)?;
    inspected.workspace_bytes = workspace_bytes;
    Ok(inspected)
}

fn inspect_node(
    node: fa::ArrayNode<'_>,
    parent: &Inspected<'_>,
    depth: usize,
    nodes: &mut usize,
    workspace: &mut u64,
) -> Result<()> {
    *nodes += 1;
    if depth > MAX_DEPTH || *nodes > MAX_NODES {
        return Err(failed("array tree exceeds the depth or node bound"));
    }
    let names = parent
        .message
        .encodings()
        .ok_or_else(|| failed("missing encoding registry"))?;
    if usize::from(node.encoding()) >= names.len() {
        return Err(failed("array encoding index is outside its registry"));
    }
    let name = names.get(usize::from(node.encoding()));
    *workspace = workspace
        .checked_add(4096)
        .ok_or_else(|| failed("workspace overflow"))?;
    for index in node.buffers().unwrap_or_default() {
        let buffers = parent.array.buffers().unwrap_or_default();
        if usize::from(index) >= buffers.len() {
            return Err(failed("array buffer index is outside its registry"));
        }
        // A Constant's protobuf scalar can allocate a tuple and strings.
        // Admit that expansion before invoking the native deserializer.
        if name == "vortex.constant" {
            *workspace = workspace
                .checked_add(u64::from(buffers.get(usize::from(index)).length()) * 64)
                .ok_or_else(|| failed("scalar workspace overflow"))?;
        }
    }
    for child in node.children().unwrap_or_default() {
        inspect_node(child, parent, depth + 1, nodes, workspace)?;
    }
    Ok(())
}

fn copy_bytes(
    bytes: &[u8],
    alignment: Alignment,
    context: &NativeExecutionContext<'_>,
) -> Result<ByteBuffer> {
    context.check_cancelled()?;
    let mut output = context
        .native_session()
        .memory()
        .allocator()
        .allocate(bytes.len(), alignment)
        .map_err(native_error)?;
    for (source, target) in bytes
        .chunks(64 << 10)
        .zip(output.as_mut_slice().chunks_mut(64 << 10))
    {
        context.check_cancelled()?;
        target.copy_from_slice(source);
    }
    context.check_cancelled()?;
    Ok(output.freeze())
}

fn receive(
    dtype_message: &[u8],
    array_message: &[u8],
    context: &NativeExecutionContext<'_>,
) -> Result<encoded_transfer::CopiedRecords> {
    // Pinned safe provider constructors still contain assertions for corrupt
    // encoding metadata. Keep their unwind inside this transactional intake;
    // all temporary allocations and grants drop before returning the diagnostic.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receive_checked(dtype_message, array_message, context)
    }))
    .map_err(|_| failed("provider rejected inconsistent encoding metadata"))?
}

fn receive_checked(
    dtype_message: &[u8],
    array_message: &[u8],
    context: &NativeExecutionContext<'_>,
) -> Result<encoded_transfer::CopiedRecords> {
    context.check_cancelled()?;
    // Verification itself is bounded before any input-dependent allocation.
    let _verification = context.memory().reserve(64 << 10)?;
    let inspected = inspect(dtype_message, array_message)?;
    let _workspace = context.memory().reserve(inspected.workspace_bytes)?;
    let dtype_bytes = copy_bytes(inspected.dtype, Alignment::new(8), context)?;
    let dtype = DType::from_flatbuffer(
        dtype_bytes.try_into().map_err(native_error)?,
        context.native_session(),
    )
    .map_err(native_error)?;
    let _schema = crate::native_payload_schema::reserve_schema(&dtype, context.memory())?;
    let mut buffers = Vec::with_capacity(inspected.array.buffers().unwrap_or_default().len());
    let mut offset = 0usize;
    for buffer in inspected.array.buffers().unwrap_or_default() {
        let start = offset + usize::from(buffer.padding());
        offset = start + buffer.length() as usize;
        let alignment = Alignment::try_from_untrusted_exponent(buffer.alignment_exponent())
            .map_err(native_error)?;
        buffers.push(BufferHandle::new_host(copy_bytes(
            &inspected.body[start..offset],
            alignment,
            context,
        )?));
    }
    let decoder = Decoder {
        inspected: &inspected,
        buffers: &buffers,
        context,
        calls: Cell::new(0),
        coordinate_work: validation::CoordinateWork::default(),
    };
    let root = inspected
        .array
        .root()
        .ok_or_else(|| failed("missing array root"))?;
    let array = decoder
        .decode(root, &dtype, inspected.message.row_count() as usize)
        .map_err(native_error)?;
    let mut copied = encoded_transfer::copy_records(
        &[array],
        context,
        encoded_transfer::TransferPolicy::PreserveEncodings,
    )?
    .ok_or_else(|| failed("unsupported record encoding"))?;
    copied.report.ipc_transport_bytes = (dtype_message.len() + array_message.len()) as u64;
    copied.report.ipc_buffer_bytes_copied =
        buffers
            .iter()
            .try_fold(inspected.dtype.len() as u64, |sum, buffer| {
                sum.checked_add(buffer.len() as u64)
                    .ok_or_else(|| failed("transport copy count overflow"))
            })?;
    copied.report.ipc_decode_workspace_peak_reserved_bytes = inspected.workspace_bytes;
    copied.report.ipc_coordinate_arrays_executed = decoder.coordinate_work.arrays_executed.get();
    copied.report.ipc_coordinate_values_checked = decoder.coordinate_work.values_checked.get();
    Ok(copied)
}

struct Decoder<'a, 'b, 's> {
    inspected: &'a Inspected<'b>,
    buffers: &'a [BufferHandle],
    context: &'a NativeExecutionContext<'s>,
    calls: Cell<usize>,
    coordinate_work: validation::CoordinateWork,
}

impl Decoder<'_, '_, '_> {
    fn decode(&self, node: fa::ArrayNode<'_>, dtype: &DType, len: usize) -> VortexResult<ArrayRef> {
        self.context
            .check_cancelled()
            .map_err(|error| vortex_err!("{error}"))?;
        self.calls.set(self.calls.get() + 1);
        if self.calls.get() > MAX_NODES {
            return Err(vortex_err!(
                "SL-NATIVE-IPC: provider child requests exceed the node bound"
            ));
        }
        if len > u32::MAX as usize {
            return Err(vortex_err!(
                "SL-NATIVE-IPC: child row count exceeds the pinned message domain"
            ));
        }
        let names = self
            .inspected
            .message
            .encodings()
            .ok_or_else(|| vortex_err!("missing registry"))?;
        let id = encoding(names.get(node.encoding() as usize))
            .ok_or_else(|| vortex_err!("unsupported encoding"))?;
        let plugin = self
            .context
            .native_session()
            .arrays()
            .registry()
            .get(&id)
            .ok_or_else(|| vortex_err!("encoding provider is unavailable"))?;
        let buffers = node
            .buffers()
            .unwrap_or_default()
            .iter()
            .map(|index| self.buffers[index as usize].clone())
            .collect::<Vec<_>>();
        let child_nodes = node.children().unwrap_or_default();
        let children = Children {
            decoder: self,
            nodes: child_nodes,
            cache: RefCell::new(vec![None; child_nodes.len()]),
            validation: RefCell::new(crate::owned_buffers::reserve(self.context.memory(), 0)?),
            reserves_validation: matches!(
                id.as_str(),
                "vortex.varbin"
                    | "vortex.varbinview"
                    | "vortex.list"
                    | "vortex.listview"
                    | "vortex.runend"
            ),
            parent_encoding: id,
            varbin_bytes: buffers.first().map_or(0, BufferHandle::len),
        };
        let _view_validation = if id.as_str() == "vortex.varbinview" {
            Some(crate::owned_buffers::reserve(
                self.context.memory(),
                (len as u64)
                    .checked_mul(32)
                    .ok_or_else(|| vortex_err!("SL-NATIVE-IPC: validation workspace overflow"))?,
            )?)
        } else {
            None
        };
        let metadata = node.metadata().map_or(&[][..], |bytes| bytes.bytes());
        let mut array = plugin.deserialize(
            dtype,
            len,
            metadata,
            &buffers,
            &children,
            self.context.native_session(),
        )?;
        if children.cache.borrow().iter().any(Option::is_none) {
            return Err(vortex_err!(
                "SL-NATIVE-IPC: provider omitted a declared child"
            ));
        }
        if array.dtype() != dtype
            || array.len() != len
            || !plugin.is_supported_encoding(&array.encoding_id())
        {
            return Err(vortex_err!(
                "SL-NATIVE-IPC: provider construction changed the declared array"
            ));
        }
        if array.is::<Dict>() {
            let dict = array.as_::<Dict>();
            validation::indices(
                dict.codes(),
                validation::IndexRule::Dictionary(dict.values().len()),
                self.context,
                &self.coordinate_work,
            )?;
            // all_values_referenced is an optimization assertion in the wire
            // metadata. Rebuild without trusting that producer assertion.
            array = DictArray::try_new(dict.codes().clone(), dict.values().clone())?.into_array();
        }
        array = validation::packed(array, self.context, &self.coordinate_work)?;
        // Deliberately do not install any serialized statistics.
        Ok(array)
    }
}

struct Children<'a, 'b, 'c, 's> {
    decoder: &'a Decoder<'b, 'c, 's>,
    nodes: flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<fa::ArrayNode<'a>>>,
    cache: RefCell<Vec<Option<ArrayRef>>>,
    validation: RefCell<shardloom_exec::live_memory::MemoryLease>,
    reserves_validation: bool,
    parent_encoding: ArrayId,
    varbin_bytes: usize,
}

impl ArrayChildren for Children<'_, '_, '_, '_> {
    fn get(&self, index: usize, dtype: &DType, len: usize) -> VortexResult<ArrayRef> {
        if index >= self.nodes.len() {
            return Err(vortex_err!(
                "SL-NATIVE-IPC: child index is outside the declared tree"
            ));
        }
        if let Some(array) = self.cache.borrow()[index].as_ref() {
            if array.dtype() != dtype || array.len() != len {
                return Err(vortex_err!(
                    "SL-NATIVE-IPC: inconsistent child schema or length"
                ));
            }
            return Ok(array.clone());
        }
        if self.reserves_validation {
            // Some pinned checked constructors still compute offset/run-end
            // statistics in a legacy session. Reserve their decoded workspace
            // explicitly before child construction or parent validation.
            let mut validation = self.validation.borrow_mut();
            let bytes = (len as u64)
                .checked_mul(32)
                .and_then(|n| n.checked_add(validation.bytes()))
                .ok_or_else(|| vortex_err!("SL-NATIVE-IPC: child validation workspace overflow"))?;
            validation
                .resize(bytes)
                .map_err(|error| vortex_err!("{error}"))?;
        }
        let array = self.decoder.decode(self.nodes.get(index), dtype, len)?;
        match (self.parent_encoding.as_str(), index) {
            ("vortex.varbin", 0) => validation::indices(
                &array,
                validation::IndexRule::Offsets(self.varbin_bytes),
                self.decoder.context,
                &self.decoder.coordinate_work,
            )?,
            ("vortex.runend", 0) => validation::indices(
                &array,
                validation::IndexRule::RunEnds,
                self.decoder.context,
                &self.decoder.coordinate_work,
            )?,
            ("fastlanes.bitpacked", 0) if dtype.is_unsigned_int() => validation::indices(
                &array,
                validation::IndexRule::PatchIndices,
                self.decoder.context,
                &self.decoder.coordinate_work,
            )?,
            _ => {}
        }
        self.cache.borrow_mut()[index] = Some(array.clone());
        Ok(array)
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }
}
