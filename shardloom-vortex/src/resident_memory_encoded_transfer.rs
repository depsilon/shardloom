//! Physical native transfer under the receiving grant. The source graph stays
//! alive through both passes; address identity is never cached across transfers.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{
    local_primitives::native_capacity::ReservedVec,
    native_payload_schema,
    resident_memory_source::{NativeTransferReport, memory_error, native_error},
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{
        Array, ArrayParts, ArrayRef, IntoArray as _, VTable,
        arrays::{Constant, ConstantArray, SharedArray, Slice, SliceArray},
        buffer::BufferHandle,
        dtype::DType,
        memory::MemorySessionExt as _,
        serde::ArrayChildren,
        session::ArraySessionExt as _,
    },
    buffer::{Buffer, ByteBuffer},
    encodings::{
        alp::{ALP, ALPRD, ALPRDArrayExt as _, ALPRDData},
        fastlanes::BitPacked,
        sparse::{Sparse, SparseData, SparseExt as _},
    },
    error::{VortexResult, vortex_err},
};

#[path = "resident_memory_encoded_values.rs"]
mod values;

#[path = "resident_memory_record_views.rs"]
mod record_views;

#[cfg(test)]
#[path = "resident_memory_encoded_transfer_tests.rs"]
mod tests;

const MAX_ENCODING_DEPTH: usize = 128;
// Covers a BTree node's keys/values, child pointers and underfilled-node slack.
// The two array maps and buffer map are reserved before insertion. Copy-cost
// inspection temporarily uses the buffer map's credit before that map exists.
const MAP_ENTRY_BYTES: u64 = 1024;

#[derive(Clone, Copy)]
pub(super) enum TransferPolicy {
    PreserveEncodings,
    MinimizeFixedWidthCopies,
}

#[derive(Clone, Copy)]
enum Kind {
    Constant,
    Sparse,
    Slice,
    Shared,
    AlpRd,
    Parts,
    Serialized,
}

struct Node {
    source: ArrayRef,
    kind: Kind,
    metadata_limit: u64,
}

struct Plan {
    nodes: BTreeMap<usize, Node>,
    order: ReservedVec<usize>,
    bookkeeping: MemoryLease,
    metadata: MemoryLease,
    scratch: MemoryLease,
    reused_array_references: u64,
}

impl Plan {
    fn new(context: &NativeExecutionContext<'_>) -> Result<Self> {
        Ok(Self {
            nodes: BTreeMap::new(),
            order: ReservedVec::new(context.memory())?,
            bookkeeping: context.memory().reserve(0)?,
            metadata: context.memory().reserve(0)?,
            scratch: context.memory().reserve(0)?,
            reused_array_references: 0,
        })
    }

    fn visit(
        &mut self,
        array: &ArrayRef,
        depth: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        context.check_cancelled()?;
        if depth > MAX_ENCODING_DEPTH {
            return Err(memory_error("native transfer encoding depth exceeds 128"));
        }
        if self.nodes.contains_key(&array.addr()) {
            self.reused_array_references = add(self.reused_array_references, 1)?;
            return Ok(true);
        }
        let kind = match array.encoding_id().as_ref() {
            "vortex.constant" => Kind::Constant,
            "vortex.sparse" => Kind::Sparse,
            "vortex.slice" => Kind::Slice,
            "vortex.shared" => Kind::Shared,
            "vortex.alprd" => Kind::AlpRd,
            "vortex.zstd" | "vortex.alp" | "fastlanes.bitpacked" => Kind::Parts,
            "vortex.null"
            | "vortex.bool"
            | "vortex.primitive"
            | "vortex.decimal"
            | "vortex.varbin"
            | "vortex.varbinview"
            | "vortex.list"
            | "vortex.listview"
            | "vortex.fixed_size_list"
            | "vortex.struct"
            | "vortex.ext"
            | "vortex.dict"
            | "vortex.chunked"
            | "vortex.runend"
            | "vortex.sequence"
            | "vortex.zigzag"
            | "vortex.fsst"
            | "vortex.bytebool"
            | "fastlanes.for"
            | "fastlanes.rle"
            | "fastlanes.delta"
            | "vortex.masked" => Kind::Serialized,
            // Classification precedes execution. This preserves the existing
            // bounded native materializer for other admitted physical providers.
            _ => return Ok(false),
        };
        let scalar_shape = match kind {
            Kind::Constant => values::shape(array.as_::<Constant>().scalar().value(), context)?,
            Kind::Sparse => values::shape(array.as_::<Sparse>().fill_scalar().value(), context)?,
            _ => values::Shape::default(),
        };
        let buffers = if matches!(kind, Kind::Constant | Kind::Sparse) {
            scalar_shape.buffers
        } else if matches!(kind, Kind::AlpRd) {
            1 // ALPRD stores its dictionary outside the ordinary buffer slots.
        } else {
            array.nbuffers() as u64
        };
        let slots = array.slots().len() as u64;
        // Pinned serializers have fixed scalar metadata. Checked parts preserve
        // Zstd frame coordinates and patch caches; ALPRD's dictionary is copied
        // directly. Constants and sparse fills never synthesize protobuf.
        let metadata_limit = add(1024, mul(buffers, 64)?)?;
        let schema_bytes = native_payload_schema::encoded_schema_bytes(array.dtype())?;
        let retained = add(
            schema_bytes,
            add(
                scalar_shape.metadata_bytes,
                add(
                    1024,
                    add(
                        mul(slots, 128)?,
                        add(mul(buffers, 256)?, mul(metadata_limit, 2)?)?,
                    )?,
                )?,
            )?,
        )?;
        // All transient serializer/provider vectors, including Chunked's cached
        // offsets, are admitted before any serializer or buffer accessor runs.
        let scratch = add(
            mul(metadata_limit, 4)?,
            add(mul(slots, 128)?, mul(buffers, 256)?)?,
        )?;
        self.metadata
            .resize(add(self.metadata.bytes(), retained)?)?;
        self.scratch.resize(self.scratch.bytes().max(scratch))?;
        self.bookkeeping.resize(add(
            self.bookkeeping.bytes(),
            mul(add(buffers, 2)?, MAP_ENTRY_BYTES)?,
        )?)?;
        self.nodes.insert(
            array.addr(),
            Node {
                source: array.clone(),
                kind,
                metadata_limit,
            },
        );
        for child in array.children_iter() {
            if !self.visit(child, depth + 1, context)? {
                return Ok(false);
            }
        }
        self.order.push(array.addr())?;
        Ok(true)
    }

    fn fixed_width_copy_expands(
        &self,
        arrays: &[ArrayRef],
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        let Some(dense_bytes) = fixed_width_record_bytes(arrays)? else {
            return Ok(false);
        };
        // No copied buffers exist yet. This set borrows their already-admitted
        // bookkeeping capacity and is dropped before the copy phase starts.
        let mut seen = BTreeSet::new();
        let mut physical_bytes = 0;
        for node in self.nodes.values() {
            context.check_cancelled()?;
            match node.kind {
                // Integer/bool constants and sparse fills have no payload buffer.
                Kind::Constant | Kind::Sparse => {}
                Kind::AlpRd => {
                    let buffer = node
                        .source
                        .as_::<ALPRD>()
                        .left_parts_dictionary()
                        .clone()
                        .into_byte_buffer();
                    if seen.insert(buffer_identity(&buffer)) {
                        physical_bytes = add(physical_bytes, buffer.len() as u64)?;
                    }
                }
                _ => {
                    for buffer in node.source.buffer_handles() {
                        let host = buffer.as_host_opt().ok_or_else(|| {
                            memory_error("native transfer requires host-resident buffers")
                        })?;
                        if seen.insert(buffer_identity(host)) {
                            physical_bytes = add(physical_bytes, host.len() as u64)?;
                        }
                    }
                }
            }
        }
        Ok(physical_bytes > dense_bytes)
    }
}

fn fixed_width_record_bytes(arrays: &[ArrayRef]) -> Result<Option<u64>> {
    let mut bytes = 0;
    for array in arrays {
        let DType::Struct(fields, _) = array.dtype() else {
            return Ok(None);
        };
        for dtype in fields.fields() {
            let payload = match dtype {
                DType::Primitive(ptype, _) if ptype.is_int() => {
                    mul(array.len() as u64, ptype.byte_width() as u64)?
                }
                DType::Bool(_) => array.len().div_ceil(8) as u64,
                // The existing materializer validates values. Keep floats,
                // decimals, extensions and nested payloads on their admitted
                // physical path so a size choice cannot narrow their semantics.
                _ => return Ok(None),
            };
            bytes = add(bytes, payload)?;
            if dtype.is_nullable() {
                bytes = add(bytes, array.len().div_ceil(8) as u64)?;
            }
        }
    }
    Ok(Some(bytes))
}

fn buffer_identity(input: &ByteBuffer) -> (usize, usize, usize) {
    (
        input.as_slice().as_ptr() as usize,
        input.len(),
        *input.alignment(),
    )
}

pub(super) struct CopiedRecords {
    pub(super) arrays: ReservedVec<(ArrayRef, u64)>,
    pub(super) report: NativeTransferReport,
}

/// `None` is a preclassified materialization strategy, never an error retry.
pub(super) fn copy_records(
    arrays: &[ArrayRef],
    context: &NativeExecutionContext<'_>,
    policy: TransferPolicy,
) -> Result<Option<CopiedRecords>> {
    context.check_cancelled()?;
    let Some(records) = record_views::collect(arrays, context)? else {
        return Ok(None);
    };
    let arrays = &records.roots.values;
    let mut plan = Plan::new(context)?;
    for array in arrays {
        if !plan.visit(array, 0, context)? {
            return Ok(None);
        }
    }
    if matches!(policy, TransferPolicy::MinimizeFixedWidthCopies)
        && plan.fixed_width_copy_expands(arrays, context)?
    {
        return Ok(None);
    }
    let Plan {
        nodes,
        order,
        bookkeeping: _bookkeeping,
        metadata,
        scratch,
        reused_array_references,
    } = plan;
    let report = NativeTransferReport {
        encoded_array_nodes_copied: nodes.len() as u64,
        reused_array_references,
        retained_metadata_bytes: metadata.bytes(),
        provider_scratch_peak_reserved_bytes: scratch.bytes(),
        ..NativeTransferReport::default()
    };
    let metadata = Arc::new(metadata);
    let mut copier = Copier {
        context,
        metadata,
        arrays: BTreeMap::new(),
        buffers: BTreeMap::new(),
        report,
    };
    for id in &order.values {
        let node = nodes
            .get(id)
            .ok_or_else(|| memory_error("native transfer plan omitted a node"))?;
        let (array, bytes) = copier.copy_node(node)?;
        copier.arrays.insert(*id, (array, bytes));
    }
    let mut output = ReservedVec::new(context.memory())?;
    output.reserve(arrays.len())?;
    for input in arrays {
        output.values.push(
            copier
                .arrays
                .get(&input.addr())
                .cloned()
                .ok_or_else(|| memory_error("native transfer omitted a record"))?,
        );
    }
    context.check_cancelled()?;
    Ok(Some(CopiedRecords {
        arrays: output,
        report: copier.report,
    }))
}

struct Copier<'a, 's> {
    context: &'a NativeExecutionContext<'s>,
    // All reconstructed nodes and copied buffers retain this transfer's bounded
    // metadata credit. A bare dtype or encoding-specific Rust data clone is not
    // an independently admitted public result.
    metadata: Arc<MemoryLease>,
    arrays: BTreeMap<usize, (ArrayRef, u64)>,
    buffers: BTreeMap<(usize, usize, usize), ByteBuffer>,
    report: NativeTransferReport,
}

impl Copier<'_, '_> {
    fn copy_buffer(&mut self, input: &ByteBuffer) -> Result<ByteBuffer> {
        let context = self.context;
        self.copy_buffer_with_check(input, || context.check_cancelled())
    }

    fn copy_buffer_with_check(
        &mut self,
        input: &ByteBuffer,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<ByteBuffer> {
        checkpoint()?;
        let key = buffer_identity(input);
        if let Some(copied) = self.buffers.get(&key) {
            self.report.reused_buffer_references = add(self.report.reused_buffer_references, 1)?;
            return Ok(copied.clone());
        }
        let mut output = self
            .context
            .native_session()
            .allocator()
            .allocate(input.len(), input.alignment())
            .map_err(native_error)?;
        for (source, destination) in input
            .as_slice()
            .chunks(64 << 10)
            .zip(output.as_mut_slice().chunks_mut(64 << 10))
        {
            checkpoint()?;
            destination.copy_from_slice(source);
        }
        checkpoint()?;
        let output =
            crate::owned_buffers::retain_shared_credit(output.freeze(), Arc::clone(&self.metadata));
        self.report.encoded_buffer_bytes_copied =
            add(self.report.encoded_buffer_bytes_copied, input.len() as u64)?;
        self.report.encoded_buffer_copies = add(self.report.encoded_buffer_copies, 1)?;
        self.buffers.insert(key, output.clone());
        Ok(output)
    }

    fn copy_node(&mut self, node: &Node) -> Result<(ArrayRef, u64)> {
        self.context.check_cancelled()?;
        let input = &node.source;
        let dtype = values::copy_dtype(input.dtype())?;
        let mut children = Vec::with_capacity(input.nchildren());
        let mut bytes = 0;
        for child in input.children_iter() {
            let (copied, child_bytes) = self
                .arrays
                .get(&child.addr())
                .ok_or_else(|| memory_error("native transfer child order is inconsistent"))?;
            children.push(copied.clone());
            bytes = add(bytes, *child_bytes)?;
        }
        let output = match node.kind {
            Kind::Constant => {
                let constant = input.as_::<Constant>();
                let scalar = constant.scalar();
                let (scalar, scalar_bytes) = self.copy_scalar(scalar, dtype)?;
                bytes = add(bytes, scalar_bytes)?;
                ConstantArray::new(scalar, input.len()).into_array()
            }
            Kind::Sparse => {
                let sparse = input.as_::<Sparse>();
                let (fill, scalar_bytes) = self.copy_scalar(sparse.fill_scalar(), dtype.clone())?;
                bytes = add(bytes, scalar_bytes)?;
                // This constructor extracts scalar patch coordinates only.
                // All retained children come from the receiving copies below.
                let data = SparseData::try_new_from_patches(sparse.patches(), fill)
                    .map_err(native_error)?;
                finish_parts(
                    ArrayParts::new(Sparse, dtype.clone(), input.len(), data),
                    dtype,
                    input,
                    children,
                )?
            }
            Kind::AlpRd => {
                let alprd = input.as_::<ALPRD>();
                let dictionary = alprd.left_parts_dictionary().clone().into_byte_buffer();
                bytes = add(bytes, dictionary.len() as u64)?;
                let alignment = dictionary.alignment();
                let dictionary =
                    Buffer::from_byte_buffer_aligned(self.copy_buffer(&dictionary)?, alignment);
                let data = ALPRDData::new(
                    dictionary,
                    alprd.right_bit_width(),
                    alprd.left_parts_patches(),
                );
                finish_parts(
                    ArrayParts::new(ALPRD, dtype.clone(), input.len(), data),
                    dtype,
                    input,
                    children,
                )?
            }
            Kind::Slice => {
                let child = children
                    .first()
                    .cloned()
                    .ok_or_else(|| memory_error("native slice transfer omitted its child"))?;
                SliceArray::try_new(child, input.as_::<Slice>().slice_range().clone())
                    .map_err(native_error)?
                    .into_array()
            }
            Kind::Shared => {
                let child = children
                    .first()
                    .cloned()
                    .ok_or_else(|| memory_error("native shared transfer omitted its child"))?;
                // Source caches can own foreign canonical arrays or errors.
                // Retain the copied encoded child with a fresh receiving cache.
                SharedArray::new(child).into_array()
            }
            Kind::Serialized | Kind::Parts => {
                let (output, buffer_bytes) = self.copy_buffered_node(node, dtype, children)?;
                bytes = add(bytes, buffer_bytes)?;
                output
            }
        };
        if output.dtype() != input.dtype()
            || output.len() != input.len()
            || output.encoding_id() != input.encoding_id()
        {
            return Err(memory_error(
                "native transfer provider changed dtype, length or encoding",
            ));
        }
        // No source statistics are installed: they may own foreign scalar bytes.
        let output = output
            .with_retained_owner(self.metadata.clone())
            .map_err(native_error)?;
        self.context.check_cancelled()?;
        Ok((output, bytes))
    }

    fn copy_buffered_node(
        &mut self,
        node: &Node,
        dtype: DType,
        children: Vec<ArrayRef>,
    ) -> Result<(ArrayRef, u64)> {
        let input = &node.source;
        let source_buffers = input.buffer_handles();
        let mut buffers = Vec::with_capacity(source_buffers.len());
        let mut bytes = 0;
        for buffer in &source_buffers {
            let host = buffer
                .as_host_opt()
                .ok_or_else(|| memory_error("native transfer requires host-resident buffers"))?;
            bytes = add(bytes, host.len() as u64)?;
            buffers.push(BufferHandle::new_host(self.copy_buffer(host)?));
        }
        let output = if matches!(node.kind, Kind::Parts) {
            // These pinned data objects carry slice/patch coordinates that their
            // serializers omit or reset. Replace payloads and keep those coordinates.
            if input.is::<ALP>() {
                let parts = ALP
                    .with_buffers(input.as_::<ALP>(), &buffers)
                    .map_err(native_error)?;
                finish_parts(parts, dtype, input, children)?
            } else if input.is::<BitPacked>() {
                let parts = BitPacked
                    .with_buffers(input.as_::<BitPacked>(), &buffers)
                    .map_err(native_error)?;
                finish_parts(parts, dtype, input, children)?
            } else {
                let parts = vortex_zstd::Zstd
                    .with_buffers(input.as_::<vortex_zstd::Zstd>(), &buffers)
                    .map_err(native_error)?;
                finish_parts(parts, dtype, input, children)?
            }
        } else {
            let plugin = self
                .context
                .native_session()
                .arrays()
                .registry()
                .get(&input.encoding_id())
                .ok_or_else(|| memory_error("native transfer provider is not registered"))?;
            let metadata = plugin
                .serialize(input, self.context.native_session())
                .map_err(native_error)?
                .ok_or_else(|| {
                    memory_error("admitted native transfer provider omitted serialization")
                })?;
            if metadata.capacity() as u64 > node.metadata_limit {
                return Err(memory_error(
                    "native transfer metadata exceeded its pinned provider bound",
                ));
            }
            plugin
                .deserialize(
                    &dtype,
                    input.len(),
                    &metadata,
                    &buffers,
                    &CheckedChildren(&children),
                    self.context.native_session(),
                )
                .map_err(native_error)?
        };
        Ok((output, bytes))
    }
}

fn finish_parts<V: VTable>(
    mut parts: ArrayParts<V>,
    dtype: DType,
    input: &ArrayRef,
    children: Vec<ArrayRef>,
) -> Result<ArrayRef> {
    if parts.retained_owner.is_some() {
        return Err(memory_error(
            "native transfer parts unexpectedly retain a source owner",
        ));
    }
    parts.dtype = dtype;
    let mut copied = children.into_iter();
    parts.slots = input
        .slots()
        .iter()
        .map(|slot| slot.as_ref().and_then(|_| copied.next()))
        .collect();
    Array::<V>::try_from_parts(parts)
        .map(vortex::array::IntoArray::into_array)
        .map_err(native_error)
}

struct CheckedChildren<'a>(&'a [ArrayRef]);

impl ArrayChildren for CheckedChildren<'_> {
    fn get(&self, index: usize, dtype: &DType, len: usize) -> VortexResult<ArrayRef> {
        let child = self
            .0
            .get(index)
            .ok_or_else(|| vortex_err!("native transfer child index exceeds the copied tree"))?;
        if child.dtype() != dtype || child.len() != len {
            return Err(vortex_err!(
                "native transfer child schema or length changed"
            ));
        }
        Ok(child.clone())
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| memory_error("native transfer size overflow"))
}

fn mul(left: u64, right: u64) -> Result<u64> {
    left.checked_mul(right)
        .ok_or_else(|| memory_error("native transfer size overflow"))
}
