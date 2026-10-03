//! Terminal JSON traversal shared by bounded collection and streaming writers.
//! Native structural views retain arrays; no list/struct scalar tree is built.

use super::{native_list, native_payload, native_relational_batch::failed, vortex_error};
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::io::Write;
use vortex::array::{
    ArrayRef, Columnar, ExecutionCtx, IntoArray as _,
    arrays::{StructArray, struct_::StructArrayExt as _},
    dtype::{DType, FieldNames},
    validity::Validity,
};

pub(super) struct Column {
    node: Node,
    _metadata: MemoryLease,
}

#[derive(Clone, Copy, Default)]
pub(super) struct WriteCounts {
    pub(super) scalars: u64,
    pub(super) utf8_bytes: u64,
}

impl WriteCounts {
    fn add(&mut self, other: Self) -> Result<()> {
        self.scalars = self
            .scalars
            .checked_add(other.scalars)
            .ok_or_else(|| failed("JSON scalar count overflow"))?;
        self.utf8_bytes = self
            .utf8_bytes
            .checked_add(other.utf8_bytes)
            .ok_or_else(|| failed("JSON UTF-8 copy count overflow"))?;
        Ok(())
    }
}

enum Node {
    Scalar(ArrayRef),
    Struct {
        names: FieldNames,
        fields: Vec<Node>,
        validity: Validity,
    },
    List {
        column: native_list::Column,
        child: Box<Node>,
    },
}

impl Column {
    pub(super) fn new(
        array: &ArrayRef,
        execution: &mut ExecutionCtx,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        let bytes = if native_payload::is_nested(array.dtype()) {
            native_payload::metadata_bytes(array.dtype())?
        } else {
            1024
        };
        let metadata = memory.reserve(bytes)?;
        Ok(Self {
            node: Node::new(array, execution)?,
            _metadata: metadata,
        })
    }

    /// Return scalar materializations and UTF-8 payload bytes copied at this sink.
    pub(super) fn write(
        &self,
        row: usize,
        writer: &mut impl Write,
        execution: &mut ExecutionCtx,
        cancellation: &CancellationToken,
    ) -> Result<WriteCounts> {
        self.node.write(row, writer, execution, cancellation)
    }
}

impl Node {
    fn new(array: &ArrayRef, execution: &mut ExecutionCtx) -> Result<Self> {
        Ok(match array.dtype() {
            DType::Struct(..) => {
                let array = array
                    .clone()
                    .execute::<StructArray>(execution)
                    .map_err(vortex_error)?;
                Self::Struct {
                    names: array.names().clone(),
                    fields: array
                        .iter_unmasked_fields()
                        .map(|field| Self::new(field, execution))
                        .collect::<Result<_>>()?,
                    validity: array.struct_validity(),
                }
            }
            DType::List(..) | DType::FixedSizeList(..) => {
                let column = native_list::Column::new(array.clone(), execution)?;
                let child = Box::new(Self::new(&column.elements, execution)?);
                Self::List { column, child }
            }
            DType::Variant(_) => Self::Scalar(array.clone()),
            DType::Extension(_) => {
                Self::Scalar(super::result_batch::scalar_storage(array, execution)?)
            }
            _ => Self::Scalar(
                array
                    .clone()
                    .execute::<Columnar>(execution)
                    .map_err(vortex_error)?
                    .into_array(),
            ),
        })
    }

    fn write(
        &self,
        row: usize,
        writer: &mut impl Write,
        execution: &mut ExecutionCtx,
        cancellation: &CancellationToken,
    ) -> Result<WriteCounts> {
        cancellation.check()?;
        match self {
            Self::Scalar(array) => {
                let scalar = super::result_batch::scalar(array, row, execution)?;
                let utf8_bytes = super::collect::write_scalar_json(writer, &scalar)?;
                Ok(WriteCounts {
                    scalars: 1,
                    utf8_bytes,
                })
            }
            Self::Struct {
                names,
                fields,
                validity,
            } => {
                if !validity
                    .execute_is_valid(row, execution)
                    .map_err(vortex_error)?
                {
                    writer.write_all(b"null").map_err(vortex_error)?;
                    return Ok(WriteCounts {
                        scalars: 1,
                        utf8_bytes: 0,
                    });
                }
                writer.write_all(b"{").map_err(vortex_error)?;
                let mut counts = WriteCounts::default();
                for (index, (name, field)) in names.iter().zip(fields).enumerate() {
                    if index != 0 {
                        writer.write_all(b",").map_err(vortex_error)?;
                    }
                    let name: &str = name.as_ref();
                    serde_json::to_writer(&mut *writer, name).map_err(vortex_error)?;
                    writer.write_all(b":").map_err(vortex_error)?;
                    counts.add(field.write(row, writer, execution, cancellation)?)?;
                }
                writer.write_all(b"}").map_err(vortex_error)?;
                Ok(counts)
            }
            Self::List { column, child } => {
                let Some((start, count)) = column.coordinates(row, execution)? else {
                    writer.write_all(b"null").map_err(vortex_error)?;
                    return Ok(WriteCounts {
                        scalars: 1,
                        utf8_bytes: 0,
                    });
                };
                writer.write_all(b"[").map_err(vortex_error)?;
                let mut counts = WriteCounts::default();
                for index in 0..count {
                    if index != 0 {
                        writer.write_all(b",").map_err(vortex_error)?;
                    }
                    counts.add(child.write(start + index, writer, execution, cancellation)?)?;
                }
                writer.write_all(b"]").map_err(vortex_error)?;
                Ok(counts)
            }
        }
    }
}
