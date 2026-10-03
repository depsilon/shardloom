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

    /// Return the number of scalar leaves actually materialized at this sink.
    pub(super) fn write(
        &self,
        row: usize,
        writer: &mut impl Write,
        execution: &mut ExecutionCtx,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
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
    ) -> Result<u64> {
        cancellation.check()?;
        let add = |left: u64, right: u64| {
            left.checked_add(right)
                .ok_or_else(|| failed("JSON scalar count overflow"))
        };
        match self {
            Self::Scalar(array) => {
                let scalar = array.execute_scalar(row, execution).map_err(vortex_error)?;
                super::collect::write_scalar_json(writer, &scalar)?;
                Ok(1)
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
                    return Ok(1);
                }
                writer.write_all(b"{").map_err(vortex_error)?;
                let mut scalars = 0;
                for (index, (name, field)) in names.iter().zip(fields).enumerate() {
                    if index != 0 {
                        writer.write_all(b",").map_err(vortex_error)?;
                    }
                    let name: &str = name.as_ref();
                    serde_json::to_writer(&mut *writer, name).map_err(vortex_error)?;
                    writer.write_all(b":").map_err(vortex_error)?;
                    scalars = add(scalars, field.write(row, writer, execution, cancellation)?)?;
                }
                writer.write_all(b"}").map_err(vortex_error)?;
                Ok(scalars)
            }
            Self::List { column, child } => {
                let Some((start, count)) = column.coordinates(row, execution)? else {
                    writer.write_all(b"null").map_err(vortex_error)?;
                    return Ok(1);
                };
                writer.write_all(b"[").map_err(vortex_error)?;
                let mut scalars = 0;
                for index in 0..count {
                    if index != 0 {
                        writer.write_all(b",").map_err(vortex_error)?;
                    }
                    scalars = add(
                        scalars,
                        child.write(start + index, writer, execution, cancellation)?,
                    )?;
                }
                writer.write_all(b"]").map_err(vortex_error)?;
                Ok(scalars)
            }
        }
    }
}
