//! Ordered native construction with flat, admitted temporary cell owners.

use serde_json::Value as Json;
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{
    ArrayRef, IntoArray, VortexSessionExecute as _,
    arrays::{ExtensionArray, FixedSizeListArray, ListArray, StructArray},
    dtype::{DType, Nullability, PType, StructFields, extension::ExtDTypeRef},
    memory::{HostAllocatorRef, HostMemorySessionExt as _},
};

use super::super::{native_capacity::ReservedVec, result_batch::Value};
use super::{add, leaf, multiply, result_batch, validity, vortex_error};

type CellOwner = Option<usize>;

struct Inputs<'a> {
    root: &'a [Option<&'a Json>],
    temporary: ReservedVec<ReservedVec<Option<&'a Json>>>,
}

impl<'a> Inputs<'a> {
    fn get(&self, owner: CellOwner) -> &[Option<&'a Json>] {
        owner.map_or(self.root, |index| &self.temporary.values[index].values)
    }

    fn push(&mut self, values: ReservedVec<Option<&'a Json>>) -> Result<usize> {
        let index = self.temporary.values.len();
        self.temporary.push(values)?;
        Ok(index)
    }

    fn release(&mut self, index: usize) {
        assert_eq!(index + 1, self.temporary.values.len());
        drop(self.temporary.values.pop());
    }

    fn field(&mut self, owner: CellOwner, name: &str, memory: &LiveMemoryPool) -> Result<usize> {
        let cells = self.get(owner);
        let mut values = ReservedVec::new(memory)?;
        values.reserve(cells.len())?;
        for value in cells {
            values.values.push(
                value
                    .and_then(Json::as_object)
                    .and_then(|value| value.get(name)),
            );
        }
        self.push(values)
    }
}

impl Drop for Inputs<'_> {
    fn drop(&mut self) {
        while let Some(values) = self.temporary.values.pop() {
            drop(values);
        }
    }
}

enum Frame<'a> {
    Extension(&'a ExtDTypeRef),
    Struct {
        dtype: &'a DType,
        cells: CellOwner,
        columns: ReservedVec<ArrayRef>,
        child_cells: usize,
    },
    List {
        dtype: &'a DType,
        cells: CellOwner,
        offsets: ReservedVec<usize>,
        child_cells: usize,
    },
}

struct Pending<'a>(ReservedVec<Frame<'a>>);

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        while let Some(frame) = self.0.values.pop() {
            drop(frame);
        }
    }
}

pub(super) fn construct<'a>(
    dtype: &'a DType,
    cells: &'a [Option<&'a Json>],
    memory: &LiveMemoryPool,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let mut inputs = Inputs {
        root: cells,
        temporary: ReservedVec::new(memory)?,
    };
    let mut pending = Pending(ReservedVec::new(memory)?);
    let mut current = (dtype, None);
    loop {
        let mut complete = match enter(current, &mut inputs, &mut pending, memory, allocator)? {
            Entry::Complete(array) => array,
            Entry::Child(dtype, cells) => {
                current = (dtype, cells);
                continue;
            }
        };
        loop {
            let Some(frame) = pending.0.values.pop() else {
                return Ok(complete);
            };
            match frame {
                Frame::Extension(extension) => {
                    complete = ExtensionArray::try_new(extension.clone(), complete)
                        .map(IntoArray::into_array)
                        .map_err(vortex_error)?;
                }
                Frame::Struct {
                    dtype,
                    cells,
                    mut columns,
                    child_cells,
                } => {
                    columns.values.push(complete);
                    inputs.release(child_cells);
                    let DType::Struct(fields, _) = dtype else {
                        unreachable!()
                    };
                    if columns.values.len() < fields.nfields() {
                        let (child, child_cells) =
                            field(fields, columns.values.len(), cells, &mut inputs, memory)?;
                        pending.0.values.push(Frame::Struct {
                            dtype,
                            cells,
                            columns,
                            child_cells,
                        });
                        current = (child, Some(child_cells));
                        break;
                    }
                    complete = finish_struct(dtype, inputs.get(cells), columns, allocator)?;
                }
                Frame::List {
                    dtype,
                    cells,
                    offsets,
                    child_cells,
                } => {
                    let result =
                        finish_list(dtype, inputs.get(cells), complete, &offsets, allocator);
                    drop(offsets);
                    inputs.release(child_cells);
                    complete = result?;
                }
            }
        }
    }
}

enum Entry<'a> {
    Complete(ArrayRef),
    Child(&'a DType, CellOwner),
}

fn enter<'a>(
    (dtype, cells): (&'a DType, CellOwner),
    inputs: &mut Inputs<'a>,
    pending: &mut Pending<'a>,
    memory: &LiveMemoryPool,
    allocator: &HostAllocatorRef,
) -> Result<Entry<'a>> {
    match dtype {
        DType::Extension(extension)
            if crate::native_extensions::structured_payload_storage(dtype).is_some() =>
        {
            pending.0.push(Frame::Extension(extension))?;
            Ok(Entry::Child(extension.storage_dtype(), cells))
        }
        DType::Struct(fields, _) => {
            pending.0.reserve_one()?;
            let mut columns = ReservedVec::new(memory)?;
            columns.reserve(fields.nfields())?;
            if fields.nfields() == 0 {
                return finish_struct(dtype, inputs.get(cells), columns, allocator)
                    .map(Entry::Complete);
            }
            let (child, child_cells) = field(fields, 0, cells, inputs, memory)?;
            pending.0.values.push(Frame::Struct {
                dtype,
                cells,
                columns,
                child_cells,
            });
            Ok(Entry::Child(child, Some(child_cells)))
        }
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
            pending.0.reserve_one()?;
            let (values, offsets) = list_cells(dtype, inputs.get(cells), memory)?;
            let child_cells = inputs.push(values)?;
            pending.0.values.push(Frame::List {
                dtype,
                cells,
                offsets,
                child_cells,
            });
            Ok(Entry::Child(child, Some(child_cells)))
        }
        _ => {
            let cells = inputs.get(cells);
            result_batch::build_column(dtype, cells.len(), allocator, |row| leaf(dtype, cells[row]))
                .map(Entry::Complete)
        }
    }
}

fn field<'a>(
    fields: &'a StructFields,
    index: usize,
    cells: CellOwner,
    inputs: &mut Inputs<'a>,
    memory: &LiveMemoryPool,
) -> Result<(&'a DType, usize)> {
    // Resolve only this child, before preparing its cells, as in the original loop.
    let child = fields
        .field_dtypes()
        .nth(index)
        .expect("admitted field index");
    let child_cells = inputs.field(cells, fields.names()[index].as_ref(), memory)?;
    Ok((child, child_cells))
}

fn finish_struct(
    dtype: &DType,
    cells: &[Option<&Json>],
    columns: ReservedVec<ArrayRef>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let DType::Struct(fields, _) = dtype else {
        unreachable!()
    };
    let validity = validity(dtype, cells, allocator)?;
    let (columns, _container_credit) = columns.into_parts();
    StructArray::try_new(fields.names().clone(), columns, cells.len(), validity)
        .map(IntoArray::into_array)
        .map_err(vortex_error)
}

fn list_cells<'a>(
    dtype: &DType,
    cells: &[Option<&'a Json>],
    memory: &LiveMemoryPool,
) -> Result<(ReservedVec<Option<&'a Json>>, ReservedVec<usize>)> {
    let mut values = ReservedVec::new(memory)?;
    let mut offsets = ReservedVec::new(memory)?;
    offsets.reserve(add(cells.len(), 1)?)?;
    offsets.values.push(0usize);
    if let DType::FixedSizeList(_, size, _) = dtype {
        values.reserve(multiply(cells.len(), *size as usize)?)?;
    }
    for value in cells {
        let items = value.and_then(Json::as_array);
        let count = if let DType::FixedSizeList(_, size, _) = dtype {
            *size as usize
        } else {
            items.map_or(0, Vec::len)
        };
        values.reserve(count)?;
        if let Some(items) = items {
            values.values.extend(items.iter().map(Some));
        } else {
            let length = add(values.values.len(), count)?;
            values.values.resize(length, None);
        }
        offsets.values.push(values.values.len());
    }
    Ok((values, offsets))
}

fn finish_list(
    dtype: &DType,
    cells: &[Option<&Json>],
    child: ArrayRef,
    offsets: &ReservedVec<usize>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let validity = validity(dtype, cells, allocator)?;
    if let DType::FixedSizeList(_, size, _) = dtype {
        FixedSizeListArray::try_new(child, *size, validity, cells.len())
            .map(IntoArray::into_array)
            .map_err(vortex_error)
    } else {
        let offsets = result_batch::build_column(
            &DType::Primitive(PType::U64, Nullability::NonNullable),
            offsets.values.len(),
            allocator,
            |row| Ok(Value::UInt(offsets.values[row] as u64)),
        )?;
        ListArray::try_new(
            child,
            offsets,
            validity,
            &mut vortex::session::VortexSession::empty()
                .with_host_allocator(allocator.clone())
                .create_execution_ctx(),
        )
        .map(IntoArray::into_array)
        .map_err(vortex_error)
    }
}
