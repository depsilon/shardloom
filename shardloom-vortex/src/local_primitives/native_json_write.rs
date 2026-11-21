//! Borrowed terminal continuations preserve field and element write boundaries.

use std::io::Write;

use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{ExecutionCtx, dtype::FieldNames};

use super::{Node, ReservedVec, Tree, WriteCounts, vortex_error};

enum Frame<'a> {
    Struct {
        names: &'a FieldNames,
        fields: &'a [usize],
        row: usize,
        next: usize,
        counts: WriteCounts,
    },
    List {
        child: usize,
        start: usize,
        count: usize,
        next: usize,
        counts: WriteCounts,
    },
}

pub(super) fn write(
    tree: &Tree,
    row: usize,
    writer: &mut impl Write,
    execution: &mut ExecutionCtx,
    cancellation: &CancellationToken,
) -> Result<WriteCounts> {
    let mut pending = ReservedVec::new(&tree.memory)?;
    let mut current = (None, row);
    loop {
        cancellation.check()?;
        let (index, row) = current;
        let mut complete = match enter(tree, index, row, &mut pending, writer, execution)? {
            Entry::Complete(counts) => counts,
            Entry::Child(child, row) => {
                current = (Some(child), row);
                continue;
            }
        };
        loop {
            let Some(frame) = pending.values.pop() else {
                return Ok(complete);
            };
            match frame {
                Frame::Struct {
                    names,
                    fields,
                    row,
                    next,
                    mut counts,
                } => {
                    counts.add(complete)?;
                    if next < fields.len() {
                        writer.write_all(b",").map_err(vortex_error)?;
                        field_name(writer, names[next].as_ref())?;
                        current = (Some(fields[next]), row);
                        pending.values.push(Frame::Struct {
                            names,
                            fields,
                            row,
                            next: next + 1,
                            counts,
                        });
                        break;
                    }
                    writer.write_all(b"}").map_err(vortex_error)?;
                    complete = counts;
                }
                Frame::List {
                    child,
                    start,
                    count,
                    next,
                    mut counts,
                } => {
                    counts.add(complete)?;
                    if next < count {
                        writer.write_all(b",").map_err(vortex_error)?;
                        current = (Some(child), start + next);
                        pending.values.push(Frame::List {
                            child,
                            start,
                            count,
                            next: next + 1,
                            counts,
                        });
                        break;
                    }
                    writer.write_all(b"]").map_err(vortex_error)?;
                    complete = counts;
                }
            }
        }
    }
}

fn field_name(writer: &mut impl Write, name: &str) -> Result<()> {
    serde_json::to_writer(&mut *writer, name).map_err(vortex_error)?;
    writer.write_all(b":").map_err(vortex_error)
}

enum Entry {
    Complete(WriteCounts),
    Child(usize, usize),
}

fn enter<'a>(
    tree: &'a Tree,
    index: Option<usize>,
    row: usize,
    pending: &mut ReservedVec<Frame<'a>>,
    writer: &mut impl Write,
    execution: &mut ExecutionCtx,
) -> Result<Entry> {
    let complete = match index.map_or(&tree.root, |index| &tree.nodes.values[index]) {
        Node::Uuid(values) => {
            match values.value(row)? {
                None => writer.write_all(b"null").map_err(vortex_error)?,
                Some(value) => {
                    serde_json::to_writer(&mut *writer, &crate::native_uuid::display(value))
                        .map_err(vortex_error)?;
                }
            }
            WriteCounts {
                scalars: 1,
                utf8_bytes: 0,
            }
        }
        Node::Scalar(array) => {
            let scalar = super::super::result_batch::scalar(array, row, execution)?;
            let utf8_bytes = super::super::collect::write_scalar_json(writer, &scalar)?;
            WriteCounts {
                scalars: 1,
                utf8_bytes,
            }
        }
        Node::Struct {
            names,
            fields,
            validity,
        } => {
            if validity
                .execute_is_valid(row, execution)
                .map_err(vortex_error)?
            {
                let fields = &fields.values;
                if !fields.is_empty() {
                    pending.reserve_one()?;
                }
                writer.write_all(b"{").map_err(vortex_error)?;
                if let Some(&child) = fields.first() {
                    field_name(writer, names[0].as_ref())?;
                    pending.values.push(Frame::Struct {
                        names,
                        fields,
                        row,
                        next: 1,
                        counts: WriteCounts::default(),
                    });
                    return Ok(Entry::Child(child, row));
                }
                writer.write_all(b"}").map_err(vortex_error)?;
                WriteCounts::default()
            } else {
                writer.write_all(b"null").map_err(vortex_error)?;
                WriteCounts {
                    scalars: 1,
                    utf8_bytes: 0,
                }
            }
        }
        Node::List { column, child } => {
            if let Some((start, count)) = column.coordinates(row, execution)? {
                if count != 0 {
                    pending.reserve_one()?;
                }
                writer.write_all(b"[").map_err(vortex_error)?;
                if count != 0 {
                    pending.values.push(Frame::List {
                        child: *child,
                        start,
                        count,
                        next: 1,
                        counts: WriteCounts::default(),
                    });
                    return Ok(Entry::Child(*child, start));
                }
                writer.write_all(b"]").map_err(vortex_error)?;
                WriteCounts::default()
            } else {
                writer.write_all(b"null").map_err(vortex_error)?;
                WriteCounts {
                    scalars: 1,
                    utf8_bytes: 0,
                }
            }
        }
    };
    Ok(Entry::Complete(complete))
}
