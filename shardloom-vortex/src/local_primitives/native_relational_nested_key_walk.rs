//! Ordered borrowed traversal for nested key validation, hashing and exact identity.

use super::{Children, Column, Container, Frames, Value, failed};
use crate::local_primitives::{native_relational_keys::KeyColumn, vortex_error};
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use std::hash::Hasher as _;
use vortex::array::dtype::DType;

trait Visit {
    fn before_container(&self, column: &Container, row: usize, root: bool) -> Result<()>;
    fn leaf(&mut self, key: &KeyColumn, row: usize, token: &CancellationToken) -> Result<()>;
    fn null(&mut self) -> Result<()>;
    fn structure(&mut self, column: &Container, count: usize) -> Result<()>;
    fn field(&mut self, column: &Container, index: usize) -> Result<()>;
    fn before_list(&mut self, _column: &Container) -> Result<()> {
        Ok(())
    }
    fn list(&mut self, column: &Container, count: usize) -> Result<()>;
}

struct Row {
    node: usize,
    row: usize,
}

enum Siblings {
    Struct {
        parent: usize,
        next: Option<usize>,
        field: usize,
        row: usize,
    },
    List {
        elements: usize,
        start: usize,
        next: usize,
        count: usize,
    },
}

impl Column {
    pub(in crate::local_primitives) fn validate_at(&self, row: usize) -> Result<()> {
        self.walk(row, &mut Validate).map(|_| ())
    }

    pub(in crate::local_primitives) fn hash_into(
        &self,
        row: usize,
        hash: &mut rustc_hash::FxHasher,
    ) -> Result<bool> {
        self.walk(row, &mut Hash(hash))
    }

    pub(in crate::local_primitives) fn write_exact_key(
        &self,
        row: usize,
        output: &mut impl std::fmt::Write,
    ) -> Result<()> {
        self.walk(row, &mut Exact(output, &self.cancellation))
            .map(|_| ())
    }

    fn walk(&self, row: usize, visitor: &mut impl Visit) -> Result<bool> {
        let mut frames = Frames::new(self.owner.pool())?;
        let mut next = Some(Row {
            node: self.root,
            row,
        });
        let mut root = true;
        loop {
            let mut siblings = if let Some(current) = next.take() {
                match &self.nodes.values[current.node].value {
                    Value::Leaf(key) => {
                        visitor.leaf(key, current.row, &self.cancellation)?;
                        continue;
                    }
                    Value::Container(column) => {
                        visitor.before_container(column, current.row, root)?;
                        self.cancellation.check()?;
                        if column.is_null(current.row)? {
                            visitor.null()?;
                            if root {
                                return Ok(false);
                            }
                            continue;
                        }
                        root = false;
                        match &column.children {
                            Children::Struct { first, count, .. } => {
                                visitor.structure(column, *count)?;
                                Siblings::Struct {
                                    parent: current.node,
                                    next: *first,
                                    field: 0,
                                    row: current.row,
                                }
                            }
                            Children::List {
                                coordinates,
                                elements,
                            } => {
                                visitor.before_list(column)?;
                                let (start, count) = coordinates
                                    .at(current.row)?
                                    .ok_or_else(|| failed("list key validity changed"))?;
                                visitor.list(column, count)?;
                                Siblings::List {
                                    elements: *elements,
                                    start,
                                    next: 0,
                                    count,
                                }
                            }
                        }
                    }
                }
            } else if let Some(siblings) = frames.pop() {
                siblings
            } else {
                return Ok(true);
            };
            if let Some(child) = siblings.next(self, visitor)? {
                if siblings.has_next() {
                    frames.push(siblings)?;
                }
                next = Some(child);
            }
        }
    }
}

impl Siblings {
    fn next(&mut self, owner: &Column, visitor: &mut impl Visit) -> Result<Option<Row>> {
        Ok(Some(match self {
            Self::Struct {
                parent,
                next,
                field,
                row,
            } => {
                let Some(node) = *next else {
                    return Ok(None);
                };
                owner.cancellation.check()?;
                let Value::Container(column) = &owner.nodes.values[*parent].value else {
                    unreachable!("a pending struct cursor owns a container");
                };
                visitor.field(column, *field)?;
                *field += 1;
                *next = owner.nodes.values[node].next_sibling;
                Row { node, row: *row }
            }
            Self::List {
                elements,
                start,
                next,
                count,
            } => {
                if *next == *count {
                    return Ok(None);
                }
                let index = *next;
                if index.is_multiple_of(1024) {
                    owner.cancellation.check()?;
                }
                *next += 1;
                Row {
                    node: *elements,
                    row: *start + index,
                }
            }
        }))
    }

    fn has_next(&self) -> bool {
        match self {
            Self::Struct { next, .. } => next.is_some(),
            Self::List { next, count, .. } => next < count,
        }
    }
}

fn hash_row(column: &Container, row: usize, root: bool) -> Result<()> {
    if !root && row >= column.rows {
        return Err(failed("hash row index exceeds key owner"));
    }
    Ok(())
}

fn names(column: &Container) -> Result<&vortex::array::dtype::FieldNames> {
    let DType::Struct(dtype, _) = &column.dtype else {
        return Err(failed("struct key dtype changed"));
    };
    Ok(dtype.names())
}

struct Validate;

impl Visit for Validate {
    fn before_container(&self, column: &Container, row: usize, root: bool) -> Result<()> {
        hash_row(column, row, root)
    }

    fn leaf(&mut self, key: &KeyColumn, row: usize, _: &CancellationToken) -> Result<()> {
        key.validate_at(row)
    }

    fn null(&mut self) -> Result<()> {
        Ok(())
    }
    fn structure(&mut self, _: &Container, _: usize) -> Result<()> {
        Ok(())
    }
    fn field(&mut self, _: &Container, _: usize) -> Result<()> {
        Ok(())
    }
    fn list(&mut self, _: &Container, _: usize) -> Result<()> {
        Ok(())
    }
}

struct Hash<'a>(&'a mut rustc_hash::FxHasher);

impl Visit for Hash<'_> {
    fn before_container(&self, column: &Container, row: usize, root: bool) -> Result<()> {
        hash_row(column, row, root)
    }

    fn leaf(&mut self, key: &KeyColumn, row: usize, _: &CancellationToken) -> Result<()> {
        key.hash_into(row, self.0).map(|_| ())
    }

    fn null(&mut self) -> Result<()> {
        self.0.write_u8(0);
        Ok(())
    }

    fn structure(&mut self, column: &Container, count: usize) -> Result<()> {
        self.0.write_u8(10);
        self.0.write_usize(count);
        names(column)?;
        Ok(())
    }

    fn field(&mut self, column: &Container, index: usize) -> Result<()> {
        let name: &str = names(column)?[index].as_ref();
        self.0.write_usize(name.len());
        self.0.write(name.as_bytes());
        Ok(())
    }

    fn before_list(&mut self, column: &Container) -> Result<()> {
        self.0.write_u8(if matches!(column.dtype, DType::List(..)) {
            11
        } else {
            12
        });
        Ok(())
    }

    fn list(&mut self, _: &Container, count: usize) -> Result<()> {
        self.0.write_usize(count);
        Ok(())
    }
}

struct Exact<'a, W>(&'a mut W, &'a CancellationToken);

impl<W: std::fmt::Write> Visit for Exact<'_, W> {
    fn before_container(&self, _: &Container, _: usize, root: bool) -> Result<()> {
        if !root {
            self.1.check()?;
        }
        Ok(())
    }

    fn leaf(&mut self, key: &KeyColumn, row: usize, token: &CancellationToken) -> Result<()> {
        key.write_exact_key(row, self.0, token)
    }

    fn null(&mut self) -> Result<()> {
        self.0.write_str("n;").map_err(vortex_error)
    }

    fn structure(&mut self, column: &Container, count: usize) -> Result<()> {
        write!(self.0, "S{count}:").map_err(vortex_error)?;
        names(column)?;
        Ok(())
    }

    fn field(&mut self, column: &Container, index: usize) -> Result<()> {
        let name: &str = names(column)?[index].as_ref();
        write!(self.0, "{}:{name}", name.len()).map_err(vortex_error)
    }

    fn list(&mut self, column: &Container, count: usize) -> Result<()> {
        write!(
            self.0,
            "{}{count}:",
            if matches!(column.dtype, DType::List(..)) {
                'L'
            } else {
                'F'
            }
        )
        .map_err(vortex_error)
    }
}
