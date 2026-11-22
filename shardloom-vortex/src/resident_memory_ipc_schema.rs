//! Validate all dtype views before the native provider can allocate lazy schema
//! nodes or intern an extension identifier.

use super::{MAX_DEPTH, MAX_NODES, failed};
use shardloom_core::Result;
use vortex::flatbuffers::dtype as fd;

pub(super) fn validate(dtype: fd::DType<'_>) -> Result<()> {
    let root = dtype
        .type__as_struct_()
        .ok_or_else(|| failed("IPC input requires a record dtype"))?;
    if root.nullable() {
        return Err(failed("IPC record rows must be nonnullable"));
    }
    visit(dtype, &mut 0)
}

fn visit(root: fd::DType<'_>, nodes: &mut usize) -> Result<()> {
    let mut stack = vec![(root, 0_usize)];
    while let Some((dtype, depth)) = stack.pop() {
        *nodes += 1;
        if depth > MAX_DEPTH || *nodes > MAX_NODES {
            return Err(failed("dtype exceeds the depth or node bound"));
        }
        match dtype.type_type() {
            fd::Type::Null
            | fd::Type::Bool
            | fd::Type::Primitive
            | fd::Type::Decimal
            | fd::Type::Utf8
            | fd::Type::Binary => {}
            fd::Type::Struct_ => {
                let fields = dtype
                    .type__as_struct_()
                    .ok_or_else(|| failed("missing struct dtype"))?;
                let names = fields
                    .names()
                    .ok_or_else(|| failed("missing struct field names"))?;
                let types = fields
                    .dtypes()
                    .ok_or_else(|| failed("missing struct field types"))?;
                if names.is_empty() || names.len() != types.len() || names.len() > MAX_NODES {
                    return Err(failed("invalid struct field count"));
                }
                for name in names {
                    if name.is_empty() {
                        return Err(failed("empty struct field name"));
                    }
                }
                stack
                    .try_reserve(types.len())
                    .map_err(|_| failed("dtype traversal workspace overflow"))?;
                for child in types.iter().rev() {
                    stack.push((child, depth + 1));
                }
            }
            fd::Type::List => {
                let list = dtype
                    .type__as_list()
                    .ok_or_else(|| failed("missing list dtype"))?;
                let child = list
                    .element_type()
                    .ok_or_else(|| failed("missing list element dtype"))?;
                stack
                    .try_reserve(1)
                    .map_err(|_| failed("dtype traversal workspace overflow"))?;
                stack.push((child, depth + 1));
            }
            fd::Type::FixedSizeList => {
                let list = dtype
                    .type__as_fixed_size_list()
                    .ok_or_else(|| failed("missing fixed-list dtype"))?;
                let child = list
                    .element_type()
                    .ok_or_else(|| failed("missing fixed-list element dtype"))?;
                stack
                    .try_reserve(1)
                    .map_err(|_| failed("dtype traversal workspace overflow"))?;
                stack.push((child, depth + 1));
            }
            fd::Type::Extension => {
                let extension = dtype
                    .type__as_extension()
                    .ok_or_else(|| failed("missing extension dtype"))?;
                if !matches!(extension.id(), Some("vortex.date" | "vortex.timestamp")) {
                    return Err(failed("unknown or unsupported dtype extension"));
                }
                if extension
                    .metadata()
                    .is_none_or(|metadata| metadata.len() > 128)
                {
                    return Err(failed("invalid temporal extension metadata"));
                }
                let child = extension
                    .storage_dtype()
                    .ok_or_else(|| failed("missing extension storage dtype"))?;
                stack
                    .try_reserve(1)
                    .map_err(|_| failed("dtype traversal workspace overflow"))?;
                stack.push((child, depth + 1));
            }
            _ => return Err(failed("unsupported IPC dtype")),
        }
    }
    Ok(())
}
