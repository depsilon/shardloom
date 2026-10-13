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
    visit(dtype, 0, &mut 0)
}

fn visit(dtype: fd::DType<'_>, depth: usize, nodes: &mut usize) -> Result<()> {
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
            for child in types {
                visit(child, depth + 1, nodes)?;
            }
        }
        fd::Type::List => {
            let list = dtype
                .type__as_list()
                .ok_or_else(|| failed("missing list dtype"))?;
            visit(
                list.element_type()
                    .ok_or_else(|| failed("missing list element dtype"))?,
                depth + 1,
                nodes,
            )?;
        }
        fd::Type::FixedSizeList => {
            let list = dtype
                .type__as_fixed_size_list()
                .ok_or_else(|| failed("missing fixed-list dtype"))?;
            visit(
                list.element_type()
                    .ok_or_else(|| failed("missing fixed-list element dtype"))?,
                depth + 1,
                nodes,
            )?;
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
            visit(
                extension
                    .storage_dtype()
                    .ok_or_else(|| failed("missing extension storage dtype"))?,
                depth + 1,
                nodes,
            )?;
        }
        _ => return Err(failed("unsupported IPC dtype")),
    }
    Ok(())
}
