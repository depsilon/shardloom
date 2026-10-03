//! Preserve nested validity through the pinned provider's field-writer seam.

use std::sync::Arc;
use vortex::{
    array::dtype::{DType, FieldPath},
    editions::{ComponentKind, EditionSessionExt as _},
    layout::{
        LayoutStrategy, LayoutStrategyEncodingValidator,
        layouts::{chunked::writer::ChunkedLayoutStrategy, flat::writer::FlatLayoutStrategy},
    },
    session::VortexSession,
};

fn is_nested(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::List(..) | DType::FixedSizeList(..) | DType::Struct(..)
    )
}

pub(super) fn has_fields(dtype: &DType) -> bool {
    dtype
        .as_struct_fields_opt()
        .is_some_and(|fields| fields.fields().any(|dtype| is_nested(&dtype)))
}

pub(super) fn field_writers(
    dtype: &DType,
    session: &VortexSession,
) -> Vec<(FieldPath, Arc<dyn LayoutStrategy>)> {
    let Some(fields) = dtype.as_struct_fields_opt().filter(|_| has_fields(dtype)) else {
        return Vec::new();
    };
    let writer: Arc<dyn LayoutStrategy> = Arc::new(ChunkedLayoutStrategy::new(
        LayoutStrategyEncodingValidator::new(
            FlatLayoutStrategy::default(),
            session
                .enabled_component_ids(ComponentKind::Array)
                .into_iter()
                .collect(),
        ),
    ));
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .filter(|(_, dtype)| is_nested(dtype))
        .map(|(name, _)| (FieldPath::from_name(name.clone()), Arc::clone(&writer)))
        .collect()
}

pub(super) fn append_evidence(
    dtype: &DType,
    layout: &mut String,
    coalescing: &mut String,
    compression: &mut String,
) {
    if has_fields(dtype) {
        layout.push_str(";nested_field_layout=chunked_flat_preserving_validity");
        coalescing.push_str(";scope=scalar_fields;nested_fields=source_chunks");
        compression.push_str(";scope=scalar_fields;nested_fields=preserved_uncompressed");
    }
}
