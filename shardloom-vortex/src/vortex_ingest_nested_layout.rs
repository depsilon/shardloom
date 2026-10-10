//! Preserve nested validity and full-domain timestamps at the provider writer seam.

use std::sync::Arc;
use vortex::{
    array::{
        dtype::{DType, FieldPath, PType},
        stats::PRUNING_STATS,
    },
    editions::{ComponentKind, EditionSessionExt as _},
    expr::stats::Stat,
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
        .is_some_and(|fields| fields.fields().any(|dtype| preserve_field(&dtype)))
}

fn is_full_domain_timestamp(dtype: &DType) -> bool {
    crate::native_temporal::temporal_storage(dtype) == Some(PType::I64)
}

fn preserve_field(dtype: &DType) -> bool {
    is_nested(dtype) || is_full_domain_timestamp(dtype)
}

fn preserved_writer(session: &VortexSession) -> Arc<dyn LayoutStrategy> {
    Arc::new(ChunkedLayoutStrategy::new(
        LayoutStrategyEncodingValidator::new(
            FlatLayoutStrategy::default(),
            session
                .enabled_component_ids(ComponentKind::Array)
                .into_iter()
                .collect(),
        ),
    ))
}

pub(super) fn root_writer(
    dtype: &DType,
    session: &VortexSession,
) -> Option<Arc<dyn LayoutStrategy>> {
    is_full_domain_timestamp(dtype).then(|| preserved_writer(session))
}

pub(super) fn field_writers(
    dtype: &DType,
    session: &VortexSession,
) -> Vec<(FieldPath, Arc<dyn LayoutStrategy>)> {
    let Some(fields) = dtype.as_struct_fields_opt().filter(|_| has_fields(dtype)) else {
        return Vec::new();
    };
    let writer = preserved_writer(session);
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .filter(|(_, dtype)| preserve_field(dtype))
        .map(|(name, _)| (FieldPath::from_name(name.clone()), Arc::clone(&writer)))
        .collect()
}

fn contains_full_domain_timestamp(dtype: &DType) -> bool {
    match dtype {
        DType::Struct(fields, _) => fields
            .fields()
            .any(|child| contains_full_domain_timestamp(&child)),
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
            contains_full_domain_timestamp(child)
        }
        _ => is_full_domain_timestamp(dtype),
    }
}

pub(super) fn file_statistics(
    options: vortex::file::VortexWriteOptions,
    dtype: &DType,
) -> vortex::file::VortexWriteOptions {
    if contains_full_domain_timestamp(dtype) {
        // Vortex 0.85 validates timestamp min/max scalars through a calendar
        // domain narrower than i64 microseconds, and may panic on valid storage.
        // Its public writer has only a file-wide statistics selection. Omit
        // extrema conservatively; never publish invented or narrowed bounds.
        options.with_file_statistics(
            PRUNING_STATS
                .iter()
                .copied()
                .filter(|stat| !matches!(stat, Stat::Min | Stat::Max))
                .collect(),
        )
    } else {
        options
    }
}

pub(super) fn append_evidence(
    dtype: &DType,
    layout: &mut String,
    coalescing: &mut String,
    compression: &mut String,
) {
    if dtype
        .as_struct_fields_opt()
        .is_some_and(|fields| fields.fields().any(|child| is_nested(&child)))
    {
        layout.push_str(";nested_field_layout=chunked_flat_preserving_validity");
        coalescing.push_str(";scope=scalar_fields;nested_fields=source_chunks");
        compression.push_str(";scope=scalar_fields;nested_fields=preserved_uncompressed");
    }
    if contains_full_domain_timestamp(dtype) {
        layout.push_str(";timestamp_layout=chunked_flat_preserving_storage;file_min_max=omitted_for_full_domain_timestamp;file_other_pruning_stats=retained");
        coalescing.push_str(";timestamp_fields=source_chunks");
        compression.push_str(";timestamp_fields=preserved_uncompressed");
    }
}
