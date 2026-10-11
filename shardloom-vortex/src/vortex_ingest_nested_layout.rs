//! Preserve nested validity and full-domain timestamps at the provider writer seam.

use std::sync::Arc;
use vortex::{
    array::dtype::{DType, FieldPath, PType},
    compressor::BtrBlocksCompressorBuilder,
    editions::{ComponentKind, EditionSessionExt as _},
    layout::{
        LayoutStrategy, LayoutStrategyEncodingValidator,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy, compressed::CompressingStrategy,
            flat::writer::FlatLayoutStrategy,
        },
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

fn timestamp_writer(session: &VortexSession) -> Arc<dyn LayoutStrategy> {
    let allowed = session
        .enabled_component_ids(ComponentKind::Array)
        .into_iter()
        .collect();
    let compressor = BtrBlocksCompressorBuilder::default()
        .retain_allowed_encodings(&allowed)
        .build();
    Arc::new(
        CompressingStrategy::new(preserved_writer(session), compressor)
            // Retain the source-chunk and parent-validity boundary. Compression
            // executes on the supplied session, with one outstanding task per
            // field; no extra runtime or independent memory owner is created.
            .with_concurrency(1)
            // The compressor computes the statistics it uses, and the file
            // writer accumulates pruning statistics before this layout stage.
            .with_stats(&[]),
    )
}

pub(super) fn root_writer(
    dtype: &DType,
    session: &VortexSession,
) -> Option<Arc<dyn LayoutStrategy>> {
    is_full_domain_timestamp(dtype).then(|| timestamp_writer(session))
}

pub(super) fn field_writers(
    dtype: &DType,
    session: &VortexSession,
) -> Vec<(FieldPath, Arc<dyn LayoutStrategy>)> {
    let Some(fields) = dtype.as_struct_fields_opt().filter(|_| has_fields(dtype)) else {
        return Vec::new();
    };
    let writer = preserved_writer(session);
    let mut timestamp = None;
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .filter(|(_, dtype)| preserve_field(dtype))
        .map(|(name, dtype)| {
            let selected: &Arc<dyn LayoutStrategy> = if contains_full_domain_timestamp(&dtype) {
                timestamp.get_or_insert_with(|| timestamp_writer(session))
            } else {
                &writer
            };
            (FieldPath::from_name(name.clone()), Arc::clone(selected))
        })
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
        if dtype.as_struct_fields_opt().is_some_and(|fields| {
            fields
                .fields()
                .any(|child| is_nested(&child) && contains_full_domain_timestamp(&child))
        }) {
            compression.push_str(";nested_fields=timestamp_subtrees_native_btrblocks;other_nested_fields=preserved_uncompressed");
        } else {
            compression.push_str(";scope=scalar_fields;nested_fields=preserved_uncompressed");
        }
    }
    if contains_full_domain_timestamp(dtype) {
        layout.push_str(";timestamp_layout=chunked_flat_native_compressed;file_min_max=provider_exact_when_present;file_other_pruning_stats=retained;timestamp_provider=vortex_array_0.85.0_full_domain_microsecond_patch");
        coalescing.push_str(";timestamp_fields=source_chunks");
        compression.push_str(
            ";timestamp_fields=native_btrblocks;timestamp_compression_concurrency_per_field=1",
        );
    }
}
