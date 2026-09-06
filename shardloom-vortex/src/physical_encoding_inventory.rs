//! Explicit inspection of persisted arrays, separate from metadata-only layout
//! inventory and ingestion timing. Reads encoded segments through Vortex's
//! native deserializer without canonicalizing or materializing their values.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use vortex::{
    array::serde::SerializedArray,
    error::{VortexResult, vortex_err},
    file::VortexFile,
    layout::{LayoutChildType, LayoutRef, layouts::flat::Flat},
};

/// Work limits for an explicit physical inspection. These bound requested
/// segment bytes and visited metadata nodes, not provider allocations or RSS.
#[derive(Debug, Clone, Copy)]
pub struct PhysicalEncodingInspectionLimits {
    /// Maximum number of layout nodes visited, including repeated references.
    pub max_layout_nodes: usize,
    /// Maximum number of Flat references inspected, including shared segments.
    pub max_flat_references: usize,
    /// Maximum serialized bytes in one segment, checked before requesting it.
    pub max_segment_bytes: u64,
    /// Maximum total serialized bytes requested, including repeated references.
    pub max_total_segment_bytes: u64,
    /// Maximum total array nodes visited in deserialized physical trees.
    pub max_array_nodes: usize,
}

impl Default for PhysicalEncodingInspectionLimits {
    fn default() -> Self {
        Self {
            max_layout_nodes: 1_000_000,
            max_flat_references: 100_000,
            max_segment_bytes: 128 << 20,
            max_total_segment_bytes: 32 << 30,
            max_array_nodes: 2_000_000,
        }
    }
}

#[derive(Default)]
struct Column {
    segments: BTreeMap<u32, u64>,
    encodings: BTreeSet<String>,
    flat_references: usize,
}

struct Pending {
    layout: LayoutRef,
    column: Vec<String>,
    auxiliary: Vec<String>,
    layout_path: Vec<String>,
}

/// Inspect actual serialized Flat array trees under their native layout fields.
///
/// Flat composite arrays without a field layout remain explicitly unattributed;
/// physical array-child names are not treated as logical column identities.
/// Shared segment bytes are counted once globally and once per referencing
/// column/role, so column totals are explicitly non-additive.
///
/// # Errors
/// Fails on corrupt/unsupported native metadata or any exceeded work limit.
/// Returns no successful partial inventory and never invokes another engine.
#[allow(clippy::too_many_lines)]
pub async fn inspect_physical_encodings(
    file: &VortexFile,
    limits: PhysicalEncodingInspectionLimits,
) -> VortexResult<Value> {
    let mut pending = vec![Pending {
        layout: file.footer().layout().clone(),
        column: Vec::new(),
        auxiliary: Vec::new(),
        layout_path: Vec::new(),
    }];
    let mut visited = 0_usize;
    let mut array_nodes = 0_usize;
    let mut requested_bytes = 0_u64;
    let mut segments = BTreeMap::<u32, u64>::new();
    let mut columns = BTreeMap::<(Vec<String>, Vec<String>), Column>::new();
    let mut references = Vec::new();
    let mut non_flat_segment_references = Vec::new();
    while let Some(mut item) = pending.pop() {
        visited = visited
            .checked_add(1)
            .ok_or_else(|| vortex_err!("layout count overflow"))?;
        if visited > limits.max_layout_nodes {
            return Err(vortex_err!(
                "physical encoding inspection layout-node limit exceeded"
            ));
        }
        item.layout_path.push(item.layout.encoding_id().to_string());
        if let Some(flat) = item.layout.as_opt::<Flat>() {
            if references.len() >= limits.max_flat_references {
                return Err(vortex_err!(
                    "physical encoding inspection Flat-reference limit exceeded"
                ));
            }
            let id = flat.segment_id();
            let spec = file
                .footer()
                .segment_map()
                .get(*id as usize)
                .ok_or_else(|| vortex_err!("physical encoding inspection missing segment {id}"))?;
            let bytes = u64::from(spec.length);
            requested_bytes = requested_bytes
                .checked_add(bytes)
                .ok_or_else(|| vortex_err!("physical encoding inspection byte count overflow"))?;
            if bytes > limits.max_segment_bytes || requested_bytes > limits.max_total_segment_bytes
            {
                return Err(vortex_err!(
                    "physical encoding inspection segment-byte limit exceeded"
                ));
            }
            let segment = file.segment_source().request(id).await?;
            if segment.len() as u64 != bytes {
                return Err(vortex_err!(
                    "physical encoding inspection segment length mismatch"
                ));
            }
            let serialized = if let Some(tree) = flat.array_tree() {
                SerializedArray::from_flatbuffer_and_segment(tree.clone(), segment)?
            } else {
                SerializedArray::try_from(segment)?
            };
            let array = serialized.decode(
                flat.dtype(),
                usize::try_from(flat.row_count())?,
                flat.array_ctx(),
                file.session(),
            )?;
            let column = columns
                .entry((item.column.clone(), item.auxiliary.clone()))
                .or_default();
            column.segments.insert(*id, bytes);
            column.flat_references += 1;
            let mut nodes = Vec::new();
            let mut arrays = vec![(Vec::<String>::new(), array)];
            while let Some((path, array)) = arrays.pop() {
                array_nodes = array_nodes
                    .checked_add(1)
                    .ok_or_else(|| vortex_err!("array count overflow"))?;
                if array_nodes > limits.max_array_nodes {
                    return Err(vortex_err!(
                        "physical encoding inspection array-node limit exceeded"
                    ));
                }
                let encoding = array.encoding_id().to_string();
                column.encodings.insert(encoding.clone());
                nodes.push(json!({"physical_child_path": path, "encoding_id": encoding, "dtype": array.dtype().to_string(), "rows": array.len()}));
                for (name, child) in array.named_children().into_iter().rev() {
                    if arrays.len() >= limits.max_array_nodes.saturating_sub(array_nodes) {
                        return Err(vortex_err!(
                            "physical encoding inspection pending-array limit exceeded"
                        ));
                    }
                    let mut child_path = path.clone();
                    child_path.push(name);
                    arrays.push((child_path, child));
                }
            }
            let shared = segments.insert(*id, bytes).is_some();
            references.push(json!({
                "column_path": item.column, "auxiliary_path": item.auxiliary,
                "layout_path": item.layout_path, "segment_id": *id,
                "segment_offset": spec.offset, "segment_bytes": bytes,
                "repeated_segment_reference": shared, "rows": flat.row_count(),
                "stored_array_nodes": nodes,
            }));
        } else {
            for id in item.layout.segment_ids() {
                non_flat_segment_references.push(json!({"column_path": item.column, "auxiliary_path": item.auxiliary, "layout_path": item.layout_path, "segment_id": *id}));
            }
        }
        for slot in (0..item.layout.nslots()).rev() {
            let Some(child) = item.layout.slot(slot)? else {
                continue;
            };
            if pending.len() >= limits.max_layout_nodes.saturating_sub(visited) {
                return Err(vortex_err!(
                    "physical encoding inspection pending-layout limit exceeded"
                ));
            }
            let mut column = item.column.clone();
            let mut auxiliary = item.auxiliary.clone();
            let relation = item.layout.slot_type(slot).ok_or_else(|| {
                vortex_err!("physical encoding inspection missing child relationship")
            })?;
            match relation {
                LayoutChildType::Field(name) if auxiliary.is_empty() => {
                    column.push(name.to_string());
                }
                LayoutChildType::Field(name) => auxiliary.push(format!("field:{name}")),
                LayoutChildType::Auxiliary(name) => auxiliary.push(name.to_string()),
                LayoutChildType::Transparent(_) | LayoutChildType::Chunk(_) => {}
            }
            pending.push(Pending {
                layout: child,
                column,
                auxiliary,
                layout_path: item.layout_path.clone(),
            });
        }
    }
    let columns = columns
        .into_iter()
        .map(|((column, auxiliary), value)| {
            json!({
                "column_path": column, "auxiliary_path": auxiliary,
                "encoding_ids": value.encodings, "flat_references": value.flat_references,
                "unique_referenced_segments": value.segments.len(),
                "referenced_segment_bytes_non_additive": value.segments.values().sum::<u64>(),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": "shardloom_physical_encoding_inventory_v1",
        "provider_version": crate::UPSTREAM_VORTEX_PROVIDER_VERSION,
        "scope": "actual_serialized_flat_array_nodes;native_layout_field_attribution;encoded_segments_read;no_value_canonicalization",
        "byte_scope": "footer_segment_lengths;unique_globally;column_totals_non_additive_for_shared_segments;excludes_footer_padding_and_postscript",
        "limits_scope": "serialized_requests_and_visited_metadata_nodes_not_provider_allocations_or_rss",
        "complete_flat_inspection": true,
        "all_referenced_segments_are_flat": non_flat_segment_references.is_empty(),
        "layout_nodes": visited, "array_nodes": array_nodes,
        "segment_bytes_requested": requested_bytes,
        "unique_flat_segments": segments.len(),
        "unique_flat_segment_bytes": segments.values().sum::<u64>(),
        "columns": columns, "flat_references": references,
        "non_flat_segment_references_not_inspected": non_flat_segment_references,
        "limits": {"layout_nodes": limits.max_layout_nodes, "flat_references": limits.max_flat_references,
            "segment_bytes": limits.max_segment_bytes, "total_segment_bytes": limits.max_total_segment_bytes, "array_nodes": limits.max_array_nodes},
        "fallback_attempted": false, "external_engine_invoked": false,
    }))
}
