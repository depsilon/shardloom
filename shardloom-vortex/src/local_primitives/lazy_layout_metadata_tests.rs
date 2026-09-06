use super::{
    DEFERRED_LAYOUT_INVENTORY, VortexLocalPrimitiveEmbeddedLayoutReport, VortexQueryPrimitiveKind,
};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicUsize, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::dtype::{DType, Nullability, PType},
    error::{VortexResult, vortex_err},
    file::{Footer, VortexFile},
    layout::{
        LayoutChildren, LayoutRef,
        layouts::{chunked::ChunkedLayout, flat::FlatLayout},
        segments::{SegmentFuture, SegmentId, SegmentSource},
    },
    session::{VortexSession, registry::ReadContext},
};

#[derive(Clone)]
struct LazyChildren {
    cached: Arc<[OnceLock<LayoutRef>]>,
    realized: Arc<AtomicUsize>,
    nested: bool,
    fail_last: bool,
}

impl LazyChildren {
    fn new(width: usize, realized: Arc<AtomicUsize>, nested: bool, fail_last: bool) -> Self {
        Self {
            cached: (0..width).map(|_| OnceLock::new()).collect(),
            realized,
            nested,
            fail_last,
        }
    }
}

impl LayoutChildren for LazyChildren {
    fn to_arc(&self) -> Arc<dyn LayoutChildren> {
        Arc::new(self.clone())
    }

    fn child(&self, index: usize, dtype: &DType) -> VortexResult<LayoutRef> {
        if self.fail_last && index + 1 == self.cached.len() {
            return Err(vortex_err!("injected malformed unrelated layout"));
        }
        Ok(self.cached[index]
            .get_or_init(|| {
                self.realized.fetch_add(1, Ordering::Relaxed);
                if self.nested && index == 1 {
                    ChunkedLayout::new(
                        1,
                        dtype.clone(),
                        Arc::new(Self::new(1, Arc::clone(&self.realized), false, false)),
                    )
                    .into_layout()
                } else {
                    FlatLayout::new(1, dtype.clone(), SegmentId::from(0), ReadContext::new([]))
                        .into_layout()
                }
            })
            .clone())
    }

    fn child_row_count(&self, _index: usize) -> u64 {
        1
    }

    fn nchildren(&self) -> usize {
        self.cached.len()
    }
}

struct NoSegmentReads;

impl SegmentSource for NoSegmentReads {
    fn request(&self, _id: SegmentId) -> SegmentFuture {
        panic!("layout metadata inspection must not request array segments")
    }
}

fn file(realized: &Arc<AtomicUsize>, fail_last: bool) -> VortexFile {
    let dtype = DType::Primitive(PType::I64, Nullability::Nullable);
    let root = ChunkedLayout::new(
        3,
        dtype,
        Arc::new(LazyChildren::new(3, Arc::clone(realized), true, fail_last)),
    )
    .into_layout();
    VortexFile::new(
        Footer::new(root, Arc::from([]), None, ReadContext::new([])),
        Arc::new(NoSegmentReads),
        VortexSession::default(),
    )
}

#[test]
fn query_metadata_does_not_realize_unrelated_layouts_and_explicit_inspection_is_complete() {
    let realized = Arc::new(AtomicUsize::new(0));
    let file = file(&realized, false);
    assert!(VortexLocalPrimitiveEmbeddedLayoutReport::inspect_file_layouts(&file, 1).is_err());
    assert_eq!(realized.load(Ordering::Relaxed), 0);
    for kind in [
        VortexQueryPrimitiveKind::CountAll,
        VortexQueryPrimitiveKind::CountWhere,
    ] {
        let report = VortexLocalPrimitiveEmbeddedLayoutReport::from_file(&file, kind, true, true);
        assert_eq!(realized.load(Ordering::Relaxed), 0);
        assert_eq!(report.footer_row_count, 3);
        assert_eq!(report.root_layout_encoding, "vortex.chunked");
        assert_eq!(report.layout_encoding_inventory, DEFERRED_LAYOUT_INVENTORY);
        assert!(
            report
                .layout_inventory_scope
                .contains("root_only;full_tree_deferred")
        );
        assert_eq!(report.layout_inventory_nodes_inspected, 1);
        assert_eq!(
            report.domain_dictionary_status,
            "dictionary_layout_availability_not_inspected"
        );
        assert!(
            report
                .per_column_metadata_contract
                .contains("encoding=layout_encoding_not_inspected")
        );
        assert!(!report.metadata_first_pruning_available);
        assert!(!report.metadata_first_pruning_consulted);
        assert!(!report.metadata_pruned_entire_input);
    }
    let inspected =
        VortexLocalPrimitiveEmbeddedLayoutReport::inspect_file_layouts(&file, 5).unwrap();
    assert_eq!(realized.load(Ordering::Relaxed), 4);
    assert_eq!(
        inspected.layout_encoding_inventory,
        "vortex.chunked,vortex.flat"
    );
    assert_eq!(inspected.layout_inventory_nodes_inspected, 5);
    assert_eq!(
        inspected.layout_inventory_scope,
        "complete_tree;explicit_metadata_inspection"
    );
    assert_eq!(
        inspected.planner_consumption_status,
        "explicit_layout_inspection_no_query_executed"
    );
    assert!(!inspected.metadata_first_pruning_consulted);
    // The actual provider cache remains reusable; inspection performs no rebuild.
    let second = VortexLocalPrimitiveEmbeddedLayoutReport::inspect_file_layouts(&file, 5).unwrap();
    assert_eq!(
        second.layout_encoding_inventory,
        inspected.layout_encoding_inventory
    );
    assert_eq!(realized.load(Ordering::Relaxed), 4);
}

#[test]
fn explicit_inspection_rejects_incomplete_or_over_budget_trees_without_changing_query_admission() {
    let realized = Arc::new(AtomicUsize::new(0));
    let file = file(&realized, true);
    let report = VortexLocalPrimitiveEmbeddedLayoutReport::from_file(
        &file,
        VortexQueryPrimitiveKind::CountAll,
        false,
        false,
    );
    assert_eq!(report.footer_row_count, 3);
    assert_eq!(realized.load(Ordering::Relaxed), 0);
    let error =
        VortexLocalPrimitiveEmbeddedLayoutReport::inspect_file_layouts(&file, 10).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected malformed unrelated layout")
    );
    assert!(VortexLocalPrimitiveEmbeddedLayoutReport::inspect_file_layouts(&file, 0).is_err());
    assert!(VortexLocalPrimitiveEmbeddedLayoutReport::inspect_file_layouts(&file, 1).is_err());
}

#[test]
fn partition_metadata_keeps_deferred_scope_and_counts_only_each_root() {
    let realized = Arc::new(AtomicUsize::new(0));
    let file = file(&realized, false);
    let report = VortexLocalPrimitiveEmbeddedLayoutReport::from_file(
        &file,
        VortexQueryPrimitiveKind::CountAll,
        false,
        false,
    );
    let mut partitioned = VortexLocalPrimitiveEmbeddedLayoutReport::partitioned();
    partitioned.merge_partition(&report).unwrap();
    partitioned.merge_partition(&report).unwrap();
    assert_eq!(
        partitioned.layout_inventory_scope,
        report.layout_inventory_scope
    );
    assert_eq!(partitioned.layout_inventory_nodes_inspected, 2);
    assert!(
        partitioned
            .layout_encoding_inventory
            .contains(DEFERRED_LAYOUT_INVENTORY)
    );
    assert_eq!(partitioned.footer_row_count, 6);
    assert_eq!(realized.load(Ordering::Relaxed), 0);
}

#[test]
fn deferred_aggregate_correlation_does_not_claim_a_missing_dictionary() {
    let realized = Arc::new(AtomicUsize::new(0));
    let file = file(&realized, false);
    let report = VortexLocalPrimitiveEmbeddedLayoutReport::from_file(
        &file,
        VortexQueryPrimitiveKind::SimpleAggregate,
        false,
        false,
    );
    let mut summary = serde_json::json!({
        "aggregate_materialized_accessor_columns": "renamed_column",
        "aggregate_accessor_blockers": "test_provider_boundary",
    })
    .to_string();
    super::annotate_simple_aggregate_layout_correlation_summary(&mut summary, &report).unwrap();
    let evidence: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        evidence["aggregate_accessor_layout_correlation_status"],
        "artifact_layout_inventory_deferred_accessor_materialized"
    );
    assert_eq!(
        evidence["aggregate_accessor_layout_correlation_dictionary_status"],
        "dictionary_layout_availability_not_inspected"
    );
    assert_eq!(realized.load(Ordering::Relaxed), 0);
}
