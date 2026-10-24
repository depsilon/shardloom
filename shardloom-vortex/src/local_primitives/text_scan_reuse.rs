//! Query-local ownership of one projected text conjunct's decoded scan range.
//!
//! The projection future owns each entry. The registry retains only weak handles,
//! so pruning, cancellation and errors retire entries without a later scan visit.

use std::{
    any::Any,
    collections::BTreeMap,
    ops::Range,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use shardloom_core::{PredicateExpr, StatValue};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use vortex::{
    array::{
        ArrayRef, IntoArray as _, MaskFuture, VortexSessionExecute as _,
        arrays::{
            Slice, StructArray, VarBinViewArray, slice::SliceArraySlotsExt as _,
            struct_::StructArrayExt as _,
        },
        dtype::{DType, FieldMask, Nullability},
        validity::Validity,
    },
    encodings::zstd::Zstd,
    error::{VortexResult, vortex_ensure, vortex_err},
    expr::{BoundExpression, root, select},
    file::VortexFile,
    layout::{ArrayFuture, LayoutReader, RowSplits, SplitRange, scan::scan_builder::ScanBuilder},
    mask::{AllOr, Mask},
    session::VortexSession,
};

use super::LocalVortexScanPlan;

#[cfg(all(test, feature = "vortex-write"))]
#[path = "../native_provider_text_scan_reuse_tests.rs"]
mod tests;

// A conservative per-entry allowance includes the Arc, mutex, ready descriptor,
// and a worst-case BTreeMap node. Payload buffers retain their own native leases.
const ENTRY_METADATA_BYTES: u64 = 4096;
const REGISTRY_METADATA_BYTES: u64 = 1024;

#[derive(Default)]
pub(super) struct Stats {
    decoded_chunks: AtomicU64,
    projected_reuses: AtomicU64,
    sparse_masks: AtomicU64,
}

impl Stats {
    pub(super) fn annotate(&self, summary: &mut String) -> super::Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary)
            .map_err(|error| super::ShardLoomError::InvalidOperation(error.to_string()))?;
        for (name, count) in [
            ("aggregate_text_scan_decoded_chunks", &self.decoded_chunks),
            (
                "aggregate_text_scan_projected_reuses",
                &self.projected_reuses,
            ),
            ("aggregate_text_scan_sparse_masks", &self.sparse_masks),
        ] {
            value[name] = count.load(Ordering::Relaxed).into();
        }
        value["aggregate_text_scan_scope"] = "query_owned_projected_text_conjunct;native_pruning_and_filter_order_unchanged;all_true_input_only;zstd_decode_shared_with_projection;final_mask_applied_after_conjunction;other_columns_read_after_filter;encoded_dictionary_paths_preserved;active_range_lifetime;no_cross_query_cache".into();
        *summary = value.to_string();
        Ok(())
    }
}

type ReusedScan = (ScanBuilder<ArrayRef>, Arc<Stats>);

fn projected_text_predicate<'a>(
    predicate: &'a PredicateExpr,
    plan: &LocalVortexScanPlan,
    dtype: &DType,
) -> Option<&'a PredicateExpr> {
    let column = match predicate {
        PredicateExpr::And(children) => {
            return children
                .iter()
                .find_map(|child| projected_text_predicate(child, plan, dtype));
        }
        PredicateExpr::Compare {
            column,
            value: StatValue::Utf8(_),
            ..
        }
        | PredicateExpr::StringContains { column, .. } => column,
        _ => return None,
    };
    (plan
        .projected_columns
        .iter()
        .any(|name| name == column.as_str())
        && dtype
            .as_struct_fields_opt()
            .and_then(|fields| fields.field(column.as_str()))
            .is_some_and(|dtype| matches!(dtype, DType::Utf8(_))))
    .then_some(predicate)
}

pub(super) fn admit(
    file: &VortexFile,
    plan: &LocalVortexScanPlan,
    predicate: Option<&PredicateExpr>,
    memory: Option<&LiveMemoryPool>,
    cancellation: Option<&CancellationToken>,
) -> VortexResult<Option<ReusedScan>> {
    let Some(predicate) =
        predicate.and_then(|predicate| projected_text_predicate(predicate, plan, file.dtype()))
    else {
        return Ok(None);
    };
    let (Some(memory), Some(_), Some(projection)) = (memory, &plan.filter, &plan.projection) else {
        return Ok(None);
    };
    if file.dtype().nullability() != Nullability::NonNullable {
        return Ok(None);
    }
    let column = predicate
        .column()
        .ok_or_else(|| vortex_err!("text scan predicate lost its column"))?;
    // The native scan still splits, prunes and orders the complete filter. Only
    // this exact leaf may share its decoded source with the later projection.
    let filter = super::predicate_to_vortex_expr(
        predicate,
        file.dtype(),
        crate::VortexQueryPrimitiveKind::SimpleAggregate,
    )
    .map_err(|error| vortex_err!(External: error))?;
    let projection = projection
        .optimize_recursive(file.dtype())?
        .bind(file.dtype())?;
    let text_projection = select([column.as_str()], root()).bind(file.dtype())?;
    let other_columns = plan
        .projected_columns
        .iter()
        .filter(|name| name.as_str() != column.as_str())
        .map(String::as_str)
        .collect::<Vec<_>>();
    let other_projection = if other_columns.is_empty() {
        None
    } else {
        Some(select(other_columns, root()).bind(file.dtype())?)
    };
    let projected_filter = filter
        .optimize_recursive(text_projection.dtype())?
        .bind(text_projection.dtype())?;
    let filter = filter
        .optimize_recursive(file.dtype())?
        .bind(file.dtype())?;
    let credit = crate::owned_buffers::reserve(memory, REGISTRY_METADATA_BYTES)?;
    let stats = Arc::new(Stats::default());
    let reader = Reader {
        inner: file.layout_reader()?,
        session: file.session().clone(),
        projection,
        text_projection,
        other_projection,
        filter,
        projected_filter,
        registry: Arc::new(Registry {
            entries: Mutex::new(BTreeMap::new()),
            memory: memory.clone(),
            _credit: credit,
        }),
        stats: Arc::clone(&stats),
        cancellation: cancellation.cloned(),
    };
    Ok(Some((
        ScanBuilder::new(file.session().clone(), Arc::new(reader)),
        stats,
    )))
}

struct Registry {
    entries: Mutex<BTreeMap<(u64, u64), Weak<Entry>>>,
    memory: LiveMemoryPool,
    _credit: MemoryLease,
}

struct Ready {
    array: ArrayRef,
    mask: Mask,
}

struct Entry {
    range: (u64, u64),
    ready: Mutex<Option<Ready>>,
    registry: Arc<Registry>,
    _credit: MemoryLease,
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.registry
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.range);
    }
}

impl Registry {
    fn insert(self: &Arc<Self>, range: &Range<u64>) -> VortexResult<Arc<Entry>> {
        let range = (range.start, range.end);
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| vortex_err!("text scan registry poisoned"))?;
        vortex_ensure!(
            !entries.contains_key(&range),
            "duplicate active text scan range"
        );
        let credit = crate::owned_buffers::reserve(&self.memory, ENTRY_METADATA_BYTES)?;
        let entry = Arc::new(Entry {
            range,
            ready: Mutex::new(None),
            registry: Arc::clone(self),
            _credit: credit,
        });
        entries.insert(range, Arc::downgrade(&entry));
        Ok(entry)
    }

    fn get(&self, range: &Range<u64>) -> VortexResult<Option<Arc<Entry>>> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| vortex_err!("text scan registry poisoned"))?
            .get(&(range.start, range.end))
            .and_then(Weak::upgrade))
    }
}

struct Reader {
    inner: Arc<dyn LayoutReader>,
    session: VortexSession,
    projection: BoundExpression,
    text_projection: BoundExpression,
    other_projection: Option<BoundExpression>,
    filter: BoundExpression,
    projected_filter: BoundExpression,
    registry: Arc<Registry>,
    stats: Arc<Stats>,
    cancellation: Option<CancellationToken>,
}

fn check_cancelled(token: Option<&CancellationToken>) -> VortexResult<()> {
    token.map_or(Ok(()), |token| {
        token.check().map_err(|error| vortex_err!(External: error))
    })
}

fn zstd_text(array: &ArrayRef) -> bool {
    let mut array = array.clone();
    while let Some(slice) = array.as_opt::<Slice>() {
        array = slice.child().clone();
    }
    array.is::<Zstd>()
}

fn mask_is_subset(selected: &Mask, previous: &Mask) -> bool {
    if selected.len() != previous.len() {
        return false;
    }
    if selected.all_false() || previous.all_true() {
        return true;
    }
    let (AllOr::Some(selected), AllOr::Some(previous)) =
        (selected.bit_buffer(), previous.bit_buffer())
    else {
        return false;
    };
    // Validate in the original row domain without allocating another mask or
    // a row-index vector. BitChunks handles sliced buffers and the final tail.
    let (selected, previous) = (selected.chunks(), previous.chunks());
    selected
        .iter()
        .zip(previous.iter())
        .all(|(selected, previous)| selected & !previous == 0)
        && selected.remainder_bits() & !previous.remainder_bits() == 0
}

impl LayoutReader for Reader {
    fn name(&self) -> &Arc<str> {
        self.inner.name()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn dtype(&self) -> &DType {
        self.inner.dtype()
    }
    fn row_count(&self) -> u64 {
        self.inner.row_count()
    }

    fn register_splits(
        &self,
        fields: &[FieldMask],
        range: &SplitRange,
        splits: &mut RowSplits,
    ) -> VortexResult<()> {
        self.inner.register_splits(fields, range, splits)
    }

    fn pruning_evaluation(
        &self,
        range: &Range<u64>,
        expr: &BoundExpression,
        mask: Mask,
    ) -> VortexResult<MaskFuture> {
        self.inner.pruning_evaluation(range, expr, mask)
    }

    fn filter_evaluation(
        &self,
        range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<MaskFuture> {
        if expr != &self.filter {
            return self.inner.filter_evaluation(range, expr, mask);
        }
        let Some(entry) = self.registry.get(range)? else {
            return self.inner.filter_evaluation(range, expr, mask);
        };
        let inner = Arc::clone(&self.inner);
        let range = range.clone();
        let original_filter = expr.clone();
        let projection = self.text_projection.clone();
        let projected_filter = self.projected_filter.clone();
        let session = self.session.clone();
        let stats = Arc::clone(&self.stats);
        let cancellation = self.cancellation.clone();
        Ok(MaskFuture::new(mask.len(), async move {
            check_cancelled(cancellation.as_ref())?;
            let mask = mask.await?;
            // Preserve the provider's sparse selection and failure ordering.
            if !mask.all_true() {
                stats.sparse_masks.fetch_add(1, Ordering::Relaxed);
                return inner
                    .filter_evaluation(&range, &original_filter, MaskFuture::ready(mask))?
                    .await;
            }
            let raw = inner
                .projection_evaluation(&range, &projection, MaskFuture::ready(mask.clone()))?
                .await?;
            check_cancelled(cancellation.as_ref())?;
            let mut ctx = session.create_execution_ctx();
            let fields = raw.execute::<StructArray>(&mut ctx)?;
            let field = fields.unmasked_field(0);
            let prepared = if zstd_text(field) {
                let text = field
                    .clone()
                    .execute::<VarBinViewArray>(&mut ctx)?
                    .into_array();
                stats.decoded_chunks.fetch_add(1, Ordering::Relaxed);
                StructArray::new(
                    fields.struct_fields().names().clone(),
                    vec![text],
                    fields.len(),
                    Validity::NonNullable,
                )
                .into_array()
            } else {
                fields.into_array()
            };
            let selected = prepared
                .clone()
                .apply_bound(&projected_filter)?
                .null_as_false()
                .execute(&mut ctx)?;
            let output_mask = mask.intersect_by_rank(&selected);
            check_cancelled(cancellation.as_ref())?;
            *entry
                .ready
                .lock()
                .map_err(|_| vortex_err!("text scan entry poisoned"))? = Some(Ready {
                // Keep the original row domain. Later conjuncts can narrow
                // the mask without decoding this column again or remapping
                // source row ordinals through an intermediate selection.
                array: prepared,
                mask: output_mask.clone(),
            });
            Ok(output_mask)
        }))
    }

    fn projection_evaluation(
        &self,
        range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<ArrayFuture> {
        if expr != &self.projection {
            return self.inner.projection_evaluation(range, expr, mask);
        }
        let entry = self.registry.insert(range)?;
        let inner = Arc::clone(&self.inner);
        let range = range.clone();
        let expr = expr.clone();
        let other_projection = self.other_projection.clone();
        let session = self.session.clone();
        let stats = Arc::clone(&self.stats);
        let cancellation = self.cancellation.clone();
        Ok(Box::pin(async move {
            let mask = mask.await?;
            check_cancelled(cancellation.as_ref())?;
            let ready = entry
                .ready
                .lock()
                .map_err(|_| vortex_err!("text scan entry poisoned"))?
                .take();
            let Some(ready) = ready else {
                return inner
                    .projection_evaluation(&range, &expr, MaskFuture::ready(mask))?
                    .await;
            };
            vortex_ensure!(
                ready.array.len() == mask.len() && mask_is_subset(&mask, &ready.mask),
                "text scan projection mask widened or changed length"
            );
            let text = if mask.all_true() {
                ready.array
            } else {
                ready.array.filter(mask.clone())?
            };
            let array = if let Some(other_projection) = other_projection {
                let other = inner
                    .projection_evaluation(
                        &range,
                        &other_projection,
                        MaskFuture::ready(mask.clone()),
                    )?
                    .await?;
                check_cancelled(cancellation.as_ref())?;
                let mut ctx = session.create_execution_ctx();
                let other = other.execute::<StructArray>(&mut ctx)?;
                let text = text.execute::<StructArray>(&mut ctx)?;
                let names = expr
                    .dtype()
                    .as_struct_fields_opt()
                    .ok_or_else(|| vortex_err!("text scan projection lost struct dtype"))?
                    .names()
                    .clone();
                let fields = names
                    .iter()
                    .map(|name| {
                        text.unmasked_field_by_name_opt(name)
                            .or_else(|| other.unmasked_field_by_name_opt(name))
                            .cloned()
                            .ok_or_else(|| vortex_err!("text scan projection lost field {name}"))
                    })
                    .collect::<VortexResult<Vec<_>>>()?;
                StructArray::try_new(names, fields, mask.true_count(), Validity::NonNullable)?
                    .into_array()
            } else {
                text
            };
            vortex_ensure!(
                array.len() == mask.true_count() && array.dtype() == expr.dtype(),
                "text scan projection shape changed"
            );
            stats.projected_reuses.fetch_add(1, Ordering::Relaxed);
            Ok(array)
        }))
    }
}
