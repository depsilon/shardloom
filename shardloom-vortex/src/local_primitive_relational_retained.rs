//! Action-owned native replay for explicit materialization and frontier iteration.

#[cfg(feature = "vortex-write")]
use super::SetKind;
use super::{
    ArrayRef, DType, MemoryLease, Metrics, NativeExecutionContext, Node, NodeKind, Nullability,
    PreparedVortexRelational, PreparedVortexSource, RefCell, ReservedVec, Result, RowSet,
    VortexRelationalPlan, cast_batch, failed, replay, vortex_error,
};
use crate::relational_query::{
    VortexRelationalCompositionReport as Report, VortexRelationalRecursionId,
    VortexRelationalRecursive,
};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};

/// The immutable definition itself is the identity. Equal but independently
/// constructed plans are different declarations; aliases retain the same Arc.
#[derive(Clone)]
pub(super) enum Definition {
    Materialized(Arc<VortexRelationalPlan>),
    Recursive(Arc<VortexRelationalRecursive>),
}

impl Definition {
    pub(super) fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Materialized(left), Self::Materialized(right)) => Arc::ptr_eq(left, right),
            (Self::Recursive(left), Self::Recursive(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    pub(super) fn recursive(&self) -> bool {
        matches!(self, Self::Recursive(_))
    }
}

/// Sharing is limited to immutable binding. Mutable native operator state is
/// constructed afresh by `run`, and completed payloads live only in State.
#[derive(Clone)]
pub(super) struct Binding {
    pub(super) definition: Definition,
    node: Arc<Mutex<Node>>,
}

impl Binding {
    pub(super) fn new(definition: Definition, node: Node) -> Self {
        Self {
            definition,
            node: Arc::new(Mutex::new(node)),
        }
    }

    pub(super) fn node(&self) -> Result<MutexGuard<'_, Node>> {
        self.node
            .lock()
            .map_err(|_| failed("retained relation binding was poisoned"))
    }

    pub(super) fn upper_rows(&self, sources: &[PreparedVortexSource]) -> Option<u64> {
        self.node().ok()?.upper_rows(sources)
    }
}

type Relations<Key> = RefCell<Option<ReservedVec<(Key, Rc<Relation>)>>>;

#[derive(Default)]
pub(super) struct State {
    retained: Relations<Definition>,
    frontiers: Relations<VortexRelationalRecursionId>,
    report: RefCell<Report>,
}

fn add(value: &mut u64, amount: u64) -> Result<()> {
    *value = value
        .checked_add(amount)
        .ok_or_else(|| failed("composition work counter overflow"))?;
    Ok(())
}

fn reserve_fields(
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
) -> Result<MemoryLease> {
    let bytes = fields.iter().try_fold(4096_u64, |bytes, (name, dtype)| {
        let field = super::super::native_payload::metadata_bytes(dtype, context)?
            .checked_add(u64::try_from(name.len()).map_err(vortex_error)?)
            .and_then(|bytes| bytes.checked_add(1024))
            .and_then(|bytes| bytes.checked_mul(4))
            .ok_or_else(|| failed("retained schema metadata size overflow"))?;
        bytes
            .checked_add(field)
            .ok_or_else(|| failed("retained schema metadata size overflow"))
    })?;
    context.memory().reserve(bytes)
}

impl State {
    pub(super) fn report(&self) -> Report {
        self.report.borrow().clone()
    }

    pub(super) fn clear(&self) -> Result<()> {
        if self
            .frontiers
            .borrow()
            .as_ref()
            .is_some_and(|values| !values.values.is_empty())
        {
            return Err(failed("recursive frontier remained active at completion"));
        }
        self.retained.borrow_mut().take();
        self.frontiers.borrow_mut().take();
        Ok(())
    }

    fn get(&self, definition: &Definition) -> Option<Rc<Relation>> {
        self.retained
            .borrow()
            .as_ref()?
            .values
            .iter()
            .find(|(key, _)| key.same(definition))
            .map(|(_, relation)| Rc::clone(relation))
    }

    fn publish(
        &self,
        definition: Definition,
        relation: Rc<Relation>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        context.check_cancelled()?;
        let mut entries = self.retained.borrow_mut();
        if entries.is_none() {
            *entries = Some(ReservedVec::new(context.memory())?);
        }
        entries
            .as_mut()
            .expect("initialized retention")
            .push((definition, relation))
    }

    pub(super) fn frontier(&self, identity: &VortexRelationalRecursionId) -> Result<Rc<Relation>> {
        self.frontiers
            .borrow()
            .as_ref()
            .and_then(|entries| {
                entries
                    .values
                    .iter()
                    .rev()
                    .find(|(key, _)| key == identity)
                    .map(|(_, relation)| Rc::clone(relation))
            })
            .ok_or_else(|| failed("recursive input is outside its active frontier"))
    }

    fn with_frontier<T>(
        &self,
        identity: &VortexRelationalRecursionId,
        relation: &Rc<Relation>,
        context: &NativeExecutionContext<'_>,
        execute: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        {
            let mut entries = self.frontiers.borrow_mut();
            if entries.is_none() {
                *entries = Some(ReservedVec::new(context.memory())?);
            }
            entries
                .as_mut()
                .expect("initialized frontier")
                .push((identity.clone(), Rc::clone(relation)))?;
        }
        let result = execute();
        let removed = self
            .frontiers
            .borrow_mut()
            .as_mut()
            .and_then(|entries| entries.values.pop());
        if removed.as_ref().is_none_or(|(key, _)| key != identity) {
            return Err(failed("recursive frontier scope changed during execution"));
        }
        result
    }

    fn copied(&self, stats: &replay::CopyStats, spilled: bool) -> Result<()> {
        let mut report = self.report.borrow_mut();
        add(&mut report.retained_batches, stats.batches)?;
        add(
            &mut report.retained_payload_bytes_copied,
            stats.buffer_bytes,
        )?;
        add(&mut report.spilled_relations, u64::from(spilled))
    }
}

enum Payload {
    Replay(replay::Completed),
    Iterations(ReservedVec<Rc<Relation>>),
}

pub(super) struct Relation {
    dtype: DType,
    rows: u64,
    payload: Payload,
    #[cfg(feature = "vortex-write")]
    owned_source: Option<crate::owned_array_source::OwnedArraySource>,
    _metadata: MemoryLease,
}

impl Relation {
    fn new(
        dtype: &DType,
        rows: u64,
        payload: Payload,
        #[cfg(feature = "vortex-write")] owned_source: Option<
            crate::owned_array_source::OwnedArraySource,
        >,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let metadata = crate::native_payload_schema::reserve_schema(dtype, context)?;
        Ok(Self {
            dtype: dtype.clone(),
            rows,
            payload,
            #[cfg(feature = "vortex-write")]
            owned_source,
            _metadata: metadata,
        })
    }

    pub(super) fn run(
        &self,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if batch_rows == 0 {
            return Err(failed("retained relation requires a positive batch size"));
        }
        match &self.payload {
            Payload::Replay(replay) => replay.run(&self.dtype, metrics, context, &mut |array| {
                for start in (0..array.len()).step_by(batch_rows) {
                    context.check_cancelled()?;
                    let end = start.saturating_add(batch_rows).min(array.len());
                    consume(array.slice(start..end).map_err(vortex_error)?)?;
                }
                Ok(())
            }),
            Payload::Iterations(parts) => {
                for part in &parts.values {
                    part.run(context, metrics, batch_rows, consume)?;
                }
                context.check_cancelled()
            }
        }
    }
}

impl PreparedVortexRelational {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_retained_filtered(
        &self,
        binding: &Binding,
        filter: Option<&super::native_relational_expression::Expression>,
        fields: &[(String, DType)],
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        probe_filters: &[super::super::native_relational_probe_filter::ProbeRuntimeFilter],
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let relation = self.ensure_retained_relation(binding, context, metrics, batch_rows)?;
        #[cfg(feature = "vortex-write")]
        if let Some(owned) = &relation.owned_source
            && super::scan::run_owned_source_pushdown(
                owned,
                filter,
                probe_filters,
                fields,
                context,
                metrics,
                batch_rows,
                &mut |already_satisfied, array| {
                    emit_retained_batch(array, filter, fields, already_satisfied, context, consume)
                },
            )?
        {
            return Ok(());
        }
        if probe_filters.is_empty() {
            return relation.run(context, metrics, batch_rows, &mut |array| {
                emit_retained_batch(array, filter, fields, false, context, consume)
            });
        }
        if filter.is_none_or(|pred| pred.is_infallible_for_join_pushdown(true))
            && super::scan::has_infallible_empty_build(probe_filters, &mut |name| {
                fields
                    .iter()
                    .find(|(col, _)| col == name)
                    .map(|(_, dtype)| dtype.clone())
            })
        {
            return Ok(());
        }
        relation.run(context, metrics, batch_rows, &mut |array| {
            let Some(mask) = super::super::native_relational_probe_filter::apply_bloom_mask(
                &array,
                probe_filters,
                context,
            )?
            else {
                return emit_retained_batch(array, filter, fields, false, context, consume);
            };
            if mask.all_false() {
                return Ok(());
            }
            if mask.all_true() {
                emit_retained_batch(array, filter, fields, false, context, consume)
            } else {
                super::LocalSelection::from_mask(&mask, context)?.for_each_batch(
                    &array,
                    fields,
                    batch_rows,
                    context,
                    &mut |selected| {
                        emit_retained_batch(selected, filter, fields, false, context, consume)
                    },
                )
            }
        })
    }

    fn ensure_retained_relation(
        &self,
        binding: &Binding,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
    ) -> Result<Rc<Relation>> {
        if let Some(relation) = metrics.composition.get(&binding.definition) {
            let mut report = metrics.composition.report.borrow_mut();
            if binding.definition.recursive() {
                add(&mut report.recursive_reuses, 1)?;
            } else {
                add(&mut report.materialized_reuses, 1)?;
            }
            return Ok(relation);
        }
        let node = binding.node()?;
        let relation = if matches!(node.kind, NodeKind::Recursive { .. }) {
            self.complete_recursive(&node, context, metrics, batch_rows)?
        } else {
            let _schema_metadata = reserve_fields(&node.fields, context)?;
            let dtype = DType::struct_(node.fields.clone(), Nullability::NonNullable);
            let schema = replay::Schema::new(&dtype, context)?;
            let mut recorder = replay::Recorder::new(&schema, metrics, context)?;
            let mut copied = replay::CopyStats::default();
            self.run(&node, context, metrics, batch_rows, None, &mut |array| {
                recorder.build(&array, &mut copied, context)
            })?;
            let payload = recorder.finish(context)?;
            metrics.composition.copied(&copied, payload.is_spilled())?;
            #[cfg(feature = "vortex-write")]
            let owned_source =
                if metrics.spill.is_none() && !payload.is_spilled() && copied.rows > 0 {
                    try_build_owned_source(
                        &self.session,
                        &payload,
                        copied.rows,
                        copied.buffer_bytes,
                        context,
                    )
                } else {
                    None
                };
            let relation = Relation::new(
                &dtype,
                copied.rows,
                Payload::Replay(payload),
                #[cfg(feature = "vortex-write")]
                owned_source,
                context,
            )?;
            let mut report = metrics.composition.report.borrow_mut();
            add(&mut report.materialized_relations, 1)?;
            add(&mut report.materialized_rows, copied.rows)?;
            relation
        };
        drop(node);
        let relation = Rc::new(relation);
        metrics
            .composition
            .publish(binding.definition.clone(), Rc::clone(&relation), context)?;
        Ok(relation)
    }

    fn complete_recursive(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
    ) -> Result<Relation> {
        let NodeKind::Recursive {
            identity,
            anchor,
            step,
            distinct,
        } = &node.kind
        else {
            return Err(failed(
                "recursive completion requires a bound recursive relation",
            ));
        };
        let _schema_metadata = reserve_fields(&node.fields, context)?;
        let dtype = DType::struct_(node.fields.clone(), Nullability::NonNullable);
        let schema = replay::Schema::new(&dtype, context)?;
        let mut history = ReservedVec::new(context.memory())?;
        let names = node
            .fields
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        #[cfg(feature = "vortex-write")]
        let use_resident_membership = *distinct && metrics.spill.is_none();
        #[cfg(not(feature = "vortex-write"))]
        let use_resident_membership = *distinct;
        let mut visited = use_resident_membership
            .then(|| RowSet::new(&node.fields, &names, context.memory()))
            .transpose()?;
        let mut iterations = 0_u64;
        let mut rows = 0_u64;
        let mut current = Rc::new(self.recursive_frontier(
            anchor,
            &node.fields,
            &dtype,
            &schema,
            &history,
            *distinct,
            &mut visited,
            context,
            metrics,
            batch_rows,
        )?);
        while current.rows != 0 {
            context.check_cancelled()?;
            add(&mut rows, current.rows)?;
            {
                let mut report = metrics.composition.report.borrow_mut();
                report.recursive_peak_frontier_rows =
                    report.recursive_peak_frontier_rows.max(current.rows);
            }
            history.push(Rc::clone(&current))?;
            iterations = context.recursion_limit().next(iterations)?;
            let next = metrics
                .composition
                .with_frontier(identity, &current, context, || {
                    self.recursive_frontier(
                        step,
                        &node.fields,
                        &dtype,
                        &schema,
                        &history,
                        *distinct,
                        &mut visited,
                        context,
                        metrics,
                        batch_rows,
                    )
                })?;
            add(
                &mut metrics.composition.report.borrow_mut().recursive_iterations,
                1,
            )?;
            current = Rc::new(next);
        }
        drop(current);
        drop(visited);
        let mut report = metrics.composition.report.borrow_mut();
        add(&mut report.recursive_relations, 1)?;
        add(&mut report.recursive_output_rows, rows)?;
        drop(report);
        Relation::new(
            &dtype,
            rows,
            Payload::Iterations(history),
            #[cfg(feature = "vortex-write")]
            None,
            context,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn recursive_frontier(
        &self,
        input: &Node,
        fields: &[(String, DType)],
        dtype: &DType,
        schema: &replay::Schema,
        history: &ReservedVec<Rc<Relation>>,
        distinct: bool,
        visited: &mut Option<RowSet<'_>>,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
    ) -> Result<Relation> {
        let mut recorder = replay::Recorder::new(schema, metrics, context)?;
        let mut copied = replay::CopyStats::default();
        let mut record = |array: ArrayRef| recorder.build(&array, &mut copied, context);
        let produce = |accept: &mut dyn FnMut(ArrayRef) -> Result<()>| {
            self.run(input, context, metrics, batch_rows, None, &mut |array| {
                add(
                    &mut metrics
                        .composition
                        .report
                        .borrow_mut()
                        .recursive_candidate_rows,
                    array.len() as u64,
                )?;
                let array = if array.dtype() == dtype {
                    array
                } else {
                    cast_batch(&array, &input.fields, fields, context)?
                };
                accept(array)
            })
        };
        if let Some(visited) = visited {
            produce(&mut |array| {
                visited
                    .insert_batch(array, None, context, batch_rows, Some(&mut record))
                    .map(|_| ())
            })?;
        } else if distinct {
            #[cfg(feature = "vortex-write")]
            {
                let spill = metrics
                    .spill
                    .as_ref()
                    .ok_or_else(|| failed("recursive distinct replay has no spill owner"))?;
                let kind = if history.values.is_empty() {
                    SetKind::UnionDistinct
                } else {
                    SetKind::Except
                };
                let report = super::super::native_relational_set_spill::run(
                    kind,
                    fields,
                    spill,
                    context,
                    batch_rows,
                    |left, accept| {
                        if left {
                            produce(accept)
                        } else {
                            for prior in &history.values {
                                prior.run(context, metrics, batch_rows, accept)?;
                            }
                            Ok(())
                        }
                    },
                    &mut record,
                )?;
                super::add(&metrics.ordered_set_stages, 1)?;
                super::add(&metrics.ordered_set_input_rows, report.input_rows)?;
                super::add(&metrics.ordered_set_groups, report.groups)?;
                super::add(&metrics.ordered_set_output_rows, report.output_rows)?;
            }
            #[cfg(not(feature = "vortex-write"))]
            return Err(failed("recursive distinct membership has no native owner"));
        } else {
            produce(&mut record)?;
        }
        #[cfg(not(feature = "vortex-write"))]
        let _ = history;
        let payload = recorder.finish(context)?;
        metrics.composition.copied(&copied, payload.is_spilled())?;
        Relation::new(
            dtype,
            copied.rows,
            Payload::Replay(payload),
            #[cfg(feature = "vortex-write")]
            None,
            context,
        )
    }
}

fn emit_retained_batch(
    array: ArrayRef,
    filter: Option<&super::native_relational_expression::Expression>,
    fields: &[(String, DType)],
    already_satisfied: bool,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    if already_satisfied {
        return consume(array);
    }
    if let Some(predicate) = filter {
        super::transform::apply_filter(predicate, fields, &array, context, consume)
    } else {
        consume(array)
    }
}

#[cfg(feature = "vortex-write")]
fn try_build_owned_source(
    session: &crate::resident_session::ResidentVortexSession,
    payload: &replay::Completed,
    rows: u64,
    buffer_bytes: u64,
    context: &NativeExecutionContext<'_>,
) -> Option<crate::owned_array_source::OwnedArraySource> {
    let arrays = payload.resident_arrays()?;
    if arrays.is_empty() || rows == 0 {
        return None;
    }
    let bounds = crate::owned_array_source::OwnedArraySourceBounds::default();
    if rows > bounds.max_rows
        || buffer_bytes > bounds.max_logical_bytes
        || arrays.len() > bounds.max_batches
    {
        return None;
    }
    let snap = context.memory().snapshot();
    let min_headroom = buffer_bytes.saturating_add(2 * 1024 * 1024);
    if snap.limit_bytes.saturating_sub(snap.reserved_bytes) < min_headroom {
        return None;
    }
    let cloned = arrays.to_vec();
    let ownership_bytes = u64::try_from(cloned.capacity().max(1) * size_of::<ArrayRef>()).ok()?;
    let ownership = context.memory().reserve(ownership_bytes).ok()?;
    let batch = session.own_completed_arrays(cloned, ownership).ok()?;
    crate::owned_array_source::OwnedArraySource::from_owned_in_context(batch, bounds, context).ok()
}
