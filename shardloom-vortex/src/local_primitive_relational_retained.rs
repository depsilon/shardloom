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

    fn recursive(&self) -> bool {
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
        let field = super::super::native_payload::metadata_bytes(dtype)?
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
    _metadata: MemoryLease,
}

impl Relation {
    fn new(
        dtype: &DType,
        rows: u64,
        payload: Payload,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let metadata = crate::native_payload_schema::reserve_schema(dtype, context.memory())?;
        Ok(Self {
            dtype: dtype.clone(),
            rows,
            payload,
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
    pub(super) fn run_retained(
        &self,
        binding: &Binding,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let relation = if let Some(relation) = metrics.composition.get(&binding.definition) {
            let mut report = metrics.composition.report.borrow_mut();
            if binding.definition.recursive() {
                add(&mut report.recursive_reuses, 1)?;
            } else {
                add(&mut report.materialized_reuses, 1)?;
            }
            relation
        } else {
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
                let relation =
                    Relation::new(&dtype, copied.rows, Payload::Replay(payload), context)?;
                let mut report = metrics.composition.report.borrow_mut();
                add(&mut report.materialized_relations, 1)?;
                add(&mut report.materialized_rows, copied.rows)?;
                relation
            };
            drop(node);
            let relation = Rc::new(relation);
            metrics.composition.publish(
                binding.definition.clone(),
                Rc::clone(&relation),
                context,
            )?;
            relation
        };
        relation.run(context, metrics, batch_rows, consume)
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
        Relation::new(&dtype, rows, Payload::Iterations(history), context)
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
        Relation::new(dtype, copied.rows, Payload::Replay(payload), context)
    }
}
