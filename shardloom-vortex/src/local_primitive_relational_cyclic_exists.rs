//! Finite cyclic semi-join: retain native keys and gather only witnessed outer rows.

use super::super::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Table, index_array, take_column},
    native_relational_index::RowIndex,
    native_relational_join::Spec,
    vortex_error,
};
use super::{Metrics, Node, NodeKind, PreparedVortexRelational, ScanSource};
use crate::{
    relational_query::{VortexRelationalJoinKind as Kind, VortexRelationalSide as Side},
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use std::hash::Hasher as _;
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::StructArray,
    dtype::{DType, FieldNames, PType},
    validity::Validity,
};

pub(super) struct Plan<'a> {
    outer: &'a Node,
    middle: &'a Node,
    last: &'a Node,
    outer_keys: [&'a str; 2],  // b, a
    middle_keys: [&'a str; 2], // b, c
    last_keys: [&'a str; 2],   // a, c
    output: &'a Spec,
}

impl<'a> Plan<'a> {
    /// Pure bound metadata only. The caller has already selected explicit spill.
    pub(super) fn admit(outer: &'a Node, right: &'a Node, output: &'a Spec) -> Option<Self> {
        if output.kind != Kind::LeftSemi
            || output.condition.is_some()
            || output.left_keys.len() != 2
            || output.right_keys.len() != 2
            || output.columns.iter().any(|(side, _)| *side != Side::Left)
        {
            return None;
        }
        let NodeKind::Join {
            left: middle,
            right: last,
            spec: inner,
        } = &right.kind
        else {
            return None;
        };
        if inner.kind != Kind::Inner
            || inner.condition.is_some()
            || inner.left_keys.len() != 1
            || inner.right_keys.len() != 1
            || inner.columns.len() != 2
            || ![outer, middle, last].into_iter().all(pure_memory_scan)
        {
            return None;
        }
        let mut middle_edge = None;
        let mut last_edge = None;
        for (outer_key, projected_key) in output.left_keys.iter().zip(&output.right_keys) {
            let position = inner
                .fields
                .iter()
                .position(|(name, _)| name == projected_key)?;
            let (side, input_key) = inner.columns.get(position)?;
            let edge = (outer_key.as_str(), input_key.as_str());
            match side {
                Side::Left => middle_edge = Some(edge),
                Side::Right => last_edge = Some(edge),
            }
        }
        let (outer_b, middle_b) = middle_edge?;
        let (outer_a, last_a) = last_edge?;
        let plan = Self {
            outer,
            middle,
            last,
            outer_keys: [outer_b, outer_a],
            middle_keys: [middle_b, &inner.left_keys[0]],
            last_keys: [last_a, &inner.right_keys[0]],
            output,
        };
        for (node, keys) in [
            (outer, plan.outer_keys),
            (middle, plan.middle_keys),
            (last, plan.last_keys),
        ] {
            if keys[0] == keys[1]
                || !keys.into_iter().all(|key| {
                    node.fields.iter().any(|(name, dtype)| {
                        name == key && matches!(dtype, DType::Primitive(PType::I64, _))
                    })
                })
            {
                return None;
            }
        }
        Some(plan)
    }

    #[allow(clippy::too_many_arguments)] // Preserve the existing recursive execution and consumer boundary.
    pub(super) fn run(
        &self,
        prepared: &PreparedVortexRelational,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        #[cfg(test)]
        observation::record(|counts| counts.executions += 1);
        // Match ordinary right-before-left source order. Both indexes belong to
        // this execution; repeated prepared calls rebuild them under the grant.
        let mut last = Indexed::new(context)?;
        prepared.run(
            self.last,
            context,
            metrics,
            batch_rows,
            parameter,
            &mut |array| {
                #[cfg(test)]
                observation::record(|counts| counts.last_rows += array.len());
                last.build(array, self.last_keys, 2, context)
            },
        )?;
        let mut middle = Indexed::new(context)?;
        prepared.run(
            self.middle,
            context,
            metrics,
            batch_rows,
            parameter,
            &mut |array| {
                #[cfg(test)]
                observation::record(|counts| counts.middle_rows += array.len());
                middle.build(array, self.middle_keys, 1, context)
            },
        )?;
        prepared.run(
            self.outer,
            context,
            metrics,
            batch_rows,
            parameter,
            &mut |array| {
                let outer = Batch::with_names(array, self.outer_keys, context)?;
                let mut selected = ReservedVec::new(context.memory())?;
                selected.reserve(batch_rows.min(outer.array.len()))?;
                for row in 0..outer.array.len() {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    #[cfg(test)]
                    observation::record(|counts| counts.outer_rows += 1);
                    if has_witness(&outer, row, &middle, &last, context)? {
                        #[cfg(test)]
                        observation::record(|counts| counts.witness_exits += 1);
                        selected.values.push(row);
                        if selected.values.len() == batch_rows {
                            gather(
                                &outer.array,
                                &selected.values,
                                self.output,
                                context,
                                consume,
                            )?;
                            selected.values.clear();
                        }
                    }
                }
                gather(
                    &outer.array,
                    &selected.values,
                    self.output,
                    context,
                    consume,
                )
            },
        )
    }
}

fn pure_memory_scan(node: &Node) -> bool {
    matches!(&node.kind, NodeKind::Scan {
        source: ScanSource::Memory(source), plan, residual: None, ..
    } if source.source_rows() >= 128 && plan.filter.is_none()
        && plan.residual_predicate.is_none() && plan.source_order_limit.is_none()
        && plan.embedded_derived_column_rewrites.is_empty())
}

struct Indexed {
    table: Table,
    index: RowIndex,
}

impl Indexed {
    fn new(context: &NativeExecutionContext<'_>) -> Result<Self> {
        Ok(Self {
            table: Table::new(context.memory())?,
            index: RowIndex::new(context.memory())?,
        })
    }

    fn build(
        &mut self,
        array: ArrayRef,
        keys: [&str; 2],
        prefix: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let batch = Batch::with_names(array, keys, context)?;
        for row in self.table.push(batch)? {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if let Some(hash) = self.table.hash_prefix(row, prefix, false)? {
                self.index
                    .insert(hash, row, context.cancellation(), |other| {
                        self.table.equal_prefix(row, other, prefix, false)
                    })?;
            }
        }
        context.check_cancelled()
    }
}

fn has_witness(
    outer: &Batch,
    row: usize,
    middle: &Indexed,
    last: &Indexed,
    context: &NativeExecutionContext<'_>,
) -> Result<bool> {
    let a = outer.key(1)?;
    if a.is_null(row)? {
        return Ok(false);
    }
    let Some(hash) = outer.hash_prefix(row, 1, false)? else {
        return Ok(false);
    };
    let Some(entry) = middle
        .index
        .find(hash, context.cancellation(), |candidate| {
            middle
                .table
                .equal_prefix_batch(candidate, outer, row, 1, false)
        })?
    else {
        return Ok(false);
    };
    for (visited, candidate) in middle.index.rows(entry)?.enumerate() {
        if visited.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        #[cfg(test)]
        observation::record(|counts| counts.middle_visits += 1);
        let (c, local) = middle.table.key(candidate, 1)?;
        let mut hash = rustc_hash::FxHasher::default();
        if !a.hash_into(row, &mut hash)? || !c.hash_into(local, &mut hash)? {
            continue;
        }
        #[cfg(test)]
        observation::record(|counts| counts.last_probes += 1);
        if last
            .index
            .find(hash.finish(), context.cancellation(), |candidate| {
                let (last_a, last_row) = last.table.key(candidate, 0)?;
                let (last_c, last_c_row) = last.table.key(candidate, 1)?;
                Ok(a.equals_at(row, last_a, last_row, false)?
                    && c.equals_at(local, last_c, last_c_row, false)?)
            })?
            .is_some()
        {
            return Ok(true);
        }
    }
    context.check_cancelled()?;
    Ok(false)
}

fn gather(
    outer: &ArrayRef,
    selected: &[usize],
    spec: &Spec,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    if selected.is_empty() {
        return Ok(());
    }
    context.check_cancelled()?;
    let indices = index_array(selected.len(), false, context, |row| {
        Ok(Some(selected[row]))
    })?;
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(spec.fields.len())?;
    for ((_, name), (_, dtype)) in spec.columns.iter().zip(&spec.fields) {
        context.check_cancelled()?;
        columns.values.push(take_column(
            &logical_field_from_native_array(outer, name)?,
            &indices,
            dtype,
            context,
        )?);
    }
    let (columns, _ownership) = columns.into_parts();
    let output = StructArray::try_new(
        spec.fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        selected.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    #[cfg(test)]
    observation::record(|counts| counts.gathered_rows += selected.len());
    consume(output)?;
    context.check_cancelled()
}

#[cfg(test)]
pub(super) mod observation {
    use shardloom_exec::compute_pool::CancellationToken;
    use std::cell::RefCell;

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub(crate) struct Counts {
        pub(crate) executions: usize,
        pub(crate) last_rows: usize,
        pub(crate) middle_rows: usize,
        pub(crate) outer_rows: usize,
        pub(crate) middle_visits: usize,
        pub(crate) last_probes: usize,
        pub(crate) witness_exits: usize,
        pub(crate) gathered_rows: usize,
    }

    #[derive(Default)]
    struct State {
        counts: Counts,
        cancel: Option<(usize, CancellationToken)>,
    }

    thread_local! { static STATE: RefCell<State> = RefCell::new(State::default()); }

    pub(crate) fn reset() {
        STATE.with(|state| *state.borrow_mut() = State::default());
    }

    pub(crate) fn snapshot() -> Counts {
        STATE.with(|state| state.borrow().counts)
    }

    pub(crate) fn cancel_at_visit(visit: usize, token: CancellationToken) {
        STATE.with(|state| state.borrow_mut().cancel = Some((visit, token)));
    }

    pub(super) fn record(update: impl FnOnce(&mut Counts)) {
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            update(&mut state.counts);
            if let Some((visit, token)) = &state.cancel
                && state.counts.middle_visits >= *visit
            {
                token.cancel();
            }
        });
    }
}
