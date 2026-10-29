//! Left-preserving exact as-of lookup over native build owners.

use super::{Batch, NativeExecutionContext, Pairs, ReservedVec, Result, Spec, Table, failed};
use crate::{
    local_primitives::{native_relational_keys::Cell, native_relational_order},
    relational_query::{
        VortexRelationalAsOfDirection as Direction, VortexRelationalAsOfOptions as Options,
    },
};
use shardloom_exec::live_memory::LiveMemoryPool;
use std::cmp::Ordering;
use vortex::array::ArrayRef;

#[cfg(feature = "vortex-write")]
#[path = "native_relational_asof_spill.rs"]
pub(in crate::local_primitives) mod spill;

pub(in crate::local_primitives) struct Join<'a> {
    spec: &'a Spec,
    right: Table,
    ordered: ReservedVec<usize>,
    sealed: bool,
}

impl<'a> Join<'a> {
    pub(in crate::local_primitives) fn new(
        spec: &'a Spec,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        options(spec)?;
        Ok(Self {
            spec,
            right: Table::new(memory)?,
            ordered: ReservedVec::new(memory)?,
            sealed: false,
        })
    }

    pub(in crate::local_primitives) fn build(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if self.sealed {
            return Err(failed("as-of build is already sealed"));
        }
        let batch = Batch::for_ordering(array, &self.spec.right_keys, context)?;
        validate(&batch, context)?;
        self.ordered.reserve(batch.array.len())?;
        let rows = self.right.push(batch)?;
        self.ordered.values.extend(rows);
        context.check_cancelled()
    }

    pub(in crate::local_primitives) fn seal(
        &mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let keys = self.spec.right_keys.len();
        native_relational_order::sort(&mut self.ordered.values, context.cancellation(), |a, b| {
            let (left, left_row) = self.right.locate_batch(a)?;
            let (right, right_row) = self.right.locate_batch(b)?;
            Ok(compare(left, left_row, right, right_row, keys)?.then_with(|| a.cmp(&b)))
        })?;
        self.sealed = true;
        context.check_cancelled()
    }

    pub(in crate::local_primitives) fn consume(
        &self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if !self.sealed {
            return Err(failed("as-of build must be sealed before probing"));
        }
        let left = Batch::for_ordering(array, &self.spec.left_keys, context)?;
        validate(&left, context)?;
        let mut pairs = Pairs::new(batch_rows, context.memory())?;
        let mut written = 0;
        let mut search = ResidentSearch {
            table: &self.right,
            ordered: &self.ordered.values,
        };
        for row in 0..left.array.len() {
            context.check_cancelled()?;
            let selected = select(
                options(self.spec)?,
                self.spec.left_keys.len(),
                &left,
                row,
                &mut search,
                context,
            )?;
            pairs.left.values.push(Some(row));
            pairs
                .right
                .values
                .push(selected.map(|position| self.ordered.values[position]));
            if pairs.left.values.len() == pairs.limit {
                self.spec.flush(
                    &self.right,
                    Some(&left.array),
                    &mut pairs,
                    &mut written,
                    context,
                    consume,
                )?;
            }
        }
        self.spec.flush(
            &self.right,
            Some(&left.array),
            &mut pairs,
            &mut written,
            context,
            consume,
        )?;
        context.check_cancelled()
    }
}

fn options(spec: &Spec) -> Result<Options> {
    if spec.left_keys.is_empty()
        || spec.left_keys.len() != spec.right_keys.len()
        || spec.kind != super::Kind::Left
        || spec.condition.is_some()
    {
        return Err(failed(
            "as-of join requires one ordering key and a left-preserving native specification",
        ));
    }
    spec.asof
        .ok_or_else(|| failed("as-of selection policy is absent"))
}

fn validate(batch: &Batch, context: &NativeExecutionContext<'_>) -> Result<()> {
    for row in 0..batch.array.len() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        batch.validate_keys(row)?;
    }
    context.check_cancelled()
}

fn compare(left: &Batch, a: usize, right: &Batch, b: usize, keys: usize) -> Result<Ordering> {
    for key in 0..keys {
        let order = match (left.key_is_null(a, key)?, right.key_is_null(b, key)?) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => left.compare_key(a, right, b, key)?,
        };
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(Ordering::Equal)
}

fn ticks(cell: &Cell) -> Result<Option<i128>> {
    Ok(match cell {
        Cell::Null => None,
        Cell::NegativeInteger(value) | Cell::Timestamp(value) | Cell::Temporal(value) => {
            Some(i128::from(*value))
        }
        Cell::NonnegativeInteger(value) => Some(i128::from(*value)),
        Cell::Date(value) => Some(i128::from(*value)),
        _ => {
            return Err(failed(
                "as-of ordering key is not an admitted exact integer or temporal domain",
            ));
        }
    })
}

fn candidate(right: &Batch, r: usize, left: &Batch, l: usize, keys: usize) -> Result<Option<i128>> {
    if !right.equal_prefix(r, left, l, keys - 1, false)? {
        return Ok(None);
    }
    ticks(&right.raw_cell(r, keys - 1)?)
}

/// Both resident and native-run lookup use this one selection algorithm.
trait Search {
    fn rows(&self) -> Result<usize>;
    fn compare(
        &mut self,
        position: usize,
        left: &Batch,
        row: usize,
        keys: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Ordering>;
    fn candidate(
        &mut self,
        position: usize,
        left: &Batch,
        row: usize,
        keys: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<i128>>;
}

struct ResidentSearch<'a> {
    table: &'a Table,
    ordered: &'a [usize],
}
impl Search for ResidentSearch<'_> {
    fn rows(&self) -> Result<usize> {
        Ok(self.ordered.len())
    }
    fn compare(
        &mut self,
        position: usize,
        left: &Batch,
        row: usize,
        keys: usize,
        _: &NativeExecutionContext<'_>,
    ) -> Result<Ordering> {
        let (right, local) = self.table.locate_batch(self.ordered[position])?;
        compare(right, local, left, row, keys)
    }
    fn candidate(
        &mut self,
        position: usize,
        left: &Batch,
        row: usize,
        keys: usize,
        _: &NativeExecutionContext<'_>,
    ) -> Result<Option<i128>> {
        let (right, local) = self.table.locate_batch(self.ordered[position])?;
        candidate(right, local, left, row, keys)
    }
}

fn bound(
    search: &mut impl Search,
    left: &Batch,
    row: usize,
    keys: usize,
    upper: bool,
    context: &NativeExecutionContext<'_>,
) -> Result<usize> {
    let (mut low, mut high) = (0, search.rows()?);
    while low < high {
        context.check_cancelled()?;
        let middle = low + (high - low) / 2;
        let ordering = search.compare(middle, left, row, keys, context)?;
        if ordering.is_lt() || (upper && ordering.is_eq()) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    Ok(low)
}

fn select(
    options: Options,
    keys: usize,
    left: &Batch,
    row: usize,
    search: &mut impl Search,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<usize>> {
    for key in 0..keys {
        if left.key_is_null(row, key)? {
            return Ok(None);
        }
    }
    let Some(value) = ticks(&left.raw_cell(row, keys - 1)?)? else {
        return Ok(None);
    };
    let backward = if options.direction == Direction::Forward {
        None
    } else {
        bound(
            search,
            left,
            row,
            keys,
            options.allow_exact_matches,
            context,
        )?
        .checked_sub(1)
    };
    let forward = if options.direction == Direction::Backward {
        None
    } else {
        let position = bound(
            search,
            left,
            row,
            keys,
            !options.allow_exact_matches,
            context,
        )?;
        (position < search.rows()?).then_some(position)
    };
    let mut best: Option<(usize, u128)> = None;
    // Backward is considered first, so equal distances preserve its tie rule.
    for position in [backward, forward].into_iter().flatten() {
        let Some(right) = search.candidate(position, left, row, keys, context)? else {
            continue;
        };
        let distance = value.abs_diff(right);
        if options
            .tolerance
            .is_some_and(|limit| distance > u128::from(limit))
        {
            continue;
        }
        if best.is_none_or(|(_, prior)| distance < prior) {
            best = Some((position, distance));
        }
    }
    Ok(best.map(|(position, _)| position))
}
