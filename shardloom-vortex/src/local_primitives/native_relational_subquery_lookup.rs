//! Collision-safe correlation and membership ranges with one held native block.

use super::records::{DATA, GROUP, HASH};
use super::{Batch, Kind, NativeExecutionContext, Result, Spec, add, failed};
use crate::local_primitives::{
    logical_field_from_native_array,
    native_relational_expression::keys,
    native_relational_keys::{Cell, KeyColumn},
    native_relational_spill::{StoredBlock, StoredOrder},
};
use std::{cmp::Ordering, ops::Range};

#[cfg(test)]
#[path = "native_relational_subquery_lookup_tests.rs"]
mod tests;

#[cfg(test)]
type BeforeCacheHit = Box<dyn FnOnce(&std::path::Path)>;
#[cfg(test)]
thread_local! {
    pub(crate) static BEFORE_CACHE_HIT: std::cell::RefCell<Option<BeforeCacheHit>> = const { std::cell::RefCell::new(None) };
}

struct Held {
    payload: Option<Batch>,
    group: KeyColumn,
    hash: KeyColumn,
    block: StoredBlock,
    start: u64,
}

#[derive(Clone, Copy)]
enum Bound {
    Group(u64),
    Hash(u64, Option<u64>),
}

pub(super) struct Search<'a> {
    source: StoredOrder<'a>,
    spec: &'a Spec,
    held: Option<Held>,
    pub(super) blocks: u64,
    pub(super) candidates: u64,
}

impl<'a> Search<'a> {
    pub(super) fn new(source: StoredOrder<'a>, spec: &'a Spec) -> Self {
        Self {
            source,
            spec,
            held: None,
            blocks: 0,
            candidates: 0,
        }
    }

    fn load(&mut self, row: u64, context: &NativeExecutionContext<'_>) -> Result<&mut Held> {
        context.check_cancelled()?;
        if row >= self.source.rows() {
            return Err(failed("subquery lookup exceeds its population"));
        }
        let start = row / self.source.block_rows() as u64 * self.source.block_rows() as u64;
        #[cfg(test)]
        if self.held.as_ref().is_some_and(|held| held.start == start)
            && let Some(path) = self.source.run_path()
            && let Some(hook) = BEFORE_CACHE_HIT.with(|slot| slot.borrow_mut().take())
        {
            hook(path);
        }
        // An already held block never authorizes reading a changed generation.
        self.source.validate()?;
        if self.held.as_ref().is_none_or(|held| held.start != start) {
            self.held = None;
            let block = self
                .source
                .read_block_at(start, context)?
                .ok_or_else(|| failed("subquery lookup block is absent"))?;
            let group = keys(
                &logical_field_from_native_array(block.array(), GROUP)?,
                context,
            )?;
            let hash = keys(
                &logical_field_from_native_array(block.array(), HASH)?,
                context,
            )?;
            add(&mut self.blocks, 1)?;
            self.held = Some(Held {
                payload: None,
                group,
                hash,
                block,
                start,
            });
        }
        self.held
            .as_mut()
            .ok_or_else(|| failed("subquery lookup cache is absent"))
    }

    fn bound(
        &mut self,
        key: Bound,
        range: Range<u64>,
        upper: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<u64> {
        let (mut low, mut high) = (range.start, range.end);
        while low < high {
            let middle = low + (high - low) / 2;
            let held = self.load(middle, context)?;
            let local = usize::try_from(middle - held.start)
                .map_err(|_| failed("subquery lookup offset overflow"))?;
            let Cell::NonnegativeInteger(group) = held.group.cell(local)? else {
                return Err(failed("subquery correlation hash changed type"));
            };
            let order = match key {
                Bound::Group(target) => group.cmp(&target),
                Bound::Hash(target, hash) => {
                    let value = match held.hash.cell(local)? {
                        Cell::Null => None,
                        Cell::NonnegativeInteger(value) => Some(value),
                        _ => return Err(failed("subquery membership hash changed type")),
                    };
                    (group, value).cmp(&(target, hash))
                }
            };
            // Private ordering is ascending with NULLS FIRST.
            if order == Ordering::Less || (upper && order == Ordering::Equal) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    fn range(
        &mut self,
        key: Bound,
        range: Range<u64>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Range<u64>> {
        let low = self.bound(key, range.clone(), false, context)?;
        let high = self.bound(key, low..range.end, true, context)?;
        Ok(low..high)
    }

    pub(super) fn evaluate(
        &mut self,
        left: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<bool>> {
        self.source.validate()?;
        let Some(group) = left.hash_prefix(row, self.spec.correlation, false)? else {
            return Ok(Some(self.spec.empty()));
        };
        let range = if self.spec.correlation == 0 {
            0..self.source.rows()
        } else {
            self.range(Bound::Group(group), 0..self.source.rows(), context)?
        };
        if matches!(self.spec.kind, Kind::In)
            && let Some(hash) = left.hash(row, false)?
        {
            let exact = self.range(Bound::Hash(group, Some(hash)), range.clone(), context)?;
            if self.scan(left, row, exact, true, context)? == Some(true) {
                return Ok(Some(true));
            }
            let nullable = self.range(Bound::Hash(group, None), range, context)?;
            return self.scan(left, row, nullable, false, context);
        }
        self.scan(left, row, range, false, context)
    }

    fn scan(
        &mut self,
        left: &Batch,
        row: usize,
        range: Range<u64>,
        exact: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<bool>> {
        let spec = self.spec;
        let all = spec.empty();
        let mut unknown = false;
        for position in range {
            let held = self.load(position, context)?;
            if held.payload.is_none() {
                held.payload = Some(Batch::new(
                    logical_field_from_native_array(held.block.array(), DATA)?,
                    &spec.right_keys,
                    context,
                )?);
            }
            let payload = held
                .payload
                .as_ref()
                .ok_or_else(|| failed("subquery payload is absent"))?;
            let local = usize::try_from(position - held.start)
                .map_err(|_| failed("subquery candidate offset overflow"))?;
            if !payload.equal_prefix(local, left, row, spec.correlation, false)? {
                continue;
            }
            let value = if exact {
                Some(payload.equal(local, left, row, false)?)
            } else if matches!(spec.kind, Kind::Exists) {
                Some(true)
            } else {
                spec.compare(|key| {
                    if left.key_is_null(row, key)? || payload.key_is_null(local, key)? {
                        Ok(None)
                    } else {
                        left.compare_key(row, payload, local, key).map(Some)
                    }
                })?
            };
            add(&mut self.candidates, 1)?;
            if value == Some(!all) {
                return Ok(value);
            }
            unknown |= value.is_none();
        }
        Ok(if unknown { None } else { Some(all) })
    }

    pub(super) fn finish(mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        self.held = None;
        self.source.finish(context)
    }
}
