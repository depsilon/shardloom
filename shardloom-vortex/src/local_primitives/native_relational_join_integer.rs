//! Optional direct addressing into the existing sealed, duplicate-preserving index.

use super::super::{native_capacity::bytes, native_relational_keys::KeyColumn};
use super::{
    Batch, Kind, NativeExecutionContext, ReservedVec, Result, RowIndex, Spec, Table, failed,
};
use vortex::mask::Mask;

const MIN_KEYS: usize = 128;
const EMPTY: usize = usize::MAX;

#[derive(Default)]
pub(super) struct State {
    considered: bool,
    directory: Option<Directory>,
}

struct Directory {
    minimum: i64,
    entries: ReservedVec<usize>,
}

enum Admission {
    Shape,
    Small,
    Range,
    Memory,
    Admitted(Directory),
}

impl State {
    pub(super) fn prepare_once(
        &mut self,
        spec: &Spec,
        right: &Table,
        index: &RowIndex,
        left: &Batch,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if self.considered || left.array.is_empty() {
            return Ok(());
        }
        self.considered = true;
        let admission = Directory::prepare(spec, right, index, left, context)?;
        #[cfg(test)]
        super::super::native_relational_join_cost::integer_lookup_record(|work| {
            work.considered += 1;
            work.build_keys += index.unique_rows() as u64;
            match &admission {
                Admission::Shape => work.declined_shape += 1,
                Admission::Small => work.declined_small += 1,
                Admission::Range => work.declined_range += 1,
                Admission::Memory => work.declined_memory += 1,
                Admission::Admitted(directory) => {
                    work.admitted += 1;
                    work.directory_slots += directory.entries.values.len() as u64;
                    work.retained_bytes += bytes::<usize>(directory.entries.values.len()).unwrap();
                }
            }
        });
        if let Admission::Admitted(directory) = admission {
            self.directory = Some(directory);
        }
        Ok(())
    }

    pub(super) fn probe<'a>(&'a self, left: &'a Batch) -> Result<Option<Probe<'a>>> {
        let Some(directory) = &self.directory else {
            return Ok(None);
        };
        // A changed physical key domain retains ordinary native lookup for
        // that batch. It never reconstructs or mutates the completed directory.
        let Some((values, valid)) = i64_values(left)? else {
            return Ok(None);
        };
        Ok(Some(Probe {
            directory,
            values,
            valid,
            #[cfg(test)]
            work: std::cell::Cell::new((0, 0, 0)),
        }))
    }
}

impl Directory {
    fn prepare(
        spec: &Spec,
        right: &Table,
        index: &RowIndex,
        left: &Batch,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Admission> {
        context.check_cancelled()?;
        if !matches!(
            spec.kind,
            Kind::Inner | Kind::Left | Kind::Right | Kind::Full
        ) || spec.asof.is_some()
            || spec.condition.is_some()
            || spec.left_keys.len() != 1
            || spec.right_keys.len() != 1
            || i64_values(left)?.is_none()
        {
            return Ok(Admission::Shape);
        }
        let keys = index.unique_rows();
        if keys < MIN_KEYS {
            return Ok(Admission::Small);
        }
        // Ordinary joins do not populate membership-only right_key_fields.
        // Prove the actual build domain from each existing typed representative.
        let mut minimum = i64::MAX;
        let mut maximum = i64::MIN;
        for (entry, row) in index.representative_rows() {
            if entry.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let Some(value) = representative(right, row)? else {
                return Ok(Admission::Shape);
            };
            minimum = minimum.min(value);
            maximum = maximum.max(value);
        }
        // Widen before subtraction: MIN..MAX is a legitimate sparse decline.
        let Ok(span) = usize::try_from(i128::from(maximum) - i128::from(minimum) + 1) else {
            return Ok(Admission::Range);
        };
        if span as u128 > keys as u128 * 2 {
            return Ok(Admission::Range);
        }
        let capacity = bytes::<usize>(span)?;
        if !index.has_adaptive_headroom(capacity) {
            return Ok(Admission::Memory);
        }
        let Some(mut entries) = ReservedVec::try_with_capacity(context.memory(), span)? else {
            return Ok(Admission::Memory);
        };
        entries.values.resize(span, EMPTY);
        for (entry, row) in index.representative_rows() {
            if entry.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let value = representative(right, row)?
                .ok_or_else(|| failed("integer join build domain changed during admission"))?;
            let offset = usize::try_from(i128::from(value) - i128::from(minimum))
                .map_err(|_| failed("integer join directory offset overflow"))?;
            let slot = entries
                .values
                .get_mut(offset)
                .ok_or_else(|| failed("integer join key exceeds its completed bounds"))?;
            if *slot != EMPTY {
                return Err(failed("integer join directory repeats a distinct key"));
            }
            *slot = entry;
        }
        context.check_cancelled()?;
        Ok(Admission::Admitted(Self { minimum, entries }))
    }
}

fn i64_values(batch: &Batch) -> Result<Option<(&[i64], &Mask)>> {
    let KeyColumn::Numeric(owner) = batch.key(0)? else {
        return Ok(None);
    };
    let Some((values, valid)) = owner.i64_values_with_validity() else {
        return Ok(None);
    };
    if values.len() != batch.array.len() || valid.len() != values.len() {
        return Err(failed("integer join key owner changed row count"));
    }
    Ok(Some((values, valid)))
}

fn representative(right: &Table, row: usize) -> Result<Option<i64>> {
    let (batch, local) = right.locate_batch(row)?;
    let Some((values, valid)) = i64_values(batch)? else {
        return Ok(None);
    };
    if !valid.value(local) {
        return Err(failed("integer join index retained a null representative"));
    }
    Ok(Some(values[local]))
}

pub(super) struct Probe<'a> {
    directory: &'a Directory,
    values: &'a [i64],
    valid: &'a Mask,
    #[cfg(test)]
    work: std::cell::Cell<(u64, u64, u64)>,
}

impl Probe<'_> {
    pub(super) fn entry(&self, row: usize) -> Option<usize> {
        let valid = self.valid.value(row);
        let entry = valid
            .then(|| {
                usize::try_from(i128::from(self.values[row]) - i128::from(self.directory.minimum))
                    .ok()
                    .and_then(|offset| self.directory.entries.values.get(offset).copied())
                    .filter(|entry| *entry != EMPTY)
            })
            .flatten();
        #[cfg(test)]
        {
            let (rows, nulls, matches) = self.work.get();
            self.work.set((
                rows + 1,
                nulls + u64::from(!valid),
                matches + u64::from(entry.is_some()),
            ));
        }
        entry
    }
}

#[cfg(test)]
impl Drop for Probe<'_> {
    fn drop(&mut self) {
        let (rows, nulls, matches) = self.work.get();
        super::super::native_relational_join_cost::integer_lookup_record(|work| {
            work.probe_batches += 1;
            work.probe_rows += rows;
            work.null_rows += nulls;
            work.matched_rows += matches;
        });
    }
}
