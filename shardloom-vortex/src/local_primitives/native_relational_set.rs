//! Exact row membership retaining only first-occurrence native payloads.

use super::{
    native_capacity::{ReservedVec, bytes},
    native_numeric_owner::NativeNumericOwner,
    native_relational_batch::{Batch, Gather, Table, failed},
    native_relational_index::RowIndex,
    native_relational_keys::VariableDomain,
    native_relational_rows::Rows,
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    array::{ArrayRef, dtype::DType},
    mask::Mask,
};

#[cfg(test)]
#[path = "native_relational_set_cache_tests.rs"]
mod cache_tests;

#[cfg(test)]
#[path = "native_relational_set_inline_cache_tests.rs"]
mod inline_cache_tests;

pub(super) enum SharedRef<'a, T: ?Sized + 'static> {
    Borrowed(&'a T),
    Shared(std::sync::Arc<T>),
}

impl<T: ?Sized + 'static> Clone for SharedRef<'_, T> {
    fn clone(&self) -> Self {
        match self {
            Self::Borrowed(value) => Self::Borrowed(value),
            Self::Shared(arc) => Self::Shared(std::sync::Arc::clone(arc)),
        }
    }
}

impl<T: ?Sized + 'static> std::ops::Deref for SharedRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(value) => value,
            Self::Shared(arc) => arc,
        }
    }
}

const MAX_EPOCH_DOMAIN_LEN: usize = 262_144;
const MAX_EPOCH_DOMAIN_COMPARE_LEN: usize = 8_192;

fn mask_word(valid: &Mask, domain_len: usize, word_idx: usize) -> u64 {
    let base = word_idx * 64;
    let bits = (domain_len - base).min(64);
    let mut word = 0_u64;
    for bit in 0..bits {
        if valid.value(base + bit) {
            word |= 1_u64 << bit;
        }
    }
    word
}

/// Owned snapshot of a small dictionary domain under the operator memory pool;
/// retains zero input `ArrayRef`, `BufferHandle`, or `MemoryLease` aliases.
struct EpochDomainMap {
    offsets: ReservedVec<u32>,
    bytes: ReservedVec<u8>,
    valid_words: ReservedVec<u64>,
    domain_len: usize,
    binary: bool,
    slots: ReservedVec<u32>,
    null_group: Option<usize>,
}

impl EpochDomainMap {
    fn matches(
        &self,
        domain: &VariableDomain,
        valid: &Mask,
        binary: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        if self.domain_len != domain.len() || self.binary != binary {
            return Ok(false);
        }
        for (word_idx, &expected) in self.valid_words.values.iter().enumerate() {
            if mask_word(valid, self.domain_len, word_idx) != expected {
                return Ok(false);
            }
        }
        for idx in 0..self.domain_len {
            if idx.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if (self.valid_words.values[idx >> 6] >> (idx & 63)) & 1 != 0 {
                let start = self.offsets.values[idx] as usize;
                let end = self.offsets.values[idx + 1] as usize;
                if domain.borrowed_bytes(idx)? != &self.bytes.values[start..end] {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn try_new(
        domain: &VariableDomain,
        valid: &Mask,
        binary: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        let domain_len = domain.len();
        let word_len = domain_len.div_ceil(64);
        let Some(offsets_len) = domain_len.checked_add(1) else {
            return Ok(None);
        };
        let mut total_bytes = 0_usize;
        for idx in 0..domain_len {
            if idx.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if valid.value(idx) {
                let Some(next) = total_bytes.checked_add(domain.borrowed_bytes(idx)?.len()) else {
                    return Ok(None);
                };
                total_bytes = next;
            }
        }
        if u32::try_from(total_bytes).is_err() {
            return Ok(None);
        }
        let offsets_bytes = bytes::<u32>(offsets_len)?;
        let payload_bytes = bytes::<u8>(total_bytes)?;
        let valid_bytes = bytes::<u64>(word_len)?;
        let slot_bytes = bytes::<u32>(domain_len)?;
        let needed = offsets_bytes
            .checked_add(payload_bytes)
            .and_then(|total| total.checked_add(valid_bytes))
            .and_then(|total| total.checked_add(slot_bytes));
        let snapshot = context.memory().snapshot();
        let available = snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes);
        if needed.is_none_or(|total| total > available / 16) {
            return Ok(None);
        }
        let Some(mut offsets) = ReservedVec::try_with_capacity(context.memory(), offsets_len)?
        else {
            return Ok(None);
        };
        let Some(mut bytes_buf) = ReservedVec::try_with_capacity(context.memory(), total_bytes)?
        else {
            return Ok(None);
        };
        let Some(mut valid_words) = ReservedVec::try_with_capacity(context.memory(), word_len)?
        else {
            return Ok(None);
        };
        let Some(mut slots) = ReservedVec::try_with_capacity(context.memory(), domain_len)? else {
            return Ok(None);
        };
        for word_idx in 0..word_len {
            valid_words
                .values
                .push(mask_word(valid, domain_len, word_idx));
        }
        offsets.values.push(0);
        for idx in 0..domain_len {
            if idx.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if valid.value(idx) {
                bytes_buf
                    .values
                    .extend_from_slice(domain.borrowed_bytes(idx)?);
            }
            let end = u32::try_from(bytes_buf.values.len())
                .map_err(|_| failed("epoch domain bytes overflow"))?;
            offsets.values.push(end);
        }
        slots.values.resize(domain_len, u32::MAX);
        Ok(Some(Self {
            offsets,
            bytes: bytes_buf,
            valid_words,
            domain_len,
            binary,
            slots,
            null_group: None,
        }))
    }
}

pub(super) struct RowSet<'a> {
    fields: SharedRef<'a, [(String, DType)]>,
    names: SharedRef<'a, [String]>,
    nulls_equal: bool,
    table: Table,
    index: RowIndex,
    epoch_domain: Option<EpochDomainMap>,
}

impl<'a> RowSet<'a> {
    pub(super) fn new(
        fields: &'a [(String, DType)],
        names: &'a [String],
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        Self::with_nulls_equal(fields, names, true, memory)
    }

    pub(super) fn new_shared(
        fields: std::sync::Arc<[(String, DType)]>,
        names: std::sync::Arc<[String]>,
        memory: &LiveMemoryPool,
    ) -> Result<RowSet<'static>> {
        Ok(RowSet {
            fields: SharedRef::Shared(fields),
            names: SharedRef::Shared(names),
            nulls_equal: true,
            table: Table::new(memory)?,
            index: RowIndex::new(memory)?,
            epoch_domain: None,
        })
    }

    pub(super) fn with_nulls_equal(
        fields: &'a [(String, DType)],
        names: &'a [String],
        nulls_equal: bool,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        Ok(Self {
            fields: SharedRef::Borrowed(fields),
            names: SharedRef::Borrowed(names),
            nulls_equal,
            table: Table::new(memory)?,
            index: RowIndex::new(memory)?,
            epoch_domain: None,
        })
    }

    pub(super) fn into_shared(self) -> RowSet<'static> {
        fn share<T: Clone + 'static>(rf: SharedRef<'_, [T]>) -> SharedRef<'static, [T]> {
            match rf {
                SharedRef::Borrowed(slice) => SharedRef::Shared(std::sync::Arc::from(slice)),
                SharedRef::Shared(arc) => SharedRef::Shared(arc),
            }
        }
        RowSet {
            fields: share(self.fields),
            names: share(self.names),
            nulls_equal: self.nulls_equal,
            table: self.table,
            index: self.index,
            epoch_domain: self.epoch_domain,
        }
    }

    pub(super) fn contains(
        &self,
        batch: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        self.contains_rows(batch, row, context)
    }

    fn contains_rows(
        &self,
        batch: &impl Rows,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        let Some(hash) = batch.hash(row, self.nulls_equal)? else {
            return Ok(false);
        };
        Ok(self
            .index
            .find(hash, context.cancellation(), |old| {
                let (old_batch, old_row) = self.table.locate_batch(old)?;
                batch.equal_batch(row, old_batch, old_row, self.nulls_equal)
            })?
            .is_some())
    }

    /// The index addresses compact retained ordinals. During this input batch,
    /// new ordinals refer through the reserved selection until native payloads
    /// are gathered. No duplicate input payload is retained across calls.
    pub(super) fn insert_batch(
        &mut self,
        array: ArrayRef,
        other: Option<(&Self, bool)>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: Option<&mut dyn FnMut(ArrayRef) -> Result<()>>,
    ) -> Result<u64> {
        self.ingest(array, other, context, batch_rows, consume, None)
    }

    pub(super) fn insert_rows(
        &mut self,
        rows: &impl Rows,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: Option<&mut dyn FnMut(ArrayRef) -> Result<()>>,
    ) -> Result<u64> {
        self.ingest_rows(rows, None, context, batch_rows, consume, None)
    }

    /// Map each input row to a stable first-occurrence group ordinal, retaining
    /// only unique native key payload. No per-input duplicate chain is added.
    pub(super) fn intern_batch(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
    ) -> Result<ReservedVec<usize>> {
        #[cfg(test)]
        let _cost = super::native_relational_grouped_cost::scope(
            super::native_relational_grouped_cost::Phase::GroupInterning,
        );
        if !self.nulls_equal {
            return Err(failed("group ordinals require null-equal membership"));
        }
        let mut mapping = ReservedVec::new(context.memory())?;
        mapping.reserve(array.len())?;
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| {
            counts.group_ordinal_rows += array.len() as u64;
            counts.group_input_batches += 1;
        });
        self.ingest(array, None, context, batch_rows, None, Some(&mut mapping))?;
        Ok(mapping)
    }

    pub(super) fn intern_rows(
        &mut self,
        rows: &impl Rows,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
    ) -> Result<ReservedVec<usize>> {
        if !self.nulls_equal {
            return Err(failed("group ordinals require null-equal membership"));
        }
        let mut mapping = ReservedVec::new(context.memory())?;
        mapping.reserve(rows.len())?;
        self.ingest_rows(rows, None, context, batch_rows, None, Some(&mut mapping))?;
        Ok(mapping)
    }

    pub(super) fn rows(&self) -> usize {
        self.table.rows()
    }

    pub(super) fn seal_join(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        self.epoch_domain = None;
        self.index.seal_join(false, context.cancellation())
    }

    pub(super) fn release_transient_caches(&mut self) {
        self.epoch_domain = None;
    }

    pub(super) fn intern_merged_batch(
        &mut self,
        batch: &Batch,
        other_index: &RowIndex,
        batch_start: usize,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        mapping: &mut ReservedVec<usize>,
    ) -> Result<()> {
        self.index.ensure_building()?;
        if !self.nulls_equal {
            return Err(failed("group ordinals require null-equal membership"));
        }
        if batch_rows == 0 {
            return Err(failed("set output batch rows must be positive"));
        }
        mapping.values.clear();
        mapping.reserve(batch.len())?;
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(batch.len())?;
        let prior_rows = self.table.rows();
        for row in 0..batch.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let hash = match other_index.entry_hash(batch_start + row) {
                Some(hash) => hash,
                None => match batch.hash(row, self.nulls_equal)? {
                    Some(hash) => hash,
                    None => continue,
                },
            };
            let equal = |old| {
                if old < prior_rows {
                    let (old_batch, old_row) = self.table.locate_batch(old)?;
                    batch.equal_batch(row, old_batch, old_row, self.nulls_equal)
                } else {
                    let selected_row = selected
                        .values
                        .get(old - prior_rows)
                        .ok_or_else(|| failed("pending set ordinal is absent"))?;
                    batch.equal(*selected_row, batch, row, self.nulls_equal)
                }
            };
            let ordinal = prior_rows
                .checked_add(selected.values.len())
                .ok_or_else(|| failed("set cardinality overflow"))?;
            let (group, inserted) =
                self.index
                    .insert_unique(hash, ordinal, context.cancellation(), equal)?;
            if inserted {
                if group != ordinal {
                    return Err(failed(
                        "interned group ordinal differs from insertion order",
                    ));
                }
                selected.values.push(row);
            }
            mapping.values.push(group);
        }
        for rows in selected.values.chunks(batch_rows) {
            context.check_cancelled()?;
            let array = batch.gather(&self.fields, rows, context)?;
            self.table.push(Batch::new(array, &self.names, context)?)?;
        }
        if self.table.rows() != self.index.unique_rows() {
            return Err(failed("set index and retained native payload disagree"));
        }
        context.check_cancelled()
    }

    pub(super) fn join_work(&self) -> Result<shardloom_core::native_join_work::NativeJoinWork> {
        self.index.join_work()
    }

    pub(super) fn gather<'b>(
        &'b self,
        rows: &[Option<usize>],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Gather<'b>> {
        self.table.gather(rows, false, context)
    }

    #[allow(clippy::too_many_lines)] // Keep key admission, exact insertion and batch publication together.
    fn ingest(
        &mut self,
        array: ArrayRef,
        other: Option<(&Self, bool)>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: Option<&mut dyn FnMut(ArrayRef) -> Result<()>>,
        mapping: Option<&mut ReservedVec<usize>>,
    ) -> Result<u64> {
        self.index.ensure_building()?;
        if batch_rows == 0 {
            return Err(failed("set output batch rows must be positive"));
        }
        #[cfg(test)]
        let preparation_cost = mapping.as_ref().map(|_| {
            super::native_relational_grouped_cost::scope(
                super::native_relational_grouped_cost::Phase::GroupPreparation,
            )
        });
        let batch = if self.nulls_equal && other.is_none() {
            Batch::for_membership(array, &self.names, context)?
        } else {
            Batch::new(array, &self.names, context)?
        };
        #[cfg(test)]
        drop(preparation_cost);
        self.ingest_rows(&batch, other, context, batch_rows, consume, mapping)
    }

    #[allow(clippy::too_many_lines)]
    fn ingest_rows(
        &mut self,
        batch: &impl Rows,
        other: Option<(&Self, bool)>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: Option<&mut dyn FnMut(ArrayRef) -> Result<()>>,
        mut mapping: Option<&mut ReservedVec<usize>>,
    ) -> Result<u64> {
        self.index.ensure_building()?;
        if batch_rows == 0 {
            return Err(failed("set output batch rows must be positive"));
        }
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(batch.len())?;
        let prior_rows = self.table.rows();
        if other.is_none()
            && self.fields.len() == 1
            && matches!(self.fields[0].1, DType::Utf8(_) | DType::Binary(_))
            && let Some((domain_values, domain_valid, codes, binary)) = batch.variable_dict_epoch()
            && !domain_values.is_empty()
            && domain_values.len() <= MAX_EPOCH_DOMAIN_LEN
        {
            let domain_len = domain_values.len();
            if domain_len <= MAX_EPOCH_DOMAIN_COMPARE_LEN {
                let reused = self
                    .epoch_domain
                    .as_ref()
                    .map(|cached| cached.matches(domain_values, domain_valid, binary, context))
                    .transpose()?
                    .unwrap_or(false);
                if !reused {
                    self.epoch_domain = None;
                    self.epoch_domain =
                        EpochDomainMap::try_new(domain_values, domain_valid, binary, context)?;
                }
                if let Some(mut epoch_map) = self.epoch_domain.take() {
                    let result = self.ingest_variable_dict_slots(
                        batch,
                        codes,
                        domain_valid,
                        domain_len,
                        &mut epoch_map.slots.values,
                        &mut epoch_map.null_group,
                        &mut selected,
                        prior_rows,
                        context,
                        mapping.as_deref_mut(),
                    );
                    if result.is_ok() {
                        self.epoch_domain = Some(epoch_map);
                    }
                    result?;
                    return self.publish_selected_rows(
                        batch,
                        &selected.values,
                        context,
                        batch_rows,
                        consume,
                    );
                }
            } else {
                self.epoch_domain = None;
                let snapshot = context.memory().snapshot();
                let available = snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes);
                if domain_len <= batch.len()
                    && bytes::<u32>(domain_len)? <= available / 16
                    && let Some(mut slots) =
                        ReservedVec::try_with_capacity(context.memory(), domain_len)?
                {
                    slots.values.resize(domain_len, u32::MAX);
                    let mut null_group = None;
                    self.ingest_variable_dict_slots(
                        batch,
                        codes,
                        domain_valid,
                        domain_len,
                        &mut slots.values,
                        &mut null_group,
                        &mut selected,
                        prior_rows,
                        context,
                        mapping.as_deref_mut(),
                    )?;
                    drop(slots);
                    return self.publish_selected_rows(
                        batch,
                        &selected.values,
                        context,
                        batch_rows,
                        consume,
                    );
                }
            }
        } else {
            self.epoch_domain = None;
        }
        let mut cache = batch.key_cache(context)?;
        for row in 0..batch.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if let Some((other, want_match)) = other
                && other.contains_rows(batch, row, context)? != want_match
            {
                continue;
            }
            let probe = cache
                .as_ref()
                .map(|cache| cache.probe(row))
                .transpose()?
                .flatten();
            if let Some(probe) = probe
                && let Some(group) = probe.group()
            {
                #[cfg(all(test, debug_assertions))]
                super::native_relational_observation::record(|counts| {
                    counts.key_cache_hits += 1;
                });
                if let Some(mapping) = &mut mapping {
                    mapping.values.push(group);
                }
                continue;
            }
            let Some(hash) = batch.hash(row, self.nulls_equal)? else {
                continue;
            };
            let equal = |old| {
                if old < prior_rows {
                    let (old_batch, old_row) = self.table.locate_batch(old)?;
                    batch.equal_batch(row, old_batch, old_row, self.nulls_equal)
                } else {
                    let selected_row = selected
                        .values
                        .get(old - prior_rows)
                        .ok_or_else(|| failed("pending set ordinal is absent"))?;
                    batch.equal(*selected_row, row, self.nulls_equal)
                }
            };
            let ordinal = prior_rows
                .checked_add(selected.values.len())
                .ok_or_else(|| failed("set cardinality overflow"))?;
            #[cfg(all(test, debug_assertions))]
            super::native_relational_observation::record(|counts| {
                counts.key_exact_lookups += 1;
            });
            let (group, inserted) =
                self.index
                    .insert_unique(hash, ordinal, context.cancellation(), equal)?;
            if inserted {
                if group != ordinal {
                    return Err(failed(
                        "interned group ordinal differs from insertion order",
                    ));
                }
                selected.values.push(row);
            }
            if let Some(mapping) = &mut mapping {
                mapping.values.push(group);
            }
            if let (Some(cache), Some(probe)) = (&mut cache, probe) {
                cache.insert(probe, group);
            }
        }
        drop(cache);
        self.publish_selected_rows(batch, &selected.values, context, batch_rows, consume)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn ingest_variable_dict_slots(
        &mut self,
        batch: &impl Rows,
        codes: &NativeNumericOwner,
        domain_valid: &Mask,
        domain_len: usize,
        slots: &mut [u32],
        null_group: &mut Option<usize>,
        selected: &mut ReservedVec<usize>,
        prior_rows: usize,
        context: &NativeExecutionContext<'_>,
        mut mapping: Option<&mut ReservedVec<usize>>,
    ) -> Result<()> {
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| {
            counts.key_cache_batches += 1;
        });
        // Reuse the original-width numeric owner so code type and validity
        // dispatch happen once per batch rather than once per input row.
        codes.visit_integer(|row, value| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let resolved_code = match value {
                None => None,
                Some(value) => {
                    let code = usize::try_from(value).map_err(vortex_error)?;
                    if code >= domain_len {
                        return Err(super::native_relational_keys::failed(
                            "dictionary code exceeds its value domain",
                        ));
                    }
                    domain_valid.value(code).then_some(code)
                }
            };
            if let Some(code) = resolved_code {
                let mapped = slots[code];
                if mapped != u32::MAX {
                    #[cfg(all(test, debug_assertions))]
                    super::native_relational_observation::record(|counts| {
                        counts.key_cache_hits += 1;
                    });
                    if let Some(mapping) = &mut mapping {
                        mapping.values.push(mapped as usize);
                    }
                    return Ok(());
                }
            } else {
                if !self.nulls_equal {
                    return Ok(());
                }
                if let Some(group) = *null_group {
                    #[cfg(all(test, debug_assertions))]
                    super::native_relational_observation::record(|counts| {
                        counts.key_cache_hits += 1;
                    });
                    if let Some(mapping) = &mut mapping {
                        mapping.values.push(group);
                    }
                    return Ok(());
                }
            }
            let Some(hash) = batch.hash(row, self.nulls_equal)? else {
                return Ok(());
            };
            let equal = |old| {
                if old < prior_rows {
                    let (old_batch, old_row) = self.table.locate_batch(old)?;
                    batch.equal_batch(row, old_batch, old_row, self.nulls_equal)
                } else {
                    let selected_row = selected
                        .values
                        .get(old - prior_rows)
                        .ok_or_else(|| failed("pending set ordinal is absent"))?;
                    batch.equal(*selected_row, row, self.nulls_equal)
                }
            };
            let ordinal = prior_rows
                .checked_add(selected.values.len())
                .ok_or_else(|| failed("set cardinality overflow"))?;
            #[cfg(all(test, debug_assertions))]
            super::native_relational_observation::record(|counts| {
                counts.key_exact_lookups += 1;
            });
            let (group, inserted) =
                self.index
                    .insert_unique(hash, ordinal, context.cancellation(), equal)?;
            if inserted {
                if group != ordinal {
                    return Err(failed(
                        "interned group ordinal differs from insertion order",
                    ));
                }
                selected.values.push(row);
            }
            if let Some(code) = resolved_code {
                if let Ok(slot) = u32::try_from(group)
                    && slot != u32::MAX
                {
                    slots[code] = slot;
                }
            } else if self.nulls_equal {
                *null_group = Some(group);
            }
            if let Some(mapping) = &mut mapping {
                mapping.values.push(group);
            }
            Ok(())
        })
    }

    fn publish_selected_rows(
        &mut self,
        batch: &impl Rows,
        selected: &[usize],
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        mut consume: Option<&mut dyn FnMut(ArrayRef) -> Result<()>>,
    ) -> Result<u64> {
        for rows in selected.chunks(batch_rows) {
            context.check_cancelled()?;
            let array = batch.gather(&self.fields, rows, context)?;
            self.table
                .push(Batch::new(array.clone(), &self.names, context)?)?;
            if let Some(consume) = consume.as_mut() {
                consume(array)?;
            }
        }
        if self.table.rows() != self.index.unique_rows() {
            return Err(failed("set index and retained native payload disagree"));
        }
        context.check_cancelled().map(|()| selected.len() as u64)
    }

    pub(super) fn table(&self) -> &Table {
        &self.table
    }

    pub(super) fn index(&self) -> &RowIndex {
        &self.index
    }
}
