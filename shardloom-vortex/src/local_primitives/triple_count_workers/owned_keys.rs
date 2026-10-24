//! Transfer only bound identities and native numeric owners to the existing
//! count job. Chunk text values can be released once the producer binds IDs.

use super::{
    AggregateDirectColumnAccessor, DirectKeys, Result, Roles, RoutedKeys,
    aggregate_direct_integer_key_slice, failed,
};

pub(super) struct OwnedKeys {
    numeric: AggregateDirectColumnAccessor,
    // None means both grouping roles borrow the same transferred numeric owner.
    minute: Option<AggregateDirectColumnAccessor>,
    row_ids: Vec<u32>,
    ids: Vec<u64>,
    prepared: bool,
}

impl OwnedKeys {
    /// Call only after the borrowed `DirectKeys` admission proves the roles and
    /// ID domain. Moving owners preserves original widths and allocates no copy.
    pub(super) fn new(
        accessors: Vec<AggregateDirectColumnAccessor>,
        roles: Roles,
        ids: Vec<u64>,
    ) -> Result<Self> {
        let (mut numeric, mut minute, mut row_ids) = (None, None, None);
        for (index, accessor) in accessors.into_iter().enumerate() {
            if index == roles.numeric_column {
                numeric = Some(accessor);
            } else if index == roles.minute_column {
                minute = Some(accessor);
            } else if index == roles.string_column {
                let AggregateDirectColumnAccessor::Utf8Dictionary { row_ids: codes, .. } = accessor
                else {
                    return Err(failed("transferred text lost its admitted dictionary"));
                };
                row_ids = Some(codes);
            }
        }
        if roles.minute_column != roles.numeric_column && minute.is_none() {
            return Err(failed("transferred minute owner is missing"));
        }
        Ok(Self {
            numeric: numeric.ok_or_else(|| failed("transferred numeric owner is missing"))?,
            minute,
            row_ids: row_ids.ok_or_else(|| failed("transferred dictionary codes are missing"))?,
            ids,
            prepared: roles.minute_column_prepared,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.row_ids.len()
    }

    pub(super) fn retained_bytes(&self) -> Result<u64> {
        let adapter = self
            .row_ids
            .capacity()
            .checked_mul(size_of::<u32>())
            .and_then(|bytes| bytes.checked_add(self.ids.capacity().checked_mul(size_of::<u64>())?))
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("transferred input capacity overflow"))?;
        [
            numeric_bytes(&self.numeric)?,
            self.minute
                .as_ref()
                .map(numeric_bytes)
                .transpose()?
                .unwrap_or(0),
        ]
        .into_iter()
        .try_fold(adapter, |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or_else(|| failed("transferred native input size overflow"))
        })
    }

    pub(super) fn route(&self, check: impl FnMut() -> Result<()>) -> Result<RoutedKeys> {
        let numeric = aggregate_direct_integer_key_slice(&self.numeric)
            .ok_or_else(|| failed("transferred numeric owner lost its typed slice"))?;
        let minute =
            aggregate_direct_integer_key_slice(self.minute.as_ref().unwrap_or(&self.numeric))
                .ok_or_else(|| failed("transferred minute owner lost its typed slice"))?;
        DirectKeys {
            numeric,
            minute,
            row_ids: &self.row_ids,
            ids: &self.ids,
            prepared: self.prepared,
        }
        .route(check)
    }
}

fn numeric_bytes(accessor: &AggregateDirectColumnAccessor) -> Result<u64> {
    match accessor {
        AggregateDirectColumnAccessor::NativeNumeric(owner) => Ok(owner.nbytes_estimate()),
        AggregateDirectColumnAccessor::UInt64(values) => vector_bytes(values.capacity()),
        AggregateDirectColumnAccessor::Int64(values) => vector_bytes(values.capacity()),
        _ => Err(failed("transferred input is not an admitted numeric owner")),
    }
}

fn vector_bytes(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(size_of::<u64>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("transferred numeric capacity overflow"))
}
