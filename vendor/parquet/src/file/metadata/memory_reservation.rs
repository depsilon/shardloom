// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom integration with the pinned Parquet metadata parser.

use super::{
    MetadataMemoryReservation, ParquetMetaDataOptions, RowGroupMetaData, memory::HeapSize,
};
use crate::{
    errors::{ParquetError, Result},
    parquet_thrift::{
        ElementType, FieldType, ThriftCompactInputProtocol, ThriftSliceInputProtocol,
        validate_list_type,
    },
};

/// Preserve the existing conservative parsing allowance for one metadata
/// region, while releasing it between row groups. Bytes already retained by
/// parsed groups are measured with the provider's own heap-size implementation.
fn workspace(encoded_bytes: usize) -> Result<usize> {
    encoded_bytes
        .checked_mul(32)
        .and_then(|n| n.checked_add(8192))
        .ok_or_else(|| general_err!("metadata parsing workspace overflow"))
}

pub(super) struct FooterReservation<'a> {
    owner: &'a dyn MetadataMemoryReservation,
    base: usize,
    retained: usize,
}

impl<'a> FooterReservation<'a> {
    pub(super) fn new(
        buf: &[u8],
        options: Option<&'a ParquetMetaDataOptions>,
    ) -> Result<Option<Self>> {
        let Some(owner) = options.and_then(ParquetMetaDataOptions::memory_reservation) else {
            return Ok(None);
        };
        let mut input = ThriftSliceInputProtocol::new(buf);
        let mut last_field = 0;
        let mut group_bytes = 0usize;
        let mut group_count = None;
        // This preflight only borrows wire bytes. It allocates no footer model,
        // and validates collection framing before any count-based reservation.
        loop {
            owner.resize(0)?;
            let field = input.read_field_begin(last_field)?;
            if field.field_type == FieldType::Stop {
                break;
            }
            if field.id == 4 {
                if field.field_type != FieldType::List || group_count.is_some() {
                    return Err(general_err!(
                        "invalid or repeated row_groups metadata field"
                    ));
                }
                let list = input.read_list_begin()?;
                validate_list_type(ElementType::Struct, &list)?;
                let count = usize::try_from(list.size)
                    .map_err(|_| general_err!("row group count overflow"))?;
                if count > input.as_slice().len() {
                    return Err(general_err!(
                        "row group count exceeds remaining footer bytes"
                    ));
                }
                group_count = Some(count);
                for _ in 0..count {
                    owner.resize(0)?;
                    let before = input.as_slice().len();
                    input.skip(FieldType::Struct)?;
                    group_bytes = group_bytes
                        .checked_add(before - input.as_slice().len())
                        .ok_or_else(|| general_err!("row group metadata size overflow"))?;
                }
            } else {
                input.skip(field.field_type)?;
            }
            last_field = field.id;
        }
        let count =
            group_count.ok_or_else(|| general_err!("Required field row_groups is missing"))?;
        let vector = count
            .checked_mul(std::mem::size_of::<RowGroupMetaData>())
            .ok_or_else(|| general_err!("row group vector capacity overflow"))?;
        let base = workspace(buf.len() - group_bytes)?
            .checked_add(vector)
            .ok_or_else(|| general_err!("metadata reservation overflow"))?;
        owner.resize(base)?;
        Ok(Some(Self {
            owner: owner.as_ref(),
            base,
            retained: 0,
        }))
    }

    pub(super) fn before_group(&self, input: &ThriftSliceInputProtocol<'_>) -> Result<()> {
        let bytes = input.as_slice();
        let mut probe = ThriftSliceInputProtocol::new(bytes);
        probe.skip(FieldType::Struct)?;
        let group = workspace(bytes.len() - probe.as_slice().len())?;
        let required = self
            .base
            .checked_add(self.retained)
            .and_then(|n| n.checked_add(group))
            .ok_or_else(|| general_err!("metadata reservation overflow"))?;
        self.owner.resize(required)
    }

    pub(super) fn after_group(&mut self, group: &RowGroupMetaData) -> Result<()> {
        self.retained = self
            .retained
            .checked_add(group.heap_size())
            .ok_or_else(|| general_err!("retained row group metadata overflow"))?;
        self.owner.resize(
            self.base
                .checked_add(self.retained)
                .ok_or_else(|| general_err!("metadata reservation overflow"))?,
        )
    }
}
