// SPDX-License-Identifier: Apache-2.0
//! Conservative retained shape credit, distinct from parser workspace and RSS.

use super::FileMetadata;
use crate::{
    error::{OutOfSpecSnafu, Result},
    schema::{DataType, NamedColumn},
    statistics::{ColumnStatistics, TypeStatistics},
};
use snafu::OptionExt;

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).context(OutOfSpecSnafu {
        msg: "retained metadata size overflow",
    })
}

fn array<T>(capacity: usize) -> Result<u64> {
    (capacity as u64)
        .checked_mul(std::mem::size_of::<T>() as u64)
        .context(OutOfSpecSnafu {
            msg: "retained metadata capacity overflow",
        })
}

fn columns(columns: &[NamedColumn]) -> Result<u64> {
    // The provider builds these vectors with collect. Include spare capacity,
    // the root's column-index set and allocation bookkeeping per schema node.
    columns
        .iter()
        .try_fold(array::<NamedColumn>(columns.len())?, |bytes, column| {
            add(
                add(bytes, column.name().len() as u64 + 256)?,
                dtype(column.data_type())?,
            )
        })
}

fn dtype(value: &DataType) -> Result<u64> {
    match value {
        DataType::Struct { children, .. } => columns(children),
        DataType::List { child, .. } => add(256, dtype(child)?),
        DataType::Map { key, value, .. } => add(add(512, dtype(key)?)?, dtype(value)?),
        DataType::Union { variants, .. } => variants
            .iter()
            .try_fold(array::<DataType>(variants.capacity())?, |bytes, value| {
                add(add(bytes, 256)?, dtype(value)?)
            }),
        _ => Ok(0),
    }
}

fn statistics(values: &[ColumnStatistics], capacity: usize) -> Result<u64> {
    values
        .iter()
        .try_fold(array::<ColumnStatistics>(capacity)?, |bytes, value| {
            let heap = match value.type_statistics() {
                Some(TypeStatistics::String {
                    lower_bound,
                    upper_bound,
                    ..
                }) => add(lower_bound.capacity() as u64, upper_bound.capacity() as u64)?,
                Some(TypeStatistics::Decimal { min, max, sum }) => add(
                    add(min.capacity() as u64, max.capacity() as u64)?,
                    sum.capacity() as u64,
                )?,
                _ => 0,
            };
            add(bytes, heap)
        })
}

pub(super) fn retained(metadata: &FileMetadata) -> Result<u64> {
    let mut bytes = add(8192, columns(metadata.root_data_type.children())?)?;
    bytes = add(
        bytes,
        statistics(
            &metadata.column_statistics,
            metadata.column_statistics.capacity(),
        )?,
    )?;
    bytes = add(
        bytes,
        array::<crate::stripe::StripeMetadata>(metadata.stripes.capacity())?,
    )?;
    for stripe in &metadata.stripes {
        let stats = stripe.column_statistics();
        bytes = add(bytes, statistics(stats, stats.len().saturating_mul(2))?)?;
    }
    bytes = add(bytes, metadata.file_format_version.capacity() as u64)?;
    bytes = add(
        bytes,
        array::<(String, Vec<u8>)>(metadata.user_custom_metadata.capacity().saturating_mul(2))?,
    )?;
    for (key, value) in &metadata.user_custom_metadata {
        bytes = add(add(bytes, key.capacity() as u64)?, value.capacity() as u64)?;
    }
    add(
        bytes,
        array::<(usize, arrow::datatypes::TimeUnit)>(
            metadata.timestamp_units.capacity().saturating_mul(2),
        )?,
    )
}
