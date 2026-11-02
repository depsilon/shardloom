// SPDX-License-Identifier: Apache-2.0
//! Stripe-local metadata admission, extent validation and retained ownership.

use crate::{
    column::ColumnFooter,
    compression::{Compression, Decompressor},
    error::{self, OutOfSpecSnafu, Result},
    proto::{self, StripeFooter},
    reader::memory::{DecoderBuffer, DecoderMemory},
    schema::RootDataType,
    stripe::StripeMetadata,
};
use bytes::Bytes;
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};
use std::{collections::HashSet, sync::Arc};

pub(super) fn read(
    bytes: Bytes,
    compression: Option<Compression>,
    memory: Option<&Arc<dyn DecoderMemory>>,
    info: &StripeMetadata,
    schema: &RootDataType,
) -> Result<Arc<ColumnFooter>> {
    let decoder = match memory {
        Some(memory) => Decompressor::new_with_memory(bytes, compression, Arc::clone(memory)),
        None => Decompressor::new(bytes, compression, Vec::new()),
    };
    let mut buffer = DecoderBuffer::new(Vec::new(), memory.cloned());
    buffer
        .read_block(decoder, usize::MAX)
        .context(error::IoSnafu)?;
    // A repeated empty message takes two wire bytes; the generated fixed
    // structs and vector growth dominate that shape. Include validation's set
    // storage before parsing, then replace it with actual retained capacities.
    let shape = buffer
        .bytes
        .len()
        .checked_mul(256)
        .and_then(|n| n.checked_add(8192))
        .context(OutOfSpecSnafu {
            msg: "stripe footer shape overflow",
        })?;
    let mut credit = memory
        .map(|memory| memory.reserve(shape))
        .transpose()
        .context(error::IoSnafu)?;
    let footer = StripeFooter::decode(buffer.bytes.as_slice()).context(error::DecodeProtoSnafu)?;
    validate(&footer, info, schema)?;
    if let Some(credit) = &mut credit {
        credit.resize(retained(&footer)?).context(error::IoSnafu)?;
    }
    Ok(Arc::new(ColumnFooter {
        data: Arc::new(footer),
        _credit: credit,
        structure_credit: None,
    }))
}

#[cfg(test)]
#[path = "stripe_footer_tests.rs"]
mod tests;

fn validate(footer: &StripeFooter, info: &StripeMetadata, schema: &RootDataType) -> Result<()> {
    ensure!(
        footer.encryption.is_empty(),
        OutOfSpecSnafu {
            msg: "encrypted ORC stripes are not supported"
        }
    );
    ensure!(
        footer.columns.len() > schema.maximum_column_index(),
        OutOfSpecSnafu {
            msg: "stripe column encodings do not cover the file schema"
        }
    );
    for column in &footer.columns {
        ensure!(
            proto::column_encoding::Kind::try_from(column.kind.unwrap_or_default()).is_ok(),
            OutOfSpecSnafu {
                msg: "unknown stripe column encoding"
            }
        );
    }
    if let Some(zone) = &footer.writer_timezone {
        ensure!(
            zone.parse::<chrono_tz::Tz>().is_ok(),
            OutOfSpecSnafu {
                msg: "invalid stripe writer timezone"
            }
        );
    }
    let mut seen = HashSet::new();
    seen.try_reserve(footer.streams.len())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))
        .context(error::IoSnafu)?;
    let mut length = 0_u64;
    for stream in &footer.streams {
        let bytes = stream.length.context(OutOfSpecSnafu {
            msg: "stripe stream length is missing",
        })?;
        let kind =
            proto::stream::Kind::try_from(stream.kind.unwrap_or_default()).map_err(|_| {
                OutOfSpecSnafu {
                    msg: "unknown stripe stream kind",
                }
                .build()
            })?;
        let column = stream.column() as usize;
        ensure!(
            column == 0 || schema.contains_column_index(column),
            OutOfSpecSnafu {
                msg: "stripe stream column is outside the file schema"
            }
        );
        ensure!(
            seen.insert((column, kind)),
            OutOfSpecSnafu {
                msg: "duplicate stripe stream"
            }
        );
        length = length.checked_add(bytes).context(OutOfSpecSnafu {
            msg: "stripe stream extent overflow",
        })?;
    }
    let expected = info
        .index_length()
        .checked_add(info.data_length())
        .context(OutOfSpecSnafu {
            msg: "stripe data extent overflow",
        })?;
    ensure!(
        length == expected,
        OutOfSpecSnafu {
            msg: "stripe streams do not cover their declared extent"
        }
    );
    Ok(())
}

fn retained(footer: &StripeFooter) -> Result<usize> {
    footer
        .streams
        .capacity()
        .checked_mul(std::mem::size_of::<proto::Stream>())
        .and_then(|n| {
            n.checked_add(
                footer
                    .columns
                    .capacity()
                    .checked_mul(std::mem::size_of::<proto::ColumnEncoding>())?,
            )
        })
        .and_then(|n| {
            n.checked_add(
                footer
                    .encryption
                    .capacity()
                    .checked_mul(std::mem::size_of::<proto::StripeEncryptionVariant>())?,
            )
        })
        .and_then(|n| n.checked_add(footer.writer_timezone.as_ref().map_or(0, String::capacity)))
        .and_then(|n| n.checked_add(std::mem::size_of::<StripeFooter>() + 256))
        .context(OutOfSpecSnafu {
            msg: "stripe retained metadata overflow",
        })
}
