// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Parse ORC file tail metadata structures from file.
//!
//! File tail structure:
//!
//! ```text
//! ------------------
//! |    Metadata    |
//! |                |
//! ------------------
//! |     Footer     |
//! |                |
//! ------------------
//! |  Postscript  |X|
//! ------------------
//! ```
//!
//! Where X is last byte in file indicating
//! Postscript length in bytes.
//!
//! Footer and Metadata lengths are encoded in Postscript.
//! Postscript is never compressed, Footer and Metadata
//! may be compressed depending Postscript config value.
//!
//! If they are compressed then their lengths indicate their
//! compressed lengths.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use arrow::datatypes::TimeUnit;

use bytes::{Bytes, BytesMut};
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};

use crate::compression::{Compression, Decompressor};
use crate::error::{self, EmptyFileSnafu, OutOfSpecSnafu, Result};
use crate::proto::{self, Footer, Metadata, PostScript};
use crate::schema::RootDataType;
use crate::statistics::ColumnStatistics;
use crate::stripe::StripeMetadata;

use crate::reader::ChunkReader;

const DEFAULT_FOOTER_SIZE: u64 = 16 * 1024;

#[path = "metadata_memory.rs"]
mod memory;

#[cfg(test)]
#[path = "metadata_tests.rs"]
mod tests;

/// The file's metadata.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    compression: Option<Compression>,
    root_data_type: RootDataType,
    number_of_rows: Option<u64>,
    file_format_version: String,
    /// Statistics of columns across entire file
    column_statistics: Vec<ColumnStatistics>,
    stripes: Vec<StripeMetadata>,
    user_custom_metadata: HashMap<String, Vec<u8>>,
    timestamp_units: Arc<HashMap<usize, TimeUnit>>,
    /// The maximum number of rows in each index entry (default 10,000)
    row_index_stride: Option<u32>,
}

impl FileMetadata {
    fn from_proto(
        postscript: &proto::PostScript,
        footer: &proto::Footer,
        metadata: &proto::Metadata,
    ) -> Result<Self> {
        let compression =
            Compression::from_proto(postscript.compression(), postscript.compression_block_size);
        let root_data_type = RootDataType::from_proto(&footer.types)?;
        let mut timestamp_units = HashMap::new();
        for (index, dtype) in footer.types.iter().enumerate() {
            for attribute in &dtype.attributes {
                if attribute.key.as_deref() != Some(crate::schema::ARROW_TIMESTAMP_UNIT_ATTRIBUTE) {
                    continue;
                }
                ensure!(
                    matches!(
                        dtype.kind(),
                        proto::r#type::Kind::Timestamp | proto::r#type::Kind::TimestampInstant
                    ),
                    OutOfSpecSnafu {
                        msg: "timestamp unit attribute on non-timestamp column"
                    }
                );
                let unit = match attribute.value.as_deref() {
                    Some("s") => TimeUnit::Second,
                    Some("ms") => TimeUnit::Millisecond,
                    Some("us") => TimeUnit::Microsecond,
                    Some("ns") => TimeUnit::Nanosecond,
                    _ => {
                        return OutOfSpecSnafu {
                            msg: "invalid timestamp unit attribute",
                        }
                        .fail()
                    }
                };
                ensure!(
                    timestamp_units.insert(index, unit).is_none(),
                    OutOfSpecSnafu {
                        msg: "duplicate timestamp unit attribute"
                    }
                );
            }
        }
        let number_of_rows = footer.number_of_rows;
        let column_statistics = if footer.statistics.len() == footer.types.len() {
            footer
                .statistics
                .iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        ensure!(
            metadata.stripe_stats.is_empty() || metadata.stripe_stats.len() == footer.stripes.len(),
            OutOfSpecSnafu {
                msg: "stripe stats length must equal the number of stripes"
            }
        );
        // TODO: confirm if this is valid
        let stripes = if metadata.stripe_stats.is_empty() {
            footer
                .stripes
                .iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>>>()?
        } else {
            footer
                .stripes
                .iter()
                .zip(metadata.stripe_stats.iter())
                .map(|(info, stats)| {
                    if stats.col_stats.len() == footer.types.len() {
                        StripeMetadata::try_from((info, stats))
                    } else {
                        StripeMetadata::try_from(info)
                    }
                })
                .collect::<Result<Vec<_>>>()?
        };
        let user_custom_metadata = footer
            .metadata
            .iter()
            .map(|kv| (kv.name().to_owned(), kv.value().to_vec()))
            .collect::<HashMap<_, _>>();

        let file_format_version = postscript
            .version
            .iter()
            .map(|v| v.to_string() + ".")
            .collect::<String>();
        let file_format_version = file_format_version
            .strip_suffix('.')
            .unwrap_or("")
            .to_string();

        let result = Self {
            compression,
            root_data_type,
            number_of_rows,
            file_format_version,
            column_statistics,
            stripes,
            user_custom_metadata,
            timestamp_units: Arc::new(timestamp_units),
            row_index_stride: footer.row_index_stride,
        };
        result.exact_number_of_rows()?;
        Ok(result)
    }

    pub fn number_of_rows(&self) -> u64 {
        self.number_of_rows.unwrap_or_default()
    }

    /// Exact footer total after checked agreement with every stripe count.
    /// A missing footer total is unavailable, even when stripe totals exist.
    pub fn exact_number_of_rows(&self) -> Result<Option<u64>> {
        let total = self.stripes.iter().try_fold(0_u64, |total, stripe| {
            total
                .checked_add(stripe.number_of_rows())
                .context(OutOfSpecSnafu {
                    msg: "stripe row total overflow",
                })
        })?;
        ensure!(
            self.number_of_rows
                .map_or(true, |declared| declared == total),
            OutOfSpecSnafu {
                msg: "file and stripe row counts disagree"
            }
        );
        Ok(self.number_of_rows)
    }

    fn validate_stripe_extents(&self, metadata_start: u64) -> Result<()> {
        let mut previous_end = 3_u64;
        for stripe in &self.stripes {
            let end = stripe
                .offset()
                .checked_add(stripe.index_length())
                .and_then(|n| n.checked_add(stripe.data_length()))
                .and_then(|n| n.checked_add(stripe.footer_length()))
                .context(OutOfSpecSnafu {
                    msg: "stripe extent overflow",
                })?;
            ensure!(
                stripe.offset() >= previous_end && end <= metadata_start,
                OutOfSpecSnafu {
                    msg: "stripe extent overlaps another stripe or file metadata"
                }
            );
            previous_end = end;
        }
        Ok(())
    }

    pub fn compression(&self) -> Option<Compression> {
        self.compression
    }

    pub fn root_data_type(&self) -> &RootDataType {
        &self.root_data_type
    }

    pub fn column_file_statistics(&self) -> &[ColumnStatistics] {
        &self.column_statistics
    }

    pub fn stripe_metadatas(&self) -> &[StripeMetadata] {
        &self.stripes
    }

    pub fn user_custom_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.user_custom_metadata
    }

    pub(crate) fn timestamp_units(&self) -> Arc<HashMap<usize, TimeUnit>> {
        Arc::clone(&self.timestamp_units)
    }

    /// Get the row index stride (rows per row group)
    ///
    /// Returns the number of rows per row group used for row-level indexes.
    /// Default is 10,000 according to ORC spec.
    ///
    /// If `None` is returned, it means row indexes are not enabled for this file.
    pub fn row_index_stride(&self) -> Option<usize> {
        self.row_index_stride.map(|s| s as usize)
    }

    pub fn file_format_version(&self) -> &str {
        &self.file_format_version
    }
}

pub fn read_metadata<R: ChunkReader>(reader: &mut R) -> Result<FileMetadata> {
    read_metadata_with_reservation(reader, &mut |_| Ok(()))
}

/// Parse metadata using a caller-owned reservation. The callback receives the
/// total requested credit before buffer growth and protobuf conversion. Keep
/// its final credit alive with the returned metadata. Encoded input buffers
/// remain owned by the reader. This shape model is not an allocator/RSS bound.
pub fn read_metadata_with_reservation<R: ChunkReader>(
    reader: &mut R,
    reserve: &mut dyn FnMut(u64) -> std::io::Result<()>,
) -> Result<FileMetadata> {
    read_metadata_controlled(reader, reserve, None)
}

/// Parse held metadata with separate caller-owned buffer/codec workspaces.
/// `reserve` retains the parsed schema/statistics credit; `memory` owns each
/// temporary decode allocation and supplies any required bounded codec.
pub fn read_metadata_with_memory<R: ChunkReader>(
    reader: &mut R,
    reserve: &mut dyn FnMut(u64) -> std::io::Result<()>,
    memory: Arc<dyn super::memory::DecoderMemory>,
) -> Result<FileMetadata> {
    read_metadata_controlled(reader, reserve, Some(memory))
}

fn read_metadata_controlled<R: ChunkReader>(
    reader: &mut R,
    reserve: &mut dyn FnMut(u64) -> std::io::Result<()>,
    memory: Option<Arc<dyn super::memory::DecoderMemory>>,
) -> Result<FileMetadata> {
    let file_len = reader.len();
    if file_len == 0 {
        return EmptyFileSnafu.fail();
    }
    ensure!(
        file_len >= 3 && reader.get_bytes(0, 3).context(error::IoSnafu)?.as_ref() == b"ORC",
        OutOfSpecSnafu {
            msg: "ORC header is missing"
        }
    );

    // Initial read of the file tail
    // Use a default size for first read in hopes of capturing all sections with one read
    // At worst need two reads to get all necessary bytes
    let assume_footer_len = file_len.min(DEFAULT_FOOTER_SIZE);
    let mut tail_bytes = reader
        .get_bytes(file_len - assume_footer_len, assume_footer_len)
        .context(error::IoSnafu)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as u64;
    tail_bytes.truncate(tail_bytes.len() - 1);

    if tail_bytes.len() < postscript_len as usize {
        return OutOfSpecSnafu {
            msg: "File too small for given postscript length",
        }
        .fail();
    }
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len as usize..])
        .context(error::DecodeProtoSnafu)?;
    let compression =
        Compression::from_proto(postscript.compression(), postscript.compression_block_size);
    tail_bytes.truncate(tail_bytes.len() - postscript_len as usize);

    let footer_length = postscript.footer_length.context(error::OutOfSpecSnafu {
        msg: "Footer length is empty",
    })?;
    let metadata_length = postscript.metadata_length.context(error::OutOfSpecSnafu {
        msg: "Metadata length is empty",
    })?;

    let metadata_start = metadata_start(file_len, postscript_len, footer_length, metadata_length)?;
    // Controlled decompression owns its actual output and codec workspace.
    // The callback only carries tail assembly and parser/retained shape here.
    let block_workspace = if memory.is_some() {
        0
    } else {
        postscript.compression_block_size.unwrap_or(256 * 1024)
    };
    let workspace = block_workspace
        .checked_mul(2)
        .and_then(|n| n.checked_add((footer_length + metadata_length).checked_mul(2)?))
        .and_then(|n| n.checked_add(8192))
        .context(OutOfSpecSnafu {
            msg: "metadata codec workspace overflow",
        })?;
    reserve(metadata_credit(0, workspace)?).context(error::IoSnafu)?;
    // Ensure we have enough bytes for Footer and Metadata
    let mut tail_bytes = if footer_length + metadata_length > tail_bytes.len() as u64 {
        // Need second read
        // -1 is the postscript length byte
        let offset = metadata_start;
        let bytes_to_read = (footer_length + metadata_length) - tail_bytes.len() as u64;
        let prepend_bytes = reader
            .get_bytes(offset, bytes_to_read)
            .context(error::IoSnafu)?;
        let mut all_bytes = BytesMut::with_capacity(prepend_bytes.len() + tail_bytes.len());
        all_bytes.extend_from_slice(&prepend_bytes);
        all_bytes.extend_from_slice(&tail_bytes);
        all_bytes.into()
    } else {
        tail_bytes
    };

    let mut retained = 0_u64;
    let footer: Footer = deserialize_admitted(
        tail_bytes.slice(tail_bytes.len() - footer_length as usize..),
        compression,
        &mut retained,
        workspace,
        reserve,
        memory.as_ref(),
    )?;
    tail_bytes.truncate(tail_bytes.len() - footer_length as usize);

    let metadata: Metadata = deserialize_admitted(
        tail_bytes.slice(tail_bytes.len() - metadata_length as usize..),
        compression,
        &mut retained,
        workspace,
        reserve,
        memory.as_ref(),
    )?;

    let result = FileMetadata::from_proto(&postscript, &footer, &metadata)?;
    result.validate_stripe_extents(metadata_start)?;
    drop(tail_bytes);
    drop(footer);
    drop(metadata);
    reserve(memory::retained(&result)?).context(error::IoSnafu)?;
    Ok(result)
}

#[cfg(feature = "async")]
pub async fn read_metadata_async<R: super::AsyncChunkReader>(
    reader: &mut R,
) -> Result<FileMetadata> {
    let file_len = reader.len().await.context(error::IoSnafu)?;
    if file_len == 0 {
        return EmptyFileSnafu.fail();
    }
    ensure!(
        file_len >= 3
            && reader
                .get_bytes(0, 3)
                .await
                .context(error::IoSnafu)?
                .as_ref()
                == b"ORC",
        OutOfSpecSnafu {
            msg: "ORC header is missing"
        }
    );

    // Initial read of the file tail
    // Use a default size for first read in hopes of capturing all sections with one read
    // At worst need two reads to get all necessary bytes
    let assume_footer_len = file_len.min(DEFAULT_FOOTER_SIZE);
    let mut tail_bytes = reader
        .get_bytes(file_len - assume_footer_len, assume_footer_len)
        .await
        .context(error::IoSnafu)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as u64;
    tail_bytes.truncate(tail_bytes.len() - 1);

    if tail_bytes.len() < postscript_len as usize {
        return OutOfSpecSnafu {
            msg: "File too small for given postscript length",
        }
        .fail();
    }
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len as usize..])
        .context(error::DecodeProtoSnafu)?;
    let compression =
        Compression::from_proto(postscript.compression(), postscript.compression_block_size);
    tail_bytes.truncate(tail_bytes.len() - postscript_len as usize);

    let footer_length = postscript.footer_length.context(error::OutOfSpecSnafu {
        msg: "Footer length is empty",
    })?;
    let metadata_length = postscript.metadata_length.context(error::OutOfSpecSnafu {
        msg: "Metadata length is empty",
    })?;

    let metadata_start = metadata_start(file_len, postscript_len, footer_length, metadata_length)?;
    // Ensure we have enough bytes for Footer and Metadata
    let mut tail_bytes = if footer_length + metadata_length > tail_bytes.len() as u64 {
        // Need second read
        // -1 is the postscript length byte
        let offset = metadata_start;
        let bytes_to_read = (footer_length + metadata_length) - tail_bytes.len() as u64;
        let prepend_bytes = reader
            .get_bytes(offset, bytes_to_read)
            .await
            .context(error::IoSnafu)?;
        let mut all_bytes = BytesMut::with_capacity(prepend_bytes.len() + tail_bytes.len());
        all_bytes.extend_from_slice(&prepend_bytes);
        all_bytes.extend_from_slice(&tail_bytes);
        all_bytes.into()
    } else {
        tail_bytes
    };

    let footer = deserialize_footer(
        tail_bytes.slice(tail_bytes.len() - footer_length as usize..),
        compression,
    )?;
    tail_bytes.truncate(tail_bytes.len() - footer_length as usize);

    let metadata = deserialize_footer_metadata(
        tail_bytes.slice(tail_bytes.len() - metadata_length as usize..),
        compression,
    )?;

    let result = FileMetadata::from_proto(&postscript, &footer, &metadata)?;
    result.validate_stripe_extents(metadata_start)?;
    Ok(result)
}

fn metadata_start(file_len: u64, postscript: u64, footer: u64, metadata: u64) -> Result<u64> {
    file_len
        .checked_sub(1)
        .and_then(|n| n.checked_sub(postscript))
        .and_then(|n| n.checked_sub(footer))
        .and_then(|n| n.checked_sub(metadata))
        .filter(|start| *start >= 3)
        .context(OutOfSpecSnafu {
            msg: "metadata extent exceeds file",
        })
}

fn metadata_credit(decoded_capacity: u64, workspace: u64) -> Result<u64> {
    // An empty repeated ColumnStatistics message needs only two wire bytes,
    // while its generated struct contains every optional statistics variant.
    // Include protobuf vector growth and conversion overlap before parsing;
    // replace this workspace estimate with retained shape credit afterwards.
    decoded_capacity
        .checked_mul(1024)
        .and_then(|n| n.checked_add(workspace))
        .and_then(|n| n.checked_add(8192))
        .context(OutOfSpecSnafu {
            msg: "metadata shape credit overflow",
        })
}

fn deserialize_admitted<T: Message + Default>(
    bytes: Bytes,
    compression: Option<Compression>,
    retained: &mut u64,
    workspace: u64,
    reserve: &mut dyn FnMut(u64) -> std::io::Result<()>,
    memory: Option<&Arc<dyn super::memory::DecoderMemory>>,
) -> Result<T> {
    reserve(metadata_credit(*retained, workspace)?).context(error::IoSnafu)?;
    let mut decoder = match memory {
        Some(memory) => Decompressor::new_with_memory(bytes, compression, Arc::clone(memory)),
        None => Decompressor::new(bytes, compression, vec![]),
    };
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let length = decoder.read(&mut chunk).context(error::IoSnafu)?;
        if length == 0 {
            break;
        }
        let wanted = buffer.len().checked_add(length).context(OutOfSpecSnafu {
            msg: "metadata buffer overflow",
        })?;
        if wanted > buffer.capacity() {
            let capacity = buffer
                .capacity()
                .checked_mul(2)
                .unwrap_or(wanted)
                .max(wanted);
            let total = retained
                .checked_add(capacity as u64)
                .context(OutOfSpecSnafu {
                    msg: "metadata capacity overflow",
                })?;
            reserve(metadata_credit(total, workspace)?).context(error::IoSnafu)?;
            buffer
                .try_reserve_exact(capacity - buffer.len())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))
                .context(error::IoSnafu)?;
        }
        buffer.extend_from_slice(&chunk[..length]);
    }
    *retained = retained
        .checked_add(buffer.capacity() as u64)
        .context(OutOfSpecSnafu {
            msg: "metadata retained capacity overflow",
        })?;
    reserve(metadata_credit(*retained, workspace)?).context(error::IoSnafu)?;
    T::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}

#[cfg(feature = "async")]
fn deserialize_footer(bytes: Bytes, compression: Option<Compression>) -> Result<Footer> {
    let mut buffer = vec![];
    Decompressor::new(bytes, compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    Footer::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}

#[cfg(feature = "async")]
fn deserialize_footer_metadata(bytes: Bytes, compression: Option<Compression>) -> Result<Metadata> {
    let mut buffer = vec![];
    Decompressor::new(bytes, compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    Metadata::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}
