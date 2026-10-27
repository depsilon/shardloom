//! Charged compressed buffers, restricted to one admitted row group's columns.

use super::{OwnedLocalRead, failed};
use bytes::Bytes;
use parquet::{
    arrow::ProjectionMask,
    errors::{ParquetError, Result as ParquetResult},
    file::{
        metadata::RowGroupMetaData,
        reader::{ChunkReader, Length},
    },
};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::io::Cursor;

struct Chunk {
    start: u64,
    bytes: Bytes,
}

pub(super) struct SelectedChunks {
    length: u64,
    chunks: Vec<Chunk>,
    _metadata: MemoryLease,
}

impl SelectedChunks {
    pub(super) fn read(
        input: &OwnedLocalRead,
        group: &RowGroupMetaData,
        mask: &ProjectionMask,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        let count = (0..group.num_columns())
            .filter(|&index| mask.leaf_included(index))
            .count();
        let capacity = count
            .checked_mul(std::mem::size_of::<Chunk>())
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| failed("column chunk metadata capacity overflow"))?;
        let credit = memory.reserve(capacity as u64)?;
        let mut chunks = Vec::new();
        chunks.try_reserve_exact(count).map_err(failed)?;
        if chunks.capacity() > count {
            return Err(failed("column chunk metadata exceeds admission"));
        }
        for (index, column) in group.columns().iter().enumerate() {
            if !mask.leaf_included(index) {
                continue;
            }
            // Validate signed metadata before the provider's byte_range(),
            // which asserts on negative offsets/sizes. No speculative reads
            // may cross the declared selected compressed chunk.
            let (start, length) = checked_range(
                column
                    .dictionary_page_offset()
                    .unwrap_or_else(|| column.data_page_offset()),
                column.compressed_size(),
                input.len(),
            )?;
            chunks.push(Chunk {
                start,
                bytes: input.bytes(start, length)?,
            });
        }
        Ok(Self {
            length: input.len(),
            chunks,
            _metadata: credit,
        })
    }

    fn chunk(&self, start: u64, length: usize) -> ParquetResult<(&Chunk, usize)> {
        self.chunks
            .iter()
            .find_map(|chunk| {
                let offset = usize::try_from(start.checked_sub(chunk.start)?).ok()?;
                (offset.checked_add(length)? <= chunk.bytes.len()).then_some((chunk, offset))
            })
            .ok_or_else(|| {
                ParquetError::General("read exceeds admitted selected column chunk".into())
            })
    }
}

fn checked_range(start: i64, length: i64, file_length: u64) -> Result<(u64, usize)> {
    let start = u64::try_from(start).map_err(failed)?;
    let length = usize::try_from(length).map_err(failed)?;
    if start
        .checked_add(length as u64)
        .is_none_or(|end| end > file_length)
    {
        return Err(failed("selected column chunk exceeds source length"));
    }
    Ok((start, length))
}

impl Length for SelectedChunks {
    fn len(&self) -> u64 {
        self.length
    }
}

impl ChunkReader for SelectedChunks {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        let (chunk, offset) = self.chunk(start, 1)?;
        Ok(Cursor::new(chunk.bytes.slice(offset..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let (chunk, offset) = self.chunk(start, length)?;
        Ok(chunk.bytes.slice(offset..offset + length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read as _;

    #[test]
    fn selected_chunks_cannot_read_gaps_or_extend_past_an_admitted_chunk() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let chunks = SelectedChunks {
            length: 100,
            chunks: vec![
                Chunk {
                    start: 10,
                    bytes: Bytes::from_static(b"first"),
                },
                Chunk {
                    start: 20,
                    bytes: Bytes::from_static(b"last"),
                },
            ],
            _metadata: memory.reserve(4096).unwrap(),
        };
        let mut cursor = chunks.get_read(12).unwrap();
        let mut data = Vec::new();
        cursor.read_to_end(&mut data).unwrap();
        assert_eq!(data, b"rst");
        assert_eq!(chunks.get_bytes(21, 3).unwrap().as_ref(), b"ast");
        for (start, length) in [(0, 1), (14, 2), (15, 1), (19, 2), (24, 1), (u64::MAX, 1)] {
            assert!(chunks.get_bytes(start, length).is_err());
        }
        assert!(chunks.get_read(15).is_err());
        assert!(chunks.get_read(100).is_err());
    }

    #[test]
    fn selected_chunk_metadata_rejects_negative_and_out_of_file_ranges() {
        for (start, length) in [(-1, 1), (0, -1), (99, 2), (101, 0), (i64::MAX, i64::MAX)] {
            assert!(checked_range(start, length, 100).is_err());
        }
        assert_eq!(checked_range(90, 10, 100).unwrap(), (90, 10));
    }
}
