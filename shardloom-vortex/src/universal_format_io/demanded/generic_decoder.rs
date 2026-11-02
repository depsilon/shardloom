//! Preserve concrete provider controls instead of erasing them behind Iterator.

use super::{OwnedLocalRead, Result, failed};
use crate::universal_format_io::owned_batch::{BatchResult, OwnedBatch};
#[cfg(test)]
use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use shardloom_core::ShardLoomError;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{
    io::{BufRead, BufReader, Read},
    sync::{Arc, Mutex},
};

pub(super) enum Decoder {
    Parquet(parquet::arrow::arrow_reader::ParquetRecordBatchReader),
    Orc(orc_rust::ArrowReader<OwnedLocalRead>),
    Avro(Box<arrow_avro::reader::Reader<AvroInput>>),
    Text(crate::universal_format_io::text_stream::TextReader),
    // IPC messages are physically framed. Text owns its per-record controller.
    Ipc(super::super::ipc::Reader),
}

impl Decoder {
    pub(super) fn set_batch_size(&mut self, rows: usize) -> Result<()> {
        match self {
            Self::Parquet(reader) => reader.set_batch_size(rows).map_err(failed),
            Self::Orc(reader) => reader.set_batch_size(rows).map_err(failed),
            Self::Avro(reader) => reader.set_batch_size(rows).map_err(source_error),
            Self::Text(reader) => reader.limit_next_batch(rows),
            Self::Ipc(_) => Err(failed("source provider has no next-batch control")),
        }
    }

    pub(super) fn credited_input(&self) -> bool {
        matches!(
            self,
            Self::Text(_) | Self::Orc(_) | Self::Avro(_) | Self::Ipc(_)
        )
    }

    pub(super) fn can_resize(&self) -> bool {
        !matches!(self, Self::Ipc(_))
    }

    pub(super) fn prepare_rows(&mut self) -> Result<Option<u64>> {
        match self {
            Self::Orc(reader) => reader
                .prepare_next_stripe()
                .map(|ready| (!ready).then_some(0))
                .map_err(source_error),
            Self::Avro(reader) => reader
                .prepare_next_block()
                .map(|rows| Some(rows as u64))
                .map_err(source_error),
            _ => Ok(None),
        }
    }
}

impl Iterator for Decoder {
    type Item = BatchResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Parquet(reader) => reader.next().map(|batch| batch.map(OwnedBatch::from)),
            Self::Orc(reader) => reader.next().map(|batch| batch.map(OwnedBatch::from)),
            Self::Avro(reader) => reader.next().map(|batch| batch.map(OwnedBatch::from)),
            Self::Text(reader) => reader.next_owned(),
            Self::Ipc(reader) => reader
                .next()
                .map_err(|error| ArrowError::ExternalError(Box::new(error)))
                .transpose(),
        }
    }
}

pub(super) struct AvroSchema {
    pub(super) schema: arrow_schema::SchemaRef,
    pub(super) credit: Arc<dyn arrow_avro::reader::DecoderMemoryReservation>,
}

pub(super) fn avro_schema(
    input: OwnedLocalRead,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<AvroSchema> {
    let reader = arrow_avro::reader::ReaderBuilder::new()
        .with_decoder_memory(Arc::new(AvroDecoderMemory {
            memory: memory.clone(),
            cancellation: cancellation.clone(),
        }))
        .build(AvroInput::new(input, memory, cancellation)?)
        .map_err(source_error)?;
    Ok(AvroSchema {
        schema: reader.schema(),
        credit: reader
            .schema_memory_reservation()
            .ok_or_else(|| failed("Avro schema memory owner missing"))?,
    })
}

pub(super) fn avro(
    input: OwnedLocalRead,
    projection: Option<Vec<usize>>,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Decoder> {
    let admission = Arc::new(AvroBlockAdmission {
        credit: Mutex::new(memory.reserve(0)?),
        cancellation: cancellation.clone(),
    });
    let mut builder = arrow_avro::reader::ReaderBuilder::new()
        .with_batch_size(super::super::batch_work::MAX_ROWS)
        .with_block_memory_reservation(admission)
        .with_decoder_memory(Arc::new(AvroDecoderMemory {
            memory: memory.clone(),
            cancellation: cancellation.clone(),
        }));
    if let Some(projection) = projection {
        builder = builder.with_projection(projection);
    }
    let reader = builder
        .build(AvroInput::new(input, memory, cancellation)?)
        .map_err(source_error)?;
    Ok(Decoder::Avro(Box::new(reader)))
}

/// The actual buffered input allocation, independent of block/schema storage.
pub(super) struct AvroInput {
    inner: BufReader<super::ColumnarCursor>,
    _credit: MemoryLease,
}

impl AvroInput {
    fn new(
        input: OwnedLocalRead,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let capacity = input.len().clamp(1, 8192);
        let credit = memory.reserve(capacity)?;
        let inner = BufReader::with_capacity(
            usize::try_from(capacity).map_err(failed)?,
            super::cursor(input),
        );
        Ok(Self {
            inner,
            _credit: credit,
        })
    }
}

impl Read for AvroInput {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(bytes)
    }
}

impl BufRead for AvroInput {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, bytes: usize) {
        self.inner.consume(bytes);
    }
}

#[derive(Debug)]
struct AvroBlockAdmission {
    credit: Mutex<MemoryLease>,
    cancellation: CancellationToken,
}

#[derive(Debug)]
struct AvroDecoderMemory {
    memory: LiveMemoryPool,
    cancellation: CancellationToken,
}

#[derive(Debug)]
struct AvroRecordCredit {
    _credit: MemoryLease,
}

impl arrow_avro::reader::DecoderMemoryReservation for AvroRecordCredit {}

impl arrow_avro::reader::DecoderMemory for AvroDecoderMemory {
    fn reserve(
        &self,
        bytes: usize,
    ) -> std::result::Result<Box<dyn arrow_avro::reader::DecoderMemoryReservation>, ArrowError>
    {
        self.cancellation
            .check()
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
        let bytes =
            u64::try_from(bytes).map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
        let credit = self
            .memory
            .reserve(bytes)
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
        Ok(Box::new(AvroRecordCredit { _credit: credit }))
    }
}

impl arrow_avro::reader::BlockMemoryReservation for AvroBlockAdmission {
    fn resize(&self, bytes: usize) -> std::result::Result<(), ArrowError> {
        self.cancellation
            .check()
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
        self.credit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .resize(
                u64::try_from(bytes).map_err(|error| ArrowError::ExternalError(Box::new(error)))?,
            )
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))
    }
}

pub(in crate::universal_format_io::demanded) fn source_error(error: ArrowError) -> ShardLoomError {
    let mut cause: &(dyn std::error::Error + 'static) = &error;
    // Providers can wrap a pool/cancellation failure in codec and I/O errors.
    // Preserve the typed native failure instead of replacing its diagnostic.
    for _ in 0..16 {
        if let Some(error) = cause.downcast_ref::<ShardLoomError>() {
            return error.clone();
        }
        let next = cause
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .map(|inner| inner as &(dyn std::error::Error + 'static))
            .or_else(|| cause.source());
        let Some(next) = next else {
            break;
        };
        cause = next;
    }
    failed(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn provider_error_wrappers_preserve_native_admission_and_cancellation() {
        let pool = LiveMemoryPool::new(1).unwrap();
        let denied = pool.reserve(2).unwrap_err();
        let token = CancellationToken::default();
        token.cancel();
        let cancelled = token.check().unwrap_err();
        for expected in [denied, cancelled] {
            let nested = std::io::Error::other(std::io::Error::other(expected.clone()));
            let wrapped = ArrowError::ExternalError(Box::new(nested));
            assert_eq!(source_error(wrapped), expected);
        }
    }

    #[test]
    fn avro_record_credits_share_native_denial_cancellation_and_output_lifetime() {
        use arrow_avro::reader::DecoderMemory;

        let pool = LiveMemoryPool::new(16).unwrap();
        let cancellation = CancellationToken::default();
        let memory = AvroDecoderMemory {
            memory: pool.clone(),
            cancellation: cancellation.clone(),
        };
        let held = memory.reserve(8).unwrap();
        assert_eq!(pool.snapshot().reserved_bytes, 8);
        let expected = pool.reserve(9).unwrap_err();
        assert_eq!(source_error(memory.reserve(9).unwrap_err()), expected);
        cancellation.cancel();
        assert_eq!(
            source_error(memory.reserve(1).unwrap_err()),
            cancellation.check().unwrap_err()
        );
        drop(memory);
        assert_eq!(pool.snapshot().reserved_bytes, 8);
        drop(held);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn orc_zstd_workspace_uses_the_same_pool_and_preserves_typed_denial() {
        let expected = b"same shared native workspace".repeat(300);
        let frame = zstd::encode_all(expected.as_slice(), 1).unwrap();
        let pool = LiveMemoryPool::new(1 << 20).unwrap();
        let mut output = vec![0; expected.len()];
        let memory = orc_memory(&pool, &CancellationToken::default());
        assert_eq!(
            memory.decode_zstd(&frame, &mut output).unwrap(),
            expected.len()
        );
        assert_eq!(output, expected);
        let workspace = pool.snapshot().peak_reserved_bytes;
        assert!(workspace > 0);
        assert_eq!(pool.snapshot().reserved_bytes, workspace);
        let pressure = pool
            .reserve(pool.snapshot().limit_bytes - workspace)
            .unwrap();
        // A second physical block reuses the actual context with no new grant.
        output.fill(0);
        assert_eq!(
            memory.decode_zstd(&frame, &mut output).unwrap(),
            expected.len()
        );
        assert_eq!(output, expected);
        drop((pressure, memory));
        assert_eq!(pool.snapshot().reserved_bytes, 0);

        let tight = LiveMemoryPool::new(workspace).unwrap();
        let retained = tight.reserve(1).unwrap();
        let expected_error = tight.reserve(workspace).unwrap_err();
        let error = orc_memory(&tight, &CancellationToken::default())
            .decode_zstd(&frame, &mut output)
            .unwrap_err();
        assert_eq!(
            source_error(ArrowError::ExternalError(Box::new(error))),
            expected_error
        );
        assert_eq!(tight.snapshot().reserved_bytes, 1);
        drop(retained);
        assert_eq!(tight.snapshot().reserved_bytes, 0);

        let cancelled = CancellationToken::default();
        cancelled.cancel();
        let error = orc_memory(&tight, &cancelled)
            .decode_zstd(&frame, &mut output)
            .unwrap_err();
        assert_eq!(
            source_error(ArrowError::ExternalError(Box::new(error))),
            cancelled.check().unwrap_err()
        );
        assert_eq!(tight.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn orc_zstd_validates_complete_members_and_releases_failed_workspace() {
        let first = zstd::encode_all(&b"first"[..], 1).unwrap();
        let second = zstd::encode_all(&b"second"[..], 1).unwrap();
        let mut members = first.clone();
        members.extend_from_slice(&[0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0]);
        members.extend_from_slice(&second);
        let pool = LiveMemoryPool::new(1 << 20).unwrap();
        let memory = orc_memory(&pool, &CancellationToken::default());
        let mut output = [0; 11];
        assert_eq!(memory.decode_zstd(&members, &mut output).unwrap(), 11);
        assert_eq!(&output, b"firstsecond");
        let workspace = pool.snapshot().reserved_bytes;
        assert!(workspace > 0);
        assert!(memory.decode_zstd(&members, &mut [0; 1]).is_err());
        assert_eq!(pool.snapshot().reserved_bytes, workspace);
        drop(memory);
        assert_eq!(pool.snapshot().reserved_bytes, 0);

        for invalid in [vec![], first[..first.len() - 1].to_vec(), {
            let mut trailing = first.clone();
            trailing.extend_from_slice(&0xFD2F_B527_u32.to_le_bytes());
            trailing
        }] {
            let fresh = LiveMemoryPool::new(1 << 20).unwrap();
            assert!(
                orc_memory(&fresh, &CancellationToken::default())
                    .decode_zstd(&invalid, &mut output)
                    .is_err()
            );
            assert_eq!(fresh.snapshot().reserved_bytes, 0);
            assert_eq!(fresh.snapshot().peak_reserved_bytes, 0);
        }
    }

    fn orc_zstd_compression() -> orc_rust::compression::Compression {
        let batch = RecordBatch::try_from_iter([(
            "n",
            Arc::new(arrow_array::Int64Array::from(vec![7])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = orc_rust::ArrowWriterBuilder::new(&mut bytes, batch.schema())
            .with_compression(orc_rust::compression::CompressionType::Zstd)
            .with_compression_block_size(4096)
            .try_build()
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        orc_rust::ArrowReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .shared_file_metadata()
            .compression()
            .unwrap()
    }

    fn physical_block(payload: &[u8], original: bool) -> Vec<u8> {
        let header = (u32::try_from(payload.len()).unwrap() << 1) | u32::from(original);
        let mut bytes = header.to_le_bytes()[..3].to_vec();
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn orc_zstd_prepares_later_compressed_blocks_before_row_allocation() {
        let compression = orc_zstd_compression();
        let tail = b"later decoded values".repeat(100);
        for compressed in [false, true] {
            let mut bytes = physical_block(b"prefix-", true);
            let payload = if compressed {
                zstd::encode_all(tail.as_slice(), 1).unwrap()
            } else {
                tail.clone()
            };
            bytes.extend(physical_block(&payload, !compressed));
            let pool = LiveMemoryPool::new(1 << 20).unwrap();
            let memory = orc_memory(&pool, &CancellationToken::default());
            let mut reader = orc_rust::compression::Decompressor::new_with_memory(
                bytes.into(),
                Some(compression),
                Arc::clone(&memory),
            );
            reader.prepare().unwrap();
            let reserved = pool.snapshot().reserved_bytes;
            assert_eq!(reserved > 0, compressed);
            let pressure = pool
                .reserve(pool.snapshot().limit_bytes - reserved)
                .unwrap();
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).unwrap();
            assert_eq!(actual, [b"prefix-".as_slice(), tail.as_slice()].concat());
            drop((pressure, reader, memory));
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }
}

pub(in crate::universal_format_io::demanded) fn orc_memory(
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Arc<dyn orc_rust::reader::memory::DecoderMemory> {
    Arc::new(OrcMemory {
        memory: memory.clone(),
        cancellation: cancellation.clone(),
        zstd: Mutex::default(),
    })
}

#[derive(Debug)]
struct OrcMemory {
    memory: LiveMemoryPool,
    cancellation: CancellationToken,
    zstd: Mutex<vortex_zstd::DecoderWorkspace>,
}

#[derive(Debug)]
struct OrcCredit {
    credit: MemoryLease,
    cancellation: CancellationToken,
}

impl orc_rust::reader::memory::DecoderMemory for OrcMemory {
    fn prepare_zstd(&self) -> std::io::Result<()> {
        self.cancellation.check().map_err(std::io::Error::other)?;
        self.zstd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prepare(&crate::owned_buffers::ReservedHostAllocator::new(
                self.memory.clone(),
            ))
            .map_err(std::io::Error::other)?;
        self.cancellation.check().map_err(std::io::Error::other)
    }

    fn decode_zstd(&self, input: &[u8], output: &mut [u8]) -> std::io::Result<usize> {
        self.cancellation.check().map_err(std::io::Error::other)?;
        let written = self
            .zstd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .decompress_frame_into(
                input,
                output,
                &crate::owned_buffers::ReservedHostAllocator::new(self.memory.clone()),
            )
            .map_err(std::io::Error::other)?;
        self.cancellation.check().map_err(std::io::Error::other)?;
        Ok(written)
    }

    fn reserve(
        &self,
        bytes: usize,
    ) -> std::io::Result<Box<dyn orc_rust::reader::memory::DecoderMemoryReservation>> {
        self.cancellation.check().map_err(std::io::Error::other)?;
        let credit = self
            .memory
            .reserve(u64::try_from(bytes).map_err(std::io::Error::other)?)
            .map_err(std::io::Error::other)?;
        Ok(Box::new(OrcCredit {
            credit,
            cancellation: self.cancellation.clone(),
        }))
    }
}

impl orc_rust::reader::memory::DecoderMemoryReservation for OrcCredit {
    fn resize(&mut self, bytes: usize) -> std::io::Result<()> {
        self.cancellation.check().map_err(std::io::Error::other)?;
        self.credit
            .resize(u64::try_from(bytes).map_err(std::io::Error::other)?)
            .map_err(std::io::Error::other)
    }
}
