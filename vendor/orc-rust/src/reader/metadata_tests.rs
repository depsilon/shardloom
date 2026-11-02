// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{ArrowReaderBuilder, ArrowWriterBuilder};
use arrow::{array::Int64Array, record_batch::RecordBatch};
use std::io::Cursor;
use std::sync::Mutex;

#[test]
fn controlled_metadata_requires_the_owned_zstd_workspace_provider() {
    use crate::compression::{CompressionType, Compressor};
    use crate::reader::memory::tests::{Counters, Memory};

    let mut data = footer(Some(0), &[]);
    data.metadata.push(proto::UserMetadataItem {
        name: Some("test".to_string()),
        value: Some(vec![0; 4096]),
    });
    let mut compressor = Compressor::new(Some(CompressionType::Zstd), Some(32_768)).unwrap();
    let encoded = compressor
        .compress(&data.encode_to_vec())
        .unwrap()
        .into_owned();
    let postscript = PostScript {
        footer_length: Some(encoded.len() as u64),
        metadata_length: Some(0),
        compression: Some(proto::CompressionKind::Zstd.into()),
        compression_block_size: Some(32_768),
        magic: Some("ORC".to_string()),
        ..Default::default()
    }
    .encode_to_vec();
    let mut bytes = b"ORC".to_vec();
    bytes.extend_from_slice(&encoded);
    bytes.extend_from_slice(&postscript);
    bytes.push(postscript.len() as u8);
    let mut bytes = Bytes::from(bytes);
    assert_eq!(
        read_metadata(&mut bytes.clone()).unwrap().number_of_rows(),
        0
    );
    let counters = Arc::new(Mutex::new(Counters::default()));
    let memory = Arc::new(Memory {
        counters: Arc::clone(&counters),
        limit: 1_000_000,
    });
    let error = read_metadata_with_memory(&mut bytes, &mut |_| Ok(()), memory).unwrap_err();
    assert!(error
        .to_string()
        .contains("requires a caller-owned decoder workspace"));
    assert!(counters.lock().unwrap().peak > 0);
    assert_eq!(counters.lock().unwrap().live, 0);
}

fn footer(total: Option<u64>, rows: &[u64]) -> Footer {
    Footer {
        number_of_rows: total,
        types: vec![proto::Type {
            kind: Some(proto::r#type::Kind::Struct.into()),
            ..Default::default()
        }],
        stripes: rows
            .iter()
            .enumerate()
            .map(|(i, rows)| proto::StripeInformation {
                offset: Some(3 + i as u64 * 3),
                index_length: Some(0),
                data_length: Some(2),
                footer_length: Some(1),
                number_of_rows: Some(*rows),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

#[test]
fn exact_counts_require_present_consistent_checked_totals() {
    for (total, rows) in [
        (0, vec![]),
        (17, vec![3, 0, 14]),
        ((1 << 53) + 1, vec![1 << 53, 1]),
        (u64::MAX, vec![u64::MAX]),
    ] {
        let metadata = FileMetadata::from_proto(
            &PostScript::default(),
            &footer(Some(total), &rows),
            &Metadata::default(),
        )
        .unwrap();
        assert_eq!(metadata.exact_number_of_rows().unwrap(), Some(total));
    }
    let missing = FileMetadata::from_proto(
        &PostScript::default(),
        &footer(None, &[7]),
        &Metadata::default(),
    )
    .unwrap();
    assert_eq!(missing.exact_number_of_rows().unwrap(), None);
    for (total, rows) in [(0, vec![1]), (5, vec![2, 2]), (u64::MAX, vec![u64::MAX, 1])] {
        assert!(FileMetadata::from_proto(
            &PostScript::default(),
            &footer(Some(total), &rows),
            &Metadata::default()
        )
        .is_err());
    }
    let mut missing = footer(Some(7), &[7]);
    missing.stripes[0].number_of_rows = None;
    assert!(
        FileMetadata::from_proto(&PostScript::default(), &missing, &Metadata::default()).is_err()
    );
}

#[test]
fn metadata_rejects_missing_overflowing_and_overlapping_extents() {
    let mut input = footer(Some(2), &[1, 1]);
    for offset in [0, 3, u64::MAX] {
        input.stripes[1].offset = Some(offset);
        let metadata =
            FileMetadata::from_proto(&PostScript::default(), &input, &Metadata::default()).unwrap();
        assert!(metadata.validate_stripe_extents(9).is_err());
    }
    input.stripes[1].offset = Some(6);
    let metadata =
        FileMetadata::from_proto(&PostScript::default(), &input, &Metadata::default()).unwrap();
    assert!(metadata.validate_stripe_extents(8).is_err());
    metadata.validate_stripe_extents(9).unwrap();
    input.stripes[1].index_length = None;
    assert!(
        FileMetadata::from_proto(&PostScript::default(), &input, &Metadata::default()).is_err()
    );
    for lengths in [(0, 0, 0, 0), (20, 2, u64::MAX, 1), (20, 2, 10, 10)] {
        assert!(metadata_start(lengths.0, lengths.1, lengths.2, lengths.3).is_err());
    }
}

#[derive(Clone)]
struct Observed {
    bytes: Bytes,
    reads: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl ChunkReader for Observed {
    type T = Cursor<Bytes>;
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn get_read(&self, offset: u64) -> std::io::Result<Self::T> {
        Ok(Cursor::new(self.bytes.slice(offset as usize..)))
    }
    fn get_bytes(&self, offset: u64, length: u64) -> std::io::Result<Bytes> {
        self.reads.lock().unwrap().push((offset, length));
        Ok(self
            .bytes
            .slice(offset as usize..(offset + length) as usize))
    }
}

fn input() -> Observed {
    let values: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
    let batch = RecordBatch::try_from_iter([("id", values)]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, batch.schema())
        .try_build()
        .unwrap();
    writer.write(&batch.slice(0, 2)).unwrap();
    writer.flush_stripe().unwrap();
    writer.write(&batch.slice(2, 2)).unwrap();
    writer.close().unwrap();
    Observed {
        bytes: bytes.into(),
        reads: Arc::default(),
    }
}

fn selection_input() -> Bytes {
    let values: arrow::array::ArrayRef = Arc::new(Int64Array::from_iter_values(0..20));
    let batch = RecordBatch::try_from_iter([("id", values)]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, batch.schema())
        .try_build()
        .unwrap();
    writer.write(&batch.slice(0, 10)).unwrap();
    writer.flush_stripe().unwrap();
    writer.write(&batch.slice(10, 10)).unwrap();
    writer.close().unwrap();
    Bytes::from(bytes)
}

#[test]
fn selected_regions_spanning_batches_preserve_exact_rows_across_stripes() {
    use crate::row_selection::RowSelector;
    let bytes = selection_input();
    let expected = (3..8).chain(12..18).collect::<Vec<i64>>();
    for batch_size in [1, 2, 4, 7, 8192] {
        let selection = vec![
            RowSelector::skip(3),
            RowSelector::select(5),
            RowSelector::skip(4),
            RowSelector::select(6),
            RowSelector::skip(2),
        ]
        .into();
        let mut reader = ArrowReaderBuilder::try_new(bytes.clone())
            .unwrap()
            .with_batch_size(batch_size)
            .with_row_selection(selection)
            .build();
        let mut actual = Vec::new();
        for batch in reader.by_ref() {
            let batch = batch.unwrap();
            assert!(batch.num_rows() > 0 && batch.num_rows() <= batch_size);
            actual.extend_from_slice(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
            );
        }
        assert_eq!(actual, expected, "batch_size={batch_size}");
        assert!(reader.next().is_none());
    }
}

#[test]
fn changing_batch_limits_preserves_selection_state_and_retained_values() {
    use crate::row_selection::RowSelector;
    let bytes = selection_input();
    for selected in [false, true] {
        let mut builder = ArrowReaderBuilder::try_new(bytes.clone()).unwrap();
        let expected = if selected {
            builder = builder.with_row_selection(
                vec![
                    RowSelector::skip(3),
                    RowSelector::select(5),
                    RowSelector::skip(4),
                    RowSelector::select(6),
                    RowSelector::skip(2),
                ]
                .into(),
            );
            (3..8).chain(12..18).collect::<Vec<i64>>()
        } else {
            (0..20).collect()
        };
        let mut reader = builder.build();
        assert!(reader.set_batch_size(0).is_err());
        let mut batches = Vec::new();
        let limits = [1, 5, 2, 3, 8];
        loop {
            let limit = limits[batches.len() % limits.len()];
            reader.set_batch_size(limit).unwrap();
            let Some(batch) = reader.next() else {
                break;
            };
            let batch = batch.unwrap();
            assert!(batch.num_rows() > 0 && batch.num_rows() <= limit);
            batches.push(batch);
        }
        reader.set_batch_size(usize::MAX).unwrap();
        assert!(reader.next().is_none());
        drop(reader);
        let mut actual = Vec::new();
        for batch in batches {
            actual.extend_from_slice(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
            );
        }
        assert_eq!(actual, expected);
    }
    let mut empty = ArrowReaderBuilder::try_new(bytes)
        .unwrap()
        .with_row_selection(vec![RowSelector::skip(20)].into())
        .build();
    empty.set_batch_size(1).unwrap();
    assert!(empty.next().is_none());
}

#[test]
fn shared_metadata_and_range_avoid_footer_reread_and_excluded_stripe_reads() {
    let observed = input();
    let builder = ArrowReaderBuilder::try_new(observed.clone()).unwrap();
    let metadata = builder.shared_file_metadata();
    assert!(std::ptr::eq(builder.file_metadata(), metadata.as_ref()));
    let stripe = &metadata.stripe_metadatas()[1];
    let start = stripe.offset();
    let end = stripe.footer_offset() + stripe.footer_length();
    observed.reads.lock().unwrap().clear();
    for _ in 0..2 {
        let batches =
            ArrowReaderBuilder::new_with_metadata(observed.clone(), Arc::clone(&metadata))
                .with_file_byte_range(start as usize..start as usize + 1)
                .build()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[3, 4]);
    }
    let reads = observed.reads.lock().unwrap();
    assert!(!reads.is_empty());
    assert!(reads
        .iter()
        .all(|(offset, length)| *offset >= start && offset + length <= end));
}

#[test]
fn metadata_workspace_admission_is_incremental_and_final_credit_is_retained_shape() {
    let mut observed = input();
    let mut credits = Vec::new();
    let metadata = read_metadata_with_reservation(&mut observed, &mut |bytes| {
        credits.push(bytes);
        Ok(())
    })
    .unwrap();
    assert_eq!(metadata.exact_number_of_rows().unwrap(), Some(4));
    assert!(credits.len() >= 4);
    let final_credit = *credits.last().unwrap();
    assert!(final_credit >= 8192);
    assert!(credits.iter().max().unwrap() > &final_credit);
    let mut calls = 0;
    let failed = read_metadata_with_reservation(&mut observed, &mut |_| {
        calls += 1;
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "test metadata grant denied",
        ))
    });
    assert!(failed.unwrap_err().to_string().contains("grant denied"));
    assert_eq!(calls, 1);
}

#[test]
fn stripe_ranges_preserve_physical_indices_and_inclusive_exclusive_boundaries() {
    let metadata = Arc::new(
        FileMetadata::from_proto(
            &PostScript::default(),
            &footer(Some(4096), &vec![1; 4096]),
            &Metadata::default(),
        )
        .unwrap(),
    );
    metadata.validate_stripe_extents(3 + 4096 * 3).unwrap();
    for (range, expected) in [
        (None, (0..4096).collect::<Vec<_>>()),
        (Some(0..3), vec![]),
        (Some(3..4), vec![0]),
        (Some(6004..6012), vec![2001, 2002]),
        (Some(12_288..usize::MAX), vec![4095]),
        (Some(usize::MAX..usize::MAX), vec![]),
        (Some(9..9), vec![]),
    ] {
        let mut cursor = crate::arrow_reader::Cursor {
            reader: (),
            file_metadata: Arc::clone(&metadata),
            projected_data_type: metadata.root_data_type().clone(),
            stripe_index: 0,
            file_byte_range: range,
            decoder_memory: None,
        };
        let indices = std::iter::from_fn(|| cursor.next_stripe_index()).collect::<Vec<_>>();
        assert_eq!(indices, expected);
        assert_eq!(cursor.next_stripe_index(), None);
    }
}

#[cfg(feature = "async")]
impl crate::reader::AsyncChunkReader for Observed {
    fn len(&mut self) -> futures::future::BoxFuture<'_, std::io::Result<u64>> {
        Box::pin(async { Ok(self.bytes.len() as u64) })
    }

    fn get_bytes(
        &mut self,
        offset: u64,
        length: u64,
    ) -> futures::future::BoxFuture<'_, std::io::Result<Bytes>> {
        Box::pin(async move { ChunkReader::get_bytes(self, offset, length) })
    }
}

#[cfg(feature = "async")]
#[tokio::test]
async fn asynchronous_shared_metadata_range_skips_excluded_streams_and_ends() {
    use futures::TryStreamExt;

    let observed = input();
    let builder = ArrowReaderBuilder::try_new_async(observed.clone())
        .await
        .unwrap();
    let metadata = builder.shared_file_metadata();
    let stripe = &metadata.stripe_metadatas()[1];
    let start = stripe.offset();
    let end = stripe.footer_offset() + stripe.footer_length();
    for (range, expected) in [
        (start as usize..start as usize + 1, vec![3, 4]),
        (0..3, vec![]),
        (end as usize..usize::MAX, vec![]),
    ] {
        observed.reads.lock().unwrap().clear();
        let batches: Vec<RecordBatch> =
            ArrowReaderBuilder::new_with_metadata(observed.clone(), Arc::clone(&metadata))
                .with_file_byte_range(range)
                .build_async()
                .try_collect()
                .await
                .unwrap();
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, expected);
        let reads = observed.reads.lock().unwrap();
        assert_eq!(reads.is_empty(), expected.is_empty());
        assert!(reads
            .iter()
            .all(|(offset, length)| *offset >= start && offset + length <= end));
    }
}
