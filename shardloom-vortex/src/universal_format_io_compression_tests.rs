// SPDX-License-Identifier: Apache-2.0
//! Exercise the reviewed compression dependency through the actual source adapters.

use super::*;
use parquet::basic::ZstdLevel;
use std::path::PathBuf;

type SourceReader = fn(&Path, usize) -> Result<FlatLocalSourceTable>;

struct Fixture(PathBuf);

impl Fixture {
    fn new(format: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-compression-{}-{}.{format}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        File::options()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn input(rows: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..rows).map(|row| (row % 5 != 0).then_some(row - 256)),
            )),
            Arc::new(StringArray::from_iter((0..rows).map(|row| {
                (row % 11 != 0).then(|| format!("compressed-λ-{}-{}", row % 7, "x".repeat(80)))
            }))),
        ],
    )
    .unwrap()
}

fn assert_rows(table: &FlatLocalSourceTable, rows: i64) {
    assert_eq!(table.header, ["id", "label"]);
    assert_eq!(
        table.column_arrow_dtypes,
        [Some(DataType::Int64), Some(DataType::Utf8)]
    );
    let expected = (0..rows)
        .map(|row| {
            BTreeMap::from([
                (
                    "id".to_string(),
                    if row % 5 == 0 {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Int64(row - 256)
                    },
                ),
                (
                    "label".to_string(),
                    if row % 11 == 0 {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Utf8(format!("compressed-λ-{}-{}", row % 7, "x".repeat(80)))
                    },
                ),
            ])
        })
        .collect::<Vec<_>>();
    assert_eq!(table.rows, expected);
}

fn check_source(format: &str, write: impl Fn(&Path, &RecordBatch), read: SourceReader) {
    for rows in [0, 513] {
        let fixture = Fixture::new(format);
        write(&fixture.0, &input(rows));
        let table = read(&fixture.0, usize::MAX).unwrap();
        assert_rows(&table, rows);
        if rows == 0 {
            continue;
        }

        let original = std::fs::read(&fixture.0).unwrap();
        let frames = original
            .windows(4)
            .enumerate()
            .filter_map(|(offset, bytes)| (bytes == [0x28, 0xb5, 0x2f, 0xfd]).then_some(offset))
            .collect::<Vec<_>>();
        assert!(
            !frames.is_empty(),
            "{format} must actually contain Zstd frames"
        );
        let mut corrupt = original.clone();
        for offset in frames {
            corrupt[offset..offset + 4].fill(0);
        }
        std::fs::write(&fixture.0, corrupt).unwrap();
        assert!(
            read(&fixture.0, usize::MAX).is_err(),
            "{format} accepted corrupted frames"
        );
        std::fs::write(&fixture.0, original).unwrap();
        assert_eq!(read(&fixture.0, usize::MAX).unwrap(), table);
    }
}

#[test]
fn compressed_ipc_preserves_values_rejects_corruption_and_reopens() {
    check_source(
        "arrow",
        |path, batch| {
            let options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))
                .unwrap();
            let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
                File::create(path).unwrap(),
                batch.schema().as_ref(),
                options,
            )
            .unwrap();
            writer.write(batch).unwrap();
            writer.finish().unwrap();
        },
        read_flat_arrow_ipc_source,
    );
}

#[test]
fn compressed_parquet_preserves_values_rejects_corruption_and_reopens() {
    check_source(
        "parquet",
        |path, batch| {
            let properties = parquet::file::properties::WriterProperties::builder()
                .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
                .set_dictionary_enabled(false)
                .set_max_row_group_row_count(Some(64))
                .build();
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                File::create(path).unwrap(),
                batch.schema(),
                Some(properties),
            )
            .unwrap();
            writer.write(batch).unwrap();
            writer.close().unwrap();
        },
        read_flat_parquet_source,
    );
}

#[test]
fn compressed_orc_preserves_values_rejects_corruption_and_reopens() {
    check_source(
        "orc",
        |path, batch| {
            let mut writer =
                orc_rust::ArrowWriterBuilder::new(File::create(path).unwrap(), batch.schema())
                    .with_compression(orc_rust::compression::CompressionType::Zstd)
                    .with_compression_block_size(256)
                    .try_build()
                    .unwrap();
            writer.write(batch).unwrap();
            writer.close().unwrap();
        },
        read_flat_orc_source,
    );
}

#[test]
fn orc_decoder_reports_first_and_later_compressed_block_errors() {
    use std::io::Read;

    let fixture = Fixture::new("orc");
    let writer =
        orc_rust::ArrowWriterBuilder::new(File::create(&fixture.0).unwrap(), input(0).schema())
            .with_compression(orc_rust::compression::CompressionType::Zstd)
            .try_build()
            .unwrap();
    writer.close().unwrap();
    let builder = orc_rust::ArrowReaderBuilder::try_new(File::open(&fixture.0).unwrap()).unwrap();
    let compression = builder.file_metadata().compression();
    assert!(compression.is_some());
    for first_valid in [false, true] {
        // ORC headers store (payload length << 1) | original in three bytes.
        let mut blocks = if first_valid {
            vec![3, 0, 0, b'x']
        } else {
            vec![]
        };
        blocks.extend_from_slice(&[14, 0, 0]);
        blocks.extend_from_slice(b"invalid");
        let mut decoder = orc_rust::compression::Decompressor::new(
            bytes::Bytes::from(blocks),
            compression,
            Vec::new(),
        );
        let mut byte = [0];
        if first_valid {
            decoder.read_exact(&mut byte).unwrap();
            assert_eq!(byte, [b'x']);
        }
        assert!(decoder.read(&mut byte).is_err());
    }
}
