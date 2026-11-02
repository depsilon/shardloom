//! Test-only ORC tails encoded independently from the provider's metadata parser.
//! Tags follow the Apache ORC v1 protobuf contract; payload streams use its writer.

use super::*;
use arrow_array::Array;

pub(super) struct Fixture {
    pub(super) directory: ParquetFixture,
    pub(super) path: PathBuf,
    pub(super) batch: RecordBatch,
    pub(super) metadata: Arc<orc_rust::reader::metadata::FileMetadata>,
    body: Vec<u8>,
}

impl Fixture {
    pub(super) fn new(rows: usize, statistics: bool) -> Self {
        Self::from_batch(ParquetFixture::batch().slice(0, rows), statistics)
    }

    pub(super) fn from_batch(batch: RecordBatch, statistics: bool) -> Self {
        let directory = ParquetFixture::new();
        let path = directory.0.join("input.orc");
        let rows = batch.num_rows();
        let mut bytes = Vec::new();
        let mut writer = orc_rust::ArrowWriterBuilder::new(&mut bytes, batch.schema())
            .try_build()
            .unwrap();
        for start in (0..rows).step_by(4) {
            writer
                .write(&batch.slice(start, (rows - start).min(4)))
                .unwrap();
            writer.flush_stripe().unwrap();
        }
        writer.close().unwrap();
        let reader =
            orc_rust::ArrowReaderBuilder::try_new(bytes::Bytes::from(bytes.clone())).unwrap();
        let metadata = reader.shared_file_metadata();
        let end = metadata
            .stripe_metadatas()
            .last()
            .map_or(3, |stripe| stripe.footer_offset() + stripe.footer_length());
        bytes.truncate(usize::try_from(end).unwrap());
        let fixture = Self {
            directory,
            path,
            batch,
            metadata,
            body: bytes,
        };
        fixture.tail(Some(rows as u64), statistics);
        fixture
    }

    pub(super) fn tail(&self, total: Option<u64>, statistics: bool) {
        let mut footer = Vec::new();
        number(&mut footer, 1, 3);
        number(&mut footer, 2, self.body.len() as u64);
        let mut metadata = Vec::new();
        let mut start = 0;
        for stripe in self.metadata.stripe_metadatas() {
            let mut info = Vec::new();
            for (tag, value) in [
                (1, stripe.offset()),
                (2, stripe.index_length()),
                (3, stripe.data_length()),
                (4, stripe.footer_length()),
                (5, stripe.number_of_rows()),
            ] {
                number(&mut info, tag, value);
            }
            message(&mut footer, 3, &info);
            if statistics {
                let rows = usize::try_from(stripe.number_of_rows()).unwrap();
                message(
                    &mut metadata,
                    1,
                    &stripe_statistics(&self.batch.slice(start, rows)),
                );
                start += rows;
            }
        }
        let mut root = Vec::new();
        number(&mut root, 1, 12); // STRUCT
        for (index, field) in self.batch.schema().fields().iter().enumerate() {
            number(&mut root, 2, index as u64 + 1);
            message(&mut root, 3, field.name().as_bytes());
        }
        message(&mut footer, 4, &root);
        for field in self.batch.schema().fields() {
            let mut dtype = Vec::new();
            number(
                &mut dtype,
                1,
                match field.data_type() {
                    DataType::Int64 => 4,
                    DataType::Utf8 => 7,
                    _ => unreachable!(),
                },
            );
            message(&mut footer, 4, &dtype);
        }
        if let Some(total) = total {
            number(&mut footer, 6, total);
        }
        let mut postscript = Vec::new();
        number(&mut postscript, 1, footer.len() as u64);
        number(&mut postscript, 2, 0); // NONE
        number(&mut postscript, 4, 0);
        number(&mut postscript, 4, 12);
        number(&mut postscript, 5, metadata.len() as u64);
        message(&mut postscript, 8000, b"ORC");
        let mut bytes = self.body.clone();
        bytes.extend(metadata);
        bytes.extend(footer);
        bytes.extend_from_slice(&postscript);
        bytes.push(u8::try_from(postscript.len()).unwrap());
        fs::write(&self.path, bytes).unwrap();
    }

    pub(super) fn corrupt(&self, index: usize) {
        let stripe = &self.metadata.stripe_metadatas()[index];
        let mut file = fs::OpenOptions::new().write(true).open(&self.path).unwrap();
        file.seek(SeekFrom::Start(stripe.footer_offset())).unwrap();
        file.write_all(&vec![
            0xff;
            usize::try_from(stripe.footer_length()).unwrap()
        ])
        .unwrap();
        file.sync_all().unwrap();
    }
}

fn stripe_statistics(batch: &RecordBatch) -> Vec<u8> {
    let mut result = Vec::new();
    let mut root = Vec::new();
    number(&mut root, 1, batch.num_rows() as u64);
    number(&mut root, 10, 0);
    message(&mut result, 1, &root);
    for column in batch.columns() {
        let mut stats = Vec::new();
        number(&mut stats, 1, (column.len() - column.null_count()) as u64);
        number(&mut stats, 10, u64::from(column.null_count() > 0));
        if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            let mut bounds = Vec::new();
            let min = values.iter().flatten().min().unwrap();
            let max = values.iter().flatten().max().unwrap();
            // Fixture integers are nonnegative; sint64 uses zig-zag encoding.
            number(&mut bounds, 1, u64::try_from(min).unwrap() * 2);
            number(&mut bounds, 2, u64::try_from(max).unwrap() * 2);
            message(&mut stats, 2, &bounds);
        } else if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            let mut bounds = Vec::new();
            message(
                &mut bounds,
                1,
                values.iter().flatten().min().unwrap().as_bytes(),
            );
            message(
                &mut bounds,
                2,
                values.iter().flatten().max().unwrap().as_bytes(),
            );
            message(&mut stats, 4, &bounds);
        }
        message(&mut result, 1, &stats);
    }
    result
}

fn varint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        bytes.push(u8::try_from(value & 127).unwrap() | 128);
        value >>= 7;
    }
    bytes.push(u8::try_from(value).unwrap());
}

fn number(bytes: &mut Vec<u8>, tag: u64, value: u64) {
    varint(bytes, tag << 3);
    varint(bytes, value);
}

fn message(bytes: &mut Vec<u8>, tag: u64, value: &[u8]) {
    varint(bytes, (tag << 3) | 2);
    varint(bytes, value.len() as u64);
    bytes.extend_from_slice(value);
}
