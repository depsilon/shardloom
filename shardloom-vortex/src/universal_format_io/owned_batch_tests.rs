//! Ownership handoffs across actual source wrappers and their ordered queues.

use super::*;
use crate::universal_format_io::{
    FlatColumnarStreamSourcePlan, IngestColumnarStreamSource,
    flat_columnar_stream_source_from_reader, text_stream::credited_batch,
};
use arrow_array::{ArrayRef, NullArray, RecordBatchOptions, StringArray, cast::AsArray, types::*};
use arrow_schema::{DataType, Field, Schema};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};

const CREDIT: u64 = 8192;

#[derive(Clone, Copy)]
enum Shape {
    Null,
    Empty,
    Url,
}

impl Shape {
    fn schema(self) -> SchemaRef {
        Arc::new(match self {
            Self::Null => Schema::new(vec![Field::new("n", DataType::Null, true)]),
            Self::Empty => Schema::empty(),
            Self::Url => Schema::new(vec![Field::new("URL", DataType::Utf8, true)]),
        })
    }

    fn batch(self, schema: SchemaRef) -> RecordBatch {
        let columns: Vec<ArrayRef> = match self {
            Self::Null => vec![Arc::new(NullArray::new(5))],
            Self::Empty => vec![],
            Self::Url => vec![Arc::new(StringArray::from(vec![
                Some("https://example.test/a"),
                None,
                Some(""),
                Some("https://other.test/b"),
                Some("https://example.test/a"),
            ]))],
        };
        RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(5)),
        )
        .unwrap()
    }
}

struct Reader {
    shape: Shape,
    schema: SchemaRef,
    memory: LiveMemoryPool,
    index: usize,
    count: usize,
    fail_at: Option<usize>,
}

impl Iterator for Reader {
    type Item = BatchResult;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.count {
            return None;
        }
        if self.fail_at == Some(self.index) {
            self.index = self.count;
            return Some(Err(ArrowError::ParseError(
                "injected owned source failure".into(),
            )));
        }
        self.index += 1;
        let lease = match self.memory.reserve(CREDIT) {
            Ok(lease) => lease,
            Err(error) => return Some(Err(ArrowError::ExternalError(Box::new(error)))),
        };
        Some(Ok(OwnedBatch::with_owner(
            self.shape.batch(Arc::clone(&self.schema)),
            Arc::new(lease),
        )))
    }
}

impl OwnedBatchReader for Reader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn source(
    shape: Shape,
    memory: &LiveMemoryPool,
    fail_at: Option<usize>,
) -> IngestColumnarStreamSource {
    let schema = shape.schema();
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let metadata = flat_columnar_stream_source_from_reader(
        &schema,
        names.clone(),
        names.clone(),
        names,
        None,
        &FlatColumnarStreamSourcePlan::product_batches(5, None, "owned_test_batches", "unchanged"),
        Box::new(SchemaReader(Arc::clone(&schema))),
    );
    IngestColumnarStreamSource::from_owned(
        metadata,
        Box::new(Reader {
            shape,
            schema,
            memory: memory.clone(),
            index: 0,
            count: 12,
            fail_at,
        }),
        memory,
    )
}

#[test]
fn credited_bufferless_batches_keep_rows_and_credits_after_transformation() {
    for shape in [Shape::Null, Shape::Empty] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let lease = memory.reserve(CREDIT).unwrap();
        let batch = credited_batch(shape.batch(shape.schema()), lease).unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(memory.snapshot().reserved_bytes, CREDIT);
        let slice = batch
            .map(|batch| Ok::<_, ArrowError>(batch.slice(2, 2)))
            .unwrap();
        assert_eq!(slice.num_rows(), 2);
        assert_eq!(memory.snapshot().reserved_bytes, CREDIT);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn queued_bufferless_batch_outlives_its_reader_and_releases_independently() {
    for parallelism in [1, 2, 4] {
        for shape in [Shape::Null, Shape::Empty] {
            let memory = LiveMemoryPool::new(1 << 20).unwrap();
            let source = source(shape, &memory, None)
                .with_prefetch(parallelism, &CancellationToken::default())
                .unwrap();
            let (metadata, _, mut reader) = source.into_parts();
            let first = reader.next().unwrap().unwrap();
            assert_eq!(first.num_rows(), 5);
            drop(reader);
            drop(metadata);
            assert_eq!(memory.snapshot().reserved_bytes, CREDIT);
            drop(first);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn derived_columns_preserve_values_and_owners_on_both_sides_of_the_queue() {
    for parallelism in [1, 4] {
        for derive_before_queue in [false, true] {
            let memory = LiveMemoryPool::new(1 << 20).unwrap();
            let token = CancellationToken::default();
            let source = source(Shape::Url, &memory, None);
            let source = if derive_before_queue {
                source
                    .with_embedded_derived_columns()
                    .with_prefetch(parallelism, &token)
                    .unwrap()
            } else {
                source
                    .with_prefetch(parallelism, &token)
                    .unwrap()
                    .with_embedded_derived_columns()
            };
            let (metadata, _, mut reader) = source.into_parts();
            let first = reader.next().unwrap().unwrap();
            assert_eq!(first.num_columns(), 3);
            assert_eq!(
                first.column(0).as_string::<i32>().value(0),
                "https://example.test/a"
            );
            let lengths = first.column(1).as_dictionary::<Int32Type>();
            let domains = first.column(2).as_dictionary::<Int32Type>();
            assert_eq!(
                lengths
                    .values()
                    .as_primitive::<UInt32Type>()
                    .value(usize::try_from(lengths.keys().value(0)).unwrap()),
                22
            );
            assert_eq!(
                domains
                    .values()
                    .as_string::<i32>()
                    .value(usize::try_from(domains.keys().value(0)).unwrap()),
                "example.test"
            );
            assert!(first.column(0).is_null(1));
            let mut count = 1;
            for batch in reader.by_ref() {
                assert_eq!(batch.unwrap().num_rows(), 5);
                count += 1;
            }
            assert_eq!(count, 12);
            drop(reader);
            drop(metadata);
            assert!(
                memory.snapshot().reserved_bytes > CREDIT,
                "derived schema and payload credits must outlive the reader"
            );
            drop(first);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn cancellation_drains_queued_owners_but_preserves_a_delivered_batch() {
    for parallelism in [1, 4] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let token = CancellationToken::default();
        let source = source(Shape::Empty, &memory, None)
            .with_prefetch(parallelism, &token)
            .unwrap();
        let (metadata, _, mut reader) = source.into_parts();
        let first = reader.next().unwrap().unwrap();
        token.cancel();
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
        drop(reader);
        drop(metadata);
        assert_eq!(memory.snapshot().reserved_bytes, CREDIT);
        drop(first);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn provider_error_drains_queued_owners_without_cancelling_the_parent() {
    for parallelism in [1, 4] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let token = CancellationToken::default();
        let source = source(Shape::Null, &memory, Some(1))
            .with_prefetch(parallelism, &token)
            .unwrap();
        let (metadata, _, mut reader) = source.into_parts();
        let first = reader.next().unwrap().unwrap();
        assert!(
            reader
                .next()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("injected owned source failure")
        );
        assert!(reader.next().is_none());
        assert!(!token.is_cancelled());
        drop(reader);
        drop(metadata);
        assert_eq!(memory.snapshot().reserved_bytes, CREDIT);
        drop(first);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
