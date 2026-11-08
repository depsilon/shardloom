//! Metadata admission does not enable nested-format execution or writing.

use super::*;
use vortex::array::dtype::FieldPath;
use vortex::array::scalar::Scalar;
use vortex::layout::layouts::file_stats::{AggregateStat, AggregateStats};

fn null_count(value: u64, dtype: &DType) -> AggregateStats {
    let aggregate = Stat::NullCount.aggregate_fn().unwrap();
    let partial = Scalar::from(value)
        .cast(&aggregate.state_dtype(dtype).unwrap())
        .unwrap();
    AggregateStats::new(vec![
        AggregateStat::try_from_partial(aggregate, partial, dtype).unwrap(),
    ])
}

fn nested_fixture(depth: usize) -> (FileStatistics, DType, Vec<DType>) {
    let mut dtype = field_dtype(0);
    let mut dtypes = vec![dtype.clone()];
    let mut aggregates = vec![null_count(1, &dtype)];
    for _ in 0..depth {
        dtype = DType::struct_(
            [
                ("node", dtype),
                ("ignored", DType::Variant(Nullability::NonNullable)),
                (
                    "empty",
                    DType::Struct(StructFields::empty(), Nullability::NonNullable),
                ),
            ],
            Nullability::Nullable,
        );
        aggregates.push(null_count(2, &dtype));
        dtypes.push(dtype.clone());
    }
    let legacy = if depth == 0 {
        vec![null_count(1, &dtype)]
    } else {
        vec![AggregateStats::default(); 3]
    };
    (
        FileStatistics::new_with_dtype(aggregates.into(), legacy.into(), &dtype),
        dtype,
        dtypes,
    )
}

#[test]
fn native_nested_statistics_admit_paths_and_release_only_after_last_alias() {
    for depth in [0, 1, 5, 6, 12] {
        let (original, dtype, dtypes) = nested_fixture(depth);
        let bytes = original.write_flatbuffer_bytes().unwrap();
        let grant = parsed_table_bytes(&original, &dtype);
        for limit in [grant - 1, grant] {
            let memory = LiveMemoryPool::new(limit).unwrap();
            let (consumer, requests) = session(&memory);
            let result = reopen(&bytes, &dtype, &consumer);
            assert_eq!(
                requests.lock().unwrap().as_slice(),
                &[usize::try_from(grant).unwrap()]
            );
            if limit < grant {
                assert!(crate::owned_buffers::is_owned_reservation_denial(
                    &result.unwrap_err()
                ));
                assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
                continue;
            }
            let parsed = result.unwrap();
            assert_eq!(parsed.dtypes(), dtypes);
            assert_eq!(parsed.aggregates().len(), depth + 1);
            assert_eq!(parsed.stats_sets().len(), if depth == 0 { 1 } else { 3 });
            let alias = parsed.clone();
            let paths_pointer = parsed.paths().as_ptr();
            let aggregates_pointer = parsed.aggregates().as_ptr();
            drop((parsed, consumer));
            assert_eq!(memory.snapshot().reserved_bytes, grant);
            assert_eq!(alias.paths().as_ptr(), paths_pointer);
            assert_eq!(alias.aggregates().as_ptr(), aggregates_pointer);
            for (index, dtype) in dtypes.iter().enumerate() {
                let path = (0..depth - index).fold(FieldPath::root(), |path, _| path.push("node"));
                let (stats, field) = alias.get_by_path(&path).unwrap();
                assert_eq!(field, dtype);
                assert!(std::ptr::eq(stats, &raw const alias.aggregates()[index]));
                assert_eq!(
                    stats.to_stats_set().get(Stat::NullCount),
                    Precision::Exact((if index == 0 { 1_u64 } else { 2 }).into())
                );
            }
            assert!(
                alias
                    .get_by_path(&FieldPath::from_name("ignored"))
                    .is_none()
            );
            assert_eq!(
                alias.write_flatbuffer_bytes().unwrap().as_slice(),
                bytes.as_slice()
            );
            drop(alias);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

fn raw_nested(entries: &[u16], specs: &[u16], partials: &[u8]) -> Vec<u8> {
    use vortex::file::flatbuffers::footer as fb;
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let id = builder.create_string("test.unknown");
    let spec = fb::AggregateSpec::create(
        &mut builder,
        &fb::AggregateSpecArgs {
            id: Some(id),
            ..Default::default()
        },
    );
    let aggregate_specs = builder.create_vector(&[spec]);
    let specs = builder.create_vector(specs);
    let set = fb::AggregateSet::create(&mut builder, &fb::AggregateSetArgs { specs: Some(specs) });
    let aggregate_sets = builder.create_vector(&[set]);
    let entry_sets = builder.create_vector(entries);
    let partials = builder.create_vector(partials);
    let root = fb::FileStatistics::create(
        &mut builder,
        &fb::FileStatisticsArgs {
            aggregate_specs: Some(aggregate_specs),
            aggregate_sets: Some(aggregate_sets),
            entry_sets: Some(entry_sets),
            partials: Some(partials),
            ..Default::default()
        },
    );
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

#[test]
fn native_nested_statistics_failures_refund_all_tables_and_preserve_retry() {
    use vortex::file::flatbuffers::footer as fb;
    let dtype = dtype(Some(2));
    for (entries, specs, partials, before_admission) in [
        (vec![0], vec![0], vec![0], true),
        (vec![0, 0], vec![1], vec![0, 0], false),
        // Aggregate-set bounds are checked while counting exact capacities,
        // before any table reservation or partial scalar parsing.
        (vec![0, 1], vec![0], vec![0, 0], true),
        (vec![0, 0], vec![0], vec![0, 0x80], false),
        (vec![0, 0], vec![0], vec![0, 0, 1], false),
    ] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let (consumer, requests) = session(&memory);
        let bytes = raw_nested(&entries, &specs, &partials);
        let root = flatbuffers::root::<fb::FileStatistics<'_>>(&bytes).unwrap();
        let error = FileStatistics::from_flatbuffer(&root, &dtype, &consumer).unwrap_err();
        assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(requests.lock().unwrap().is_empty(), before_admission);
        assert_eq!(memory.snapshot().peak_reserved_bytes == 0, before_admission);
        let valid = raw_nested(&[0, 0], &[0], &[0, 0]);
        let root = flatbuffers::root::<fb::FileStatistics<'_>>(&valid).unwrap();
        let parsed = FileStatistics::from_flatbuffer(&root, &dtype, &consumer).unwrap();
        assert_eq!(parsed.aggregates().len(), 2);
        assert!(parsed.aggregates().iter().all(AggregateStats::is_empty));
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(parsed);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
