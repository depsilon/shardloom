use super::*;
use crate::owned_buffers::ReservedHostAllocator;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{VarBinViewArray, varbinview::BinaryView},
        dtype::{DType, Nullability},
        memory::HostAllocator as _,
        validity::Validity,
    },
    buffer::{Alignment, Buffer, ByteBuffer},
};

#[test]
fn borrowed_native_strings_match_provider_for_sliced_inline_and_multiple_buffers() {
    let first = "shared-prefix-東京-first";
    let second = "shared-prefix-東京-second";
    let expected = ["outside", "", "123456789012", first, second, first, "tail"];
    let buffers = vec![
        ByteBuffer::from(format!("pad{first}").into_bytes()),
        ByteBuffer::from(format!("padding{second}").into_bytes()),
    ];
    let views = Buffer::from(vec![
        BinaryView::new_inlined(expected[0].as_bytes()),
        BinaryView::new_inlined(expected[1].as_bytes()),
        BinaryView::new_inlined(expected[2].as_bytes()),
        BinaryView::make_view(first.as_bytes(), 0, 3),
        BinaryView::make_view(second.as_bytes(), 1, 7),
        BinaryView::make_view(first.as_bytes(), 0, 3),
        BinaryView::new_inlined(expected[6].as_bytes()),
    ]);
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let source = VarBinViewArray::try_new(
        views,
        buffers.into(),
        DType::Utf8(Nullability::NonNullable),
        Validity::NonNullable,
        &mut ctx,
    )
    .unwrap()
    .into_array();
    for range in [0..expected.len(), 1..6, 2..2] {
        let input = source.slice(range.clone()).unwrap();
        let values = input.clone().execute::<VarBinViewArray>(&mut ctx).unwrap();
        let provider_buffers = values
            .data_buffers()
            .iter()
            .map(|buffer| buffer.as_host().as_slice())
            .collect::<Vec<_>>();
        let mut dictionary = utf8_chunk_dictionary::Utf8ChunkDictionary::default();
        let mut expected_counts = std::collections::BTreeMap::new();
        for (row, value) in expected[range].iter().enumerate() {
            let bytes = native_utf8::borrowed_bytes(&values, row);
            assert_eq!(bytes, value.as_bytes());
            assert_eq!(bytes, values.views()[row].bytes(&provider_buffers));
            let owned = values.bytes_at(row);
            assert_eq!(bytes, owned.as_slice());
            // The provider uses a dangling sentinel for an empty owned buffer.
            if !bytes.is_empty() {
                assert_eq!(bytes.as_ptr(), owned.as_ptr());
            }
            dictionary.intern_source("text", &values, row).unwrap();
            *expected_counts.entry((*value).to_owned()).or_insert(0_u64) += 1;
        }
        let (dictionary_values, copied, retained) = dictionary.into_values();
        assert_eq!(copied, 0);
        assert_eq!(dictionary_values.len(), expected_counts.len());
        assert_eq!(
            retained,
            expected_counts
                .keys()
                .map(|value| value.len() as u64)
                .sum::<u64>()
        );
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut lease = memory
            .reserve(string_count_partial::partial_bytes(&input).unwrap())
            .unwrap();
        let partial = string_count_partial::count_string_chunk(
            &input,
            vortex::array::legacy_session().create_execution_ctx(),
            &aggregate_chunk_jobs::ChunkWorkerContext::Inline(
                shardloom_exec::compute_pool::CancellationToken::default(),
            ),
            &mut lease,
        )
        .unwrap();
        drop(provider_buffers);
        drop(values);
        drop(input);
        drop(dictionary_values);
        let mut actual = std::collections::BTreeMap::new();
        partial
            .for_each_count(|value, count| {
                assert!(actual.insert(value.to_owned(), count).is_none());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected_counts);
        for index in 0..actual.len() {
            let (bytes, _, count) = partial.entry(index).unwrap();
            assert_eq!(actual[std::str::from_utf8(bytes).unwrap()], count);
        }
        drop(partial);
        drop(lease);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn source_dictionary_credits_release_after_accessor_not_persistent_keys() {
    let expected = [
        "short",
        "https://example.test/z",
        "https://example.test/a",
        "short",
    ];
    let template = VarBinViewArray::from_iter_str(expected);
    let memory = LiveMemoryPool::new(4096).unwrap();
    let allocator = ReservedHostAllocator::new(memory.clone());
    let copy = |bytes: &[u8], alignment| {
        let mut owned = allocator.allocate(bytes.len(), alignment).unwrap();
        owned.as_mut_slice().copy_from_slice(bytes);
        owned.freeze()
    };
    let views = copy(
        template.views_handle().as_host(),
        Alignment::of::<BinaryView>(),
    );
    let buffers = template
        .data_buffers()
        .iter()
        .map(|buffer| copy(buffer.as_host(), Alignment::of::<u8>()))
        .collect::<Vec<_>>();
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let source = VarBinViewArray::try_new(
        Buffer::from_byte_buffer(views),
        buffers.into(),
        DType::Utf8(Nullability::NonNullable),
        Validity::NonNullable,
        &mut ctx,
    )
    .unwrap();
    drop(template);
    let retained = memory.snapshot().reserved_bytes;
    assert!(retained > 0);
    let input = source.clone().into_array();
    let mut work = native_numeric_accessor::Utf8AccessorWork::default();
    let accessor =
        aggregate_direct_utf8_chunk_dictionary_accessor_profiled("renamed", &input, &mut work)
            .unwrap()
            .unwrap();
    assert_eq!(work.copied_bytes, 0);
    assert_eq!(
        work.source_backed_bytes,
        expected[..3].iter().map(|s| s.len() as u64).sum::<u64>()
    );
    let AggregateDirectColumnAccessor::Utf8Dictionary {
        values, row_ids, ..
    } = &accessor
    else {
        panic!("dictionary")
    };
    assert_eq!(row_ids, &[0, 1, 2, 0]);
    for (row, value) in values.iter().enumerate() {
        assert_eq!(value.as_bytes().as_ptr(), source.bytes_at(row).as_ptr());
    }
    let mut interner = AggregateStringInterner::default();
    let id = interner.intern_dictionary(&values[1]).unwrap();
    assert_eq!(interner.intern_dictionary(&values[1]).unwrap(), id);
    let key = interner.value_arc(id).unwrap();
    assert_ne!(key.as_bytes().as_ptr(), values[1].as_bytes().as_ptr());
    let mut distinct = AggregateDistinctSet::default();
    aggregate_direct_count_distinct_insert_utf8_dictionary_values(
        vec![true; 3],
        values,
        &mut distinct,
    )
    .unwrap();
    let escaped = aggregate_direct_distinct_value(&accessor, 2).unwrap();
    drop(source);
    drop(input);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    let full_budget_payload = 4096 - *Alignment::DEFAULT_ALIGNMENT;
    assert!(
        allocator
            .allocate(full_budget_payload, Alignment::of::<u8>())
            .is_err()
    );
    assert_eq!(
        aggregate_direct_stat_value(&accessor, 3).unwrap(),
        StatValue::Utf8("short".into())
    );
    drop(accessor);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(interner.value(id).unwrap(), expected[1]);
    assert_eq!(distinct.len(), 3);
    assert_eq!(
        escaped,
        AggregateDistinctValue::Utf8(std::sync::Arc::from(expected[2]))
    );
    let capacity = allocator
        .allocate(full_budget_payload, Alignment::of::<u8>())
        .unwrap();
    drop(capacity);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
