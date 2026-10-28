//! The optional cache cannot invent equality or retain uncredited state.

use super::*;
use vortex::{
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::varbinview::BinaryView,
        arrays::{BoolArray, DictArray, PrimitiveArray, StructArray, VarBinViewArray},
        dtype::{DType, FieldNames, Nullability},
        validity::Validity,
    },
    buffer::Buffer,
};

fn keys(arrays: &[ArrayRef], memory: &LiveMemoryPool) -> Vec<KeyColumn> {
    let mut execution = vortex::array::array_session().create_execution_ctx();
    arrays
        .iter()
        .map(|array| {
            KeyColumn::values(array, &mut execution, memory, &CancellationToken::default()).unwrap()
        })
        .collect()
}

#[test]
fn inline_cache_sample_is_not_proof_and_bucket_collision_is_only_a_miss() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let token = CancellationToken::default();
    let mut seen = vec![None; SLOTS];
    let (left, right) = (0..=SLOTS)
        .find_map(|value| {
            let text = format!("k{value}");
            let slot = bucket([BinaryView::new_inlined(text.as_bytes()).as_u128(), 0]);
            seen[slot].replace(text.clone()).map(|prior| (prior, text))
        })
        .expect("more different keys than slots must collide");
    let source = VarBinViewArray::from_iter_str((0..1024).map(|row| {
        if row == 512 {
            right.as_str()
        } else {
            left.as_str()
        }
    }))
    .into_array();
    let keys = keys(&[source], &memory);
    let prior = memory.snapshot().reserved_bytes;
    let mut cache = Cache::new(&keys, &memory, &token).unwrap().unwrap();
    assert_eq!(
        memory.snapshot().reserved_bytes - prior,
        bytes::<Entry>(SLOTS).unwrap()
    );
    let first = cache.probe(0).unwrap().unwrap();
    assert_eq!(
        first.group(),
        None,
        "sampling has not proved a RowIndex group"
    );
    cache.insert(first, 17);
    assert_eq!(cache.probe(511).unwrap().unwrap().group(), Some(17));
    let collision = cache.probe(512).unwrap().unwrap();
    assert_eq!(first.slot, collision.slot);
    assert_ne!(first.exact, collision.exact);
    assert_eq!(collision.group(), None);
    cache.insert(collision, 29);
    assert_eq!(cache.probe(512).unwrap().unwrap().group(), Some(29));
    assert_eq!(cache.probe(513).unwrap().unwrap().group(), None);
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, prior);
    let fresh = Cache::new(&keys, &memory, &token).unwrap().unwrap();
    assert_eq!(fresh.probe(0).unwrap().unwrap().group(), None);
    assert!(fresh.probe(1024).is_err());
    drop(fresh);
    drop(keys);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn inline_cache_masks_padding_and_distinguishes_empty_null_and_binary_lengths() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let mut execution = vortex::array::array_session().create_execution_ctx();
    for dtype in [
        DType::Utf8(Nullability::NonNullable),
        DType::Binary(Nullability::NonNullable),
    ] {
        for value in [
            b"".as_slice(),
            b"a",
            b"a\0",
            b"abcdefghijk",
            b"abcdefghijkl",
        ] {
            let raw = BinaryView::new_inlined(value).as_u128();
            let bits = 32 + value.len() * 8;
            let padding = if bits == 128 { 0 } else { u128::MAX << bits };
            let views = Buffer::from_iter(
                (0..512).map(|row| BinaryView::from(raw | if row % 2 == 0 { 0 } else { padding })),
            );
            let source = VarBinViewArray::try_new(
                views,
                Vec::new().into(),
                dtype.clone(),
                Validity::NonNullable,
                &mut execution,
            )
            .unwrap()
            .into_array();
            let bound = keys(&[source], &memory);
            assert_eq!(identity(&bound[0], 0).unwrap(), Some(raw));
            assert_eq!(identity(&bound[0], 1).unwrap(), Some(raw));
            let mut cache = Cache::new(&bound, &memory, &CancellationToken::default())
                .unwrap()
                .unwrap();
            let first = cache.probe(0).unwrap().unwrap();
            cache.insert(first, 3);
            assert_eq!(cache.probe(1).unwrap().unwrap().group(), Some(3));
        }
    }
    let array = VarBinViewArray::from_iter_nullable_str([
        None,
        Some(""),
        Some("\0"),
        Some("\0\0"),
        Some("é"),
        Some("abcdefghijkl"),
        Some("abcdefghijklm"),
    ])
    .into_array();
    let bound = keys(&[array], &memory);
    let values = (0..6)
        .map(|row| identity(&bound[0], row).unwrap().unwrap())
        .collect::<Vec<_>>();
    for (index, value) in values.iter().enumerate() {
        assert!(!values[..index].contains(value));
    }
    assert_eq!(identity(&bound[0], 6).unwrap(), None);
    drop(bound);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn inline_cache_integer_boolean_and_null_identities_are_exact() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    macro_rules! check {
        ($($ty:ty),+) => { $(
            let source = PrimitiveArray::from_option_iter([
                Some(<$ty>::MIN), Some(<$ty>::MAX), None, Some(0), Some(<$ty>::MAX),
            ]).into_array();
            let bound = keys(&[source], &memory);
            let values = (0..5).map(|row| identity(&bound[0], row).unwrap().unwrap()).collect::<Vec<_>>();
            for left in 0..5 {
                for right in 0..5 {
                    assert_eq!(values[left] == values[right], bound[0].cell(left).unwrap() == bound[0].cell(right).unwrap());
                }
            }
        )+ };
    }
    check!(u8, u16, u32, u64, i8, i16, i32, i64);
    let bound = keys(
        &[BoolArray::from_iter([Some(false), None, Some(true), Some(false)]).into_array()],
        &memory,
    );
    assert_eq!(
        (0..4)
            .map(|row| identity(&bound[0], row).unwrap())
            .collect::<Vec<_>>(),
        vec![Some(0), Some(2), Some(1), Some(0)]
    );
    assert_eq!(identity(&KeyColumn::Null(1), 0).unwrap(), Some(0));
    drop(bound);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn inline_cache_declines_unsupported_sparse_tiny_and_pressure_domains() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let token = CancellationToken::default();
    let repeated = VarBinViewArray::from_iter_str(std::iter::repeat_n("short", 512)).into_array();
    let nested = StructArray::try_new(
        FieldNames::from(["child"]),
        vec![repeated.clone()],
        512,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter(std::iter::repeat_n(0u8, 512)).into_array(),
        VarBinViewArray::from_iter_str(["short"]).into_array(),
    )
    .unwrap()
    .into_array();
    let cases = [
        vec![VarBinViewArray::from_iter_str((0..512).map(|row| format!("key-{row}"))).into_array()],
        vec![repeated.slice(0..511).unwrap()],
        vec![
            VarBinViewArray::from_iter_str(std::iter::repeat_n("more-than-twelve-bytes", 512))
                .into_array(),
        ],
        vec![
            PrimitiveArray::from_iter(std::iter::repeat_n(1.0_f64, 512)).into_array(),
            repeated.clone(),
        ],
        vec![nested, repeated.clone()],
        vec![dictionary, repeated.clone()],
        vec![repeated.clone(), repeated.clone(), repeated.clone()],
    ];
    for arrays in cases {
        let bound = keys(&arrays, &memory);
        let before = memory.snapshot().reserved_bytes;
        assert!(Cache::new(&bound, &memory, &token).unwrap().is_none());
        assert_eq!(memory.snapshot().reserved_bytes, before);
    }
    let bound = keys(&[repeated], &memory);
    let prior = memory.snapshot().reserved_bytes;
    let required = bytes::<Entry>(SLOTS).unwrap();
    let pressure = memory
        .reserve(memory.snapshot().limit_bytes - prior - required * 16 + 1)
        .unwrap();
    assert!(Cache::new(&bound, &memory, &token).unwrap().is_none());
    drop(pressure);
    assert_eq!(memory.snapshot().reserved_bytes, prior);
    token.cancel();
    assert!(Cache::new(&bound, &memory, &token).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, prior);
    drop(bound);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
