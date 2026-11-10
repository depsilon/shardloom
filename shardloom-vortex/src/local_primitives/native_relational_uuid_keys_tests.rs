//! Fixed independent UUID identities exercise the complete native key domain.

use super::*;
use std::sync::Arc;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{ConstantArray, DictArray, ExtensionArray, FixedSizeListArray, StructArray},
    dtype::{
        Nullability,
        extension::{ExtDType, ExtVTable as _},
    },
    validity::Validity,
};
use vortex::{
    VortexSessionDefault as _,
    encodings::uuid::{Uuid, UuidMetadata},
};

const PREFIX: u128 = 0x01234567_89ab_cdef_fedc_ba9876543200;

fn array(values: &[Option<u128>]) -> ArrayRef {
    let bytes = PrimitiveArray::from_iter(
        values
            .iter()
            .flat_map(|value| value.unwrap_or(u128::MAX).to_be_bytes()),
    )
    .into_array();
    let storage = FixedSizeListArray::try_new(
        bytes,
        16,
        Validity::from_iter(values.iter().map(Option::is_some)),
        values.len(),
    )
    .unwrap()
    .into_array();
    let dtype = ExtDType::try_with_vtable(Uuid, UuidMetadata::default(), storage.dtype().clone())
        .unwrap()
        .erased();
    ExtensionArray::new(dtype, storage).into_array()
}

fn key(array: &ArrayRef, memory: &LiveMemoryPool) -> KeyColumn {
    KeyColumn::new(
        array,
        &mut vortex::session::VortexSession::default().create_execution_ctx(),
        memory,
        &CancellationToken::default(),
    )
    .unwrap()
}

fn hash(column: &KeyColumn, row: usize) -> (bool, u64) {
    let mut hash = rustc_hash::FxHasher::default();
    (column.hash_into(row, &mut hash).unwrap(), hash.finish())
}

#[test]
fn native_uuid_keys_preserve_full_bytes_order_nulls_and_dictionary_slices() {
    let expected = [
        Some(u128::MAX),
        Some(0),
        Some(PREFIX + 1),
        None,
        Some(PREFIX),
        Some(PREFIX + 1),
    ];
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let domain = array(&expected);
    let direct = key(&domain, &memory);
    let codes = PrimitiveArray::from_iter([0u16, 0, 1, 2, 3, 4, 5, 0]).into_array();
    let dictionary = DictArray::try_new(codes, domain.clone())
        .unwrap()
        .into_array()
        .slice(1..7)
        .unwrap();
    let dictionary = key(&dictionary, &memory);
    assert!(direct.same_flat_order_domain(&dictionary));
    for (row, expected) in expected.iter().enumerate() {
        let cell = expected.map_or(Cell::Null, |value| Cell::Uuid(value.to_be_bytes()));
        assert_eq!(direct.cell(row).unwrap(), cell);
        assert_eq!(dictionary.cell(row).unwrap(), cell);
        assert_eq!(hash(&direct, row), hash(&dictionary, row));
        assert_eq!(
            direct.equals_at(row, &dictionary, row, false).unwrap(),
            expected.is_some()
        );
        assert!(direct.equals_at(row, &dictionary, row, true).unwrap());
    }
    for left in 0..expected.len() {
        for right in 0..expected.len() {
            assert_eq!(
                direct.compare_at(left, &dictionary, right).unwrap(),
                expected[left].cmp(&expected[right])
            );
        }
    }
    assert_ne!(hash(&direct, 1), hash(&direct, 3)); // nil and SQL NULL
    assert_ne!(hash(&direct, 2), hash(&direct, 4)); // final byte participates
    assert!(direct.cell(expected.len()).is_err());
    let bytes = VarBinViewArray::from_iter_bin([0_u128.to_be_bytes()]).into_array();
    let binary = key(&bytes, &memory);
    assert!(!direct.same_flat_order_domain(&binary));
    assert!(direct.compare_at(1, &binary, 0).is_err());
    assert_ne!(hash(&direct, 1), hash(&binary, 0));
    let mut context = vortex::session::VortexSession::default().create_execution_ctx();
    let repeated =
        ConstantArray::new(domain.execute_scalar(2, &mut context).unwrap(), 9).into_array();
    let repeated = key(&repeated, &memory);
    for row in 0..9 {
        assert_eq!(
            repeated.cell(row).unwrap(),
            Cell::Uuid((PREFIX + 1).to_be_bytes())
        );
        assert_eq!(hash(&repeated, row), hash(&direct, 2));
    }
    drop((direct, dictionary, binary, repeated));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_uuid_nested_keys_keep_leaf_identity_and_parent_nulls() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let values = array(&[Some(0), Some(0), None, Some(PREFIX)]);
    let nested = StructArray::try_new(
        ["id"].into_iter().collect(),
        vec![values],
        4,
        Validity::from_iter([true, false, true, true]),
    )
    .unwrap()
    .into_array();
    let keys = key(&nested, &memory);
    let mut text = Vec::new();
    for row in 0..4 {
        let mut output = String::new();
        keys.write_exact_key(row, &mut output, &CancellationToken::default())
            .unwrap();
        text.push(output);
    }
    assert!(text[0].contains("U00000000000000000000000000000000;"));
    for left in 0..4 {
        for right in 0..4 {
            assert_eq!(
                keys.equals_at(left, &keys, right, true).unwrap(),
                left == right
            );
            assert_eq!(text[left] == text[right], left == right);
        }
    }
    assert!(keys.is_null(1).unwrap());
    assert!(!keys.is_null(2).unwrap());
    drop(keys);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_uuid_restricted_metadata_is_rejected_even_without_values() {
    let storage = DType::FixedSizeList(
        Arc::new(DType::Primitive(PType::U8, Nullability::NonNullable)),
        16,
        Nullability::Nullable,
    );
    for encoded in [vec![0], vec![4], vec![15]] {
        let dtype = DType::Extension(
            ExtDType::try_with_vtable(
                Uuid,
                Uuid.deserialize_metadata(&encoded).unwrap(),
                storage.clone(),
            )
            .unwrap()
            .erased(),
        );
        assert!(crate::native_uuid::storage(&dtype).is_none());
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        assert!(crate::native_payload_schema::reserve_schema(&dtype, &memory).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let memory = LiveMemoryPool::new(1).unwrap();
    let mut context = vortex::session::VortexSession::default().create_execution_ctx();
    assert!(
        KeyColumn::new(
            &array(&[]),
            &mut context,
            &memory,
            &CancellationToken::default()
        )
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
