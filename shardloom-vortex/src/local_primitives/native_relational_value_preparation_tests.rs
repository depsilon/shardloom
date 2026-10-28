use super::*;
use vortex::array::{
    ArrayParts, IntoArray as _, VortexSessionExecute as _,
    arrays::{DictArray, ListViewArray, StructArray, VarBin, VarBinArray},
    dtype::{FieldNames, Nullability},
    validity::Validity,
};

fn prepare(array: &ArrayRef, memory: &LiveMemoryPool, purpose: Preparation) -> Result<KeyColumn> {
    KeyColumn::prepare(
        array,
        &mut vortex::array::array_session().create_execution_ctx(),
        memory,
        &CancellationToken::default(),
        purpose,
    )
}

#[test]
fn value_preparation_preserves_plain_dictionary_and_binary_values_without_hash_slots() {
    for domain in [
        VarBinViewArray::from_iter_nullable_str([Some("東京\0"), Some(""), None, Some("unused")])
            .into_array(),
        VarBinArray::from(vec![
            Some(&b"\xff\0"[..]),
            Some(&b""[..]),
            None,
            Some(&b"unused"[..]),
        ])
        .into_array(),
    ] {
        let encoded = DictArray::try_new(
            PrimitiveArray::from_iter([0u8, 1, 0, 2]).into_array(),
            domain.clone(),
        )
        .unwrap()
        .into_array();
        for array in [&domain, &encoded] {
            let memory = LiveMemoryPool::new(4096).unwrap();
            let values = prepare(array, &memory, Preparation::Values).unwrap();
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
            let hashed = prepare(array, &memory, Preparation::Hash).unwrap();
            assert_eq!(memory.snapshot().reserved_bytes, 4 * 8);
            for row in 0..array.len() {
                assert_eq!(values.raw_cell(row).unwrap(), hashed.raw_cell(row).unwrap());
                assert_eq!(
                    values.compare_at(row, &hashed, row).unwrap(),
                    Ordering::Equal
                );
                values.validate_at(row).unwrap();
                hashed
                    .hash_into(row, &mut rustc_hash::FxHasher::default())
                    .unwrap();
            }
            assert!(
                values
                    .hash_into(0, &mut rustc_hash::FxHasher::default())
                    .unwrap_err()
                    .to_string()
                    .contains("no admitted hash state")
            );
            drop((values, hashed));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn value_preparation_keeps_hash_admission_cancellation_and_empty_null_contracts() {
    let source = VarBinViewArray::from_iter_str(["a", "b"]).into_array();
    let memory = LiveMemoryPool::new(15).unwrap();
    let values = prepare(&source, &memory, Preparation::Values).unwrap();
    assert_eq!(
        values.cell(1).unwrap(),
        Cell::Utf8(ByteBuffer::from(b"b".to_vec()))
    );
    assert!(prepare(&source, &memory, Preparation::Hash).is_err());
    drop(values);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for array in [
        VarBinViewArray::from_iter_nullable_str([None::<&str>, None]).into_array(),
        VarBinViewArray::from_iter_str([] as [&str; 0]).into_array(),
    ] {
        let values = prepare(&array, &memory, Preparation::Values).unwrap();
        for row in 0..array.len() {
            assert_eq!(values.cell(row).unwrap(), Cell::Null);
            values.validate_at(row).unwrap();
        }
        assert!(values.validate_at(array.len()).is_err());
    }
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    let mut context = vortex::array::array_session().create_execution_ctx();
    assert!(KeyColumn::values(&source, &mut context, &memory, &cancellation).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn value_preparation_rejects_unreferenced_malformed_utf8_domain_values() {
    // The safe low-level VarBin parts API defers value checking to execution.
    // Only dictionary entry zero is referenced; entry one is still required
    // to pass the existing complete-domain UTF-8 check for either purpose.
    let binary = VarBinArray::from(vec![&b"good"[..], &b"\xff"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    let domain = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        2,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let dictionary = DictArray::try_new(PrimitiveArray::from_iter([0u8]).into_array(), domain)
        .unwrap()
        .into_array();
    for purpose in [
        Preparation::Values,
        Preparation::Hash,
        Preparation::HashOnDemand,
    ] {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let error = prepare(&dictionary, &memory, purpose)
            .err()
            .expect("invalid domain admitted");
        assert!(error.to_string().contains("utf-8"), "{error}");
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn value_preparation_checks_dictionary_codes_when_observed() {
    for codes in [
        PrimitiveArray::from_iter([0u64, u64::MAX]).into_array(),
        PrimitiveArray::from_iter([0i64, -1]).into_array(),
        PrimitiveArray::from_iter([0u8, 2]).into_array(),
    ] {
        let dictionary = DictArray::try_new(
            codes,
            VarBinViewArray::from_iter_str(["first", "second"]).into_array(),
        )
        .unwrap()
        .into_array();
        for purpose in [
            Preparation::Values,
            Preparation::Hash,
            Preparation::HashOnDemand,
        ] {
            let memory = LiveMemoryPool::new(4096).unwrap();
            let values = prepare(&dictionary, &memory, purpose).unwrap();
            values.validate_at(0).unwrap();
            assert!(values.validate_at(1).is_err());
            assert!(values.cell(1).is_err());
            if purpose != Preparation::Values {
                values
                    .hash_into(0, &mut rustc_hash::FxHasher::default())
                    .unwrap();
                assert!(
                    values
                        .hash_into(1, &mut rustc_hash::FxHasher::default())
                        .is_err()
                );
            }
            drop(values);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn value_preparation_validates_every_observed_nested_child_but_skips_null_parents() {
    for null_parent in [false, true] {
        let nested = StructArray::try_new(
            FieldNames::from(["first", "later"]),
            vec![
                PrimitiveArray::from_iter([1i64, 2]).into_array(),
                PrimitiveArray::from_iter([0.0f64, f64::NAN]).into_array(),
            ],
            2,
            Validity::from_iter([true, !null_parent]),
        )
        .unwrap()
        .into_array();
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let values = prepare(&nested, &memory, Preparation::Values).unwrap();
        let hashed = prepare(&nested, &memory, Preparation::Hash).unwrap();
        assert_eq!(values.validate_at(1).is_ok(), null_parent);
        assert_eq!(
            hashed
                .hash_into(1, &mut rustc_hash::FxHasher::default())
                .is_ok(),
            null_parent
        );
        values.validate_at(0).unwrap();
        drop((values, hashed));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let children = DecimalArray::new(
        vec![0i128, 10].into(),
        DecimalDType::new(1, 0),
        Validity::NonNullable,
    )
    .into_array();
    let lists = ListViewArray::try_new(
        children,
        PrimitiveArray::from_iter([0u64, 0]).into_array(),
        PrimitiveArray::from_iter([1u64, 2]).into_array(),
        Validity::NonNullable,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::array::array_session(),
        ),
    )
    .unwrap()
    .into_array();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let values = prepare(&lists, &memory, Preparation::Values).unwrap();
    values.validate_at(0).unwrap();
    assert!(
        values
            .validate_at(1)
            .unwrap_err()
            .to_string()
            .contains("declared precision")
    );
    drop(values);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
