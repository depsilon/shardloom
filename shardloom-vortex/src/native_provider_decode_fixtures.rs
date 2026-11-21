//! Explicit wire-version and conditional-child coverage for registered codecs.

use vortex::{
    array::{
        ArrayRef, IntoArray, VortexSessionExecute as _,
        arrays::{PrimitiveArray, VarBinArray},
        dtype::{DType, DecimalDType, Nullability},
        session::ArraySessionExt as _,
        validity::Validity,
    },
    buffer::buffer,
    encodings::{
        alp::{ALPArrayExt as _, ALPRDArrayExt as _, RDEncoder, RDEncoderExt as _, alp_encode},
        bytebool::ByteBool,
        decimal_byte_parts::DecimalByteParts,
        fastlanes::{BitPackedData, Delta, FoR, RLE},
        parquet_variant::ParquetVariant,
        pco::Pco,
        sequence::Sequence,
        sparse::Sparse,
        zigzag::zigzag_encode,
    },
    session::VortexSession,
};

pub(super) struct Fixture {
    pub name: &'static str,
    pub array: ArrayRef,
}

pub(super) fn arrays(session: &VortexSession) -> Vec<Fixture> {
    let mut fixtures = floating_and_decimal_arrays(session);
    fixtures.extend(fastlanes_arrays(session));
    fixtures.extend(other_arrays(session));
    assert_eq!(fixtures.len(), 19);
    fixtures
}

fn floating_and_decimal_arrays(session: &VortexSession) -> Vec<Fixture> {
    let mut context = session.create_execution_ctx();
    let mut fixtures = Vec::new();
    let mut add = |name, wire_id: &str, array: ArrayRef| {
        let serialization = session.array_serialize(&array).unwrap().unwrap();
        assert_eq!(serialization.serialized_id.as_ref(), wire_id, "{name}");
        fixtures.push(Fixture { name, array });
    };

    let plain = PrimitiveArray::from_iter([1.25_f64, 2.5, -4.0]);
    add(
        "alp",
        "vortex.alp",
        alp_encode(plain.as_view(), None, &mut context)
            .unwrap()
            .into_array(),
    );
    let patch_values = PrimitiveArray::from_option_iter((0..100).map(|index| match index % 3 {
        0 => Some(1.0_f64),
        1 => None,
        _ => Some(std::f64::consts::PI),
    }));
    let patched = alp_encode(patch_values.as_view(), None, &mut context).unwrap();
    assert!(patched.patches().is_some());
    add("alp-nullable-patches", "vortex.alp", patched.into_array());

    let values = [0.125_f64, 0.25, 0.5];
    let input = PrimitiveArray::from_iter(values);
    add(
        "alprd",
        "vortex.alprd",
        RDEncoder::new(&values).encode(input.as_view()).into_array(),
    );
    let input = PrimitiveArray::from_option_iter(
        (0..32).map(|index| (index % 5 != 0).then_some(0.1_f64.next_up())),
    );
    let patched = RDEncoder::new(&[1.123_848_591_110_992_f64.powi(-2)]).encode(input.as_view());
    assert!(patched.left_parts_patches().is_some());
    add(
        "alprd-nullable-patches",
        "vortex.alprd",
        patched.into_array(),
    );
    add(
        "bytebool-nullable",
        "vortex.bytebool",
        ByteBool::from_option_vec(vec![Some(true), None, Some(false), Some(true)]).into_array(),
    );
    add(
        "decimal-byte-parts-v1",
        "vortex.decimal_byte_parts",
        DecimalByteParts::try_new(
            PrimitiveArray::from_option_iter([Some(-3_i32), None, Some(7)]).into_array(),
            DecimalDType::new(9, 2),
        )
        .unwrap()
        .into_array(),
    );
    add(
        "decimal-byte-parts-v2",
        "vortex.decimal_byte_parts.v2",
        DecimalByteParts::try_new_with_lower_parts(
            PrimitiveArray::from_option_iter([Some(-1_i64), None, Some(0)]).into_array(),
            vec![buffer![u64::MAX, 0, 7].into_array()],
            DecimalDType::new(38, 2),
        )
        .unwrap()
        .into_array(),
    );
    fixtures
}

fn fastlanes_arrays(session: &VortexSession) -> Vec<Fixture> {
    let mut context = session.create_execution_ctx();
    let mut fixtures = Vec::new();
    let mut add = |name, wire_id: &str, array: ArrayRef| {
        let serialization = session.array_serialize(&array).unwrap().unwrap();
        assert_eq!(serialization.serialized_id.as_ref(), wire_id, "{name}");
        fixtures.push(Fixture { name, array });
    };
    let values =
        PrimitiveArray::from_option_iter([Some(1_i32), None, Some(2), Some(3)]).into_array();
    add(
        "bitpacked-v1",
        "fastlanes.bitpacked",
        BitPackedData::encode(&values, 2, &mut context)
            .unwrap()
            .into_array(),
    );
    let values =
        PrimitiveArray::from_option_iter([Some(1_i32), None, Some(512), Some(3)]).into_array();
    add(
        "bitpacked-v1-patches",
        "fastlanes.bitpacked",
        BitPackedData::encode(&values, 2, &mut context)
            .unwrap()
            .into_array(),
    );
    add(
        "bitpacked-v2",
        "fastlanes.bitpacked.v2",
        BitPackedData::encode_blocked(&values, &[2], &mut context)
            .unwrap()
            .into_array(),
    );
    let values = PrimitiveArray::from_option_iter([Some(-4_i32), None, Some(3), Some(7)]);
    add(
        "delta-nullable",
        "fastlanes.delta",
        Delta::try_from_primitive_array(&values, &mut context)
            .unwrap()
            .into_array(),
    );
    add(
        "for-v1",
        "fastlanes.for",
        FoR::try_new(values.clone().into_array(), 100_i32.into())
            .unwrap()
            .into_array(),
    );
    add(
        "for-v2",
        "fastlanes.for.v2",
        FoR::try_new_chunked(values.into_array(), buffer![100_i32].into_array(), 0)
            .unwrap()
            .into_array(),
    );
    let values = PrimitiveArray::from_option_iter([Some(1_i32), Some(1), None, Some(2)]);
    add(
        "rle-nullable",
        "fastlanes.rle",
        RLE::encode(values.as_view(), &mut context)
            .unwrap()
            .into_array(),
    );
    fixtures
}

fn other_arrays(session: &VortexSession) -> Vec<Fixture> {
    let mut context = session.create_execution_ctx();
    let mut fixtures = Vec::new();
    let mut add = |name, wire_id: &str, array: ArrayRef| {
        let serialization = session.array_serialize(&array).unwrap().unwrap();
        assert_eq!(serialization.serialized_id.as_ref(), wire_id, "{name}");
        fixtures.push(Fixture { name, array });
    };
    add(
        "parquet-variant-typed-nullable",
        "vortex.parquet.variant",
        ParquetVariant::try_new(
            Validity::from_iter([true, false, true]),
            VarBinArray::from_iter(
                [Some(b"\x01\x00".as_slice()); 3],
                DType::Binary(Nullability::NonNullable),
            )
            .into_array(),
            None,
            Some(PrimitiveArray::from_option_iter([Some(12_i32), None, Some(-4)]).into_array()),
        )
        .unwrap()
        .into_array(),
    );
    let values = PrimitiveArray::from_option_iter([Some(1_i32), Some(1), None, Some(2)]);
    add(
        "pco-nullable",
        "vortex.pco",
        Pco::from_primitive(values.as_view(), 3, 2, &mut context)
            .unwrap()
            .into_array(),
    );
    add(
        "sparse",
        "vortex.sparse",
        Sparse::try_new(
            buffer![1_u64, 3].into_array(),
            buffer![7_i32, -3].into_array(),
            5,
            0_i32.into(),
            &mut context,
        )
        .unwrap()
        .into_array(),
    );
    add(
        "zigzag-nullable",
        "vortex.zigzag",
        zigzag_encode(PrimitiveArray::from_option_iter([Some(-3_i32), None, Some(7)]).as_view())
            .unwrap()
            .into_array(),
    );
    add(
        "sequence-leaf",
        "vortex.sequence",
        Sequence::try_new_typed(-3_i64, 2, Nullability::NonNullable, 4)
            .unwrap()
            .into_array(),
    );
    fixtures
}
