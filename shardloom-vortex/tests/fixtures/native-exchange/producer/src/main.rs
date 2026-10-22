// SPDX-License-Identifier: Apache-2.0
// Independent native exchange inputs: upstream Vortex only, no workspace patches.
// The separate Python recipe specifies every logical value and expected schema.

use std::{fs, io::Write, path::Path};
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef, IntoArray, VortexSessionExecute,
        arrays::{
            BoolArray, ConstantArray, DictArray, ListArray, PrimitiveArray, StructArray,
            VarBinArray,
        },
        dtype::{DType, Nullability, PType},
        validity::Validity,
    },
    encodings::{fastlanes::BitPackedData, runend::RunEnd},
    ipc::messages::{EncoderMessage, MessageEncoder},
    scalar::Scalar,
    session::VortexSession,
};

type Error = Box<dyn std::error::Error>;

fn session() -> VortexSession {
    let session = VortexSession::default();
    // With file support disabled, these native providers require registration.
    vortex::encodings::fastlanes::initialize(&session);
    vortex::encodings::runend::initialize(&session);
    session
}

fn emit(root: &Path, name: &str, array: ArrayRef) -> Result<(), Error> {
    let mut encoder = MessageEncoder::new(session());
    let dtype = encoder.encode(EncoderMessage::DType(array.dtype()))?;
    let body = encoder.encode(EncoderMessage::Array(&array))?;
    let dtype_bytes: usize = dtype.iter().map(|part| part.len()).sum();
    let body_bytes: usize = body.iter().map(|part| part.len()).sum();
    assert!(dtype_bytes + body_bytes <= 64 << 20);
    for (suffix, parts) in [("dtype", dtype), ("array", body)] {
        let mut output = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(root.join(format!("{name}.{suffix}.vxipc")))?;
        for part in parts {
            output.write_all(&part)?;
        }
        output.sync_all()?;
    }
    Ok(())
}

fn ids(rows: usize) -> ArrayRef {
    PrimitiveArray::from_iter((0..rows).map(|i| (1_i64 << 52) + 17 * i as i64)).into_array()
}

fn text(values: &[Option<String>]) -> ArrayRef {
    VarBinArray::from_iter(
        values.iter().map(Option::as_deref),
        DType::Utf8(Nullability::Nullable),
    )
    .into_array()
}

fn plain(rows: usize) -> Result<ArrayRef, Error> {
    let strings: Vec<_> = (0..rows)
        .map(|i| {
            (i % 17 != 0).then(|| {
                if i % 13 == 0 {
                    String::new()
                } else {
                    format!("row-{i}-λ\0猫")
                }
            })
        })
        .collect();
    Ok(StructArray::from_fields(&[
        ("event_key", ids(rows)),
        (
            "measure",
            PrimitiveArray::from_option_iter(
                (0..rows).map(|i| (i % 7 != 0).then_some((i % 201) as i32 - 100)),
            )
            .into_array(),
        ),
        (
            "enabled",
            BoolArray::from_iter((0..rows).map(|i| (i % 11 != 0).then_some(i % 2 == 0)))
                .into_array(),
        ),
        ("caption", text(&strings)),
    ])?
    .into_array())
}

fn dictionary(rows: usize, domain: usize, variant: &str) -> Result<ArrayRef, Error> {
    let values: Vec<_> = (0..domain)
        .map(|value| match value {
            0 => None,
            1 => Some(String::new()),
            _ => Some(format!(
                "dict-{variant}-{value:03}-λ\0猫-{}",
                "prefix/".repeat(4)
            )),
        })
        .collect();
    let codes = PrimitiveArray::from_option_iter(
        (0..rows).map(|row| (row % 97 != 0).then_some(((row * 19 + 3) % domain) as u16)),
    )
    .into_array();
    Ok(DictArray::try_new(codes, text(&values))?.into_array())
}

fn dictionary_record(rows: usize, domain: usize, variant: &str) -> Result<ArrayRef, Error> {
    Ok(StructArray::from_fields(&[
        ("event_key", ids(rows)),
        ("category", dictionary(rows, domain, variant)?),
    ])?
    .into_array())
}

fn nested(rows: usize) -> Result<ArrayRef, Error> {
    let attributes = StructArray::from_fields(&[
        ("category", dictionary(rows, 17, "nested")?),
        (
            "enabled",
            BoolArray::from_iter((0..rows).map(|i| (i % 11 != 0).then_some(i % 2 == 0)))
                .into_array(),
        ),
    ])?
    .into_array();
    let samples = ListArray::try_new(
        PrimitiveArray::from_option_iter(
            (0..2 * rows).map(|i| (i % 5 != 0).then_some(i as i32 - 200)),
        )
        .into_array(),
        PrimitiveArray::from_iter((0..=rows).map(|i| (2 * i) as u64)).into_array(),
        Validity::from_iter((0..rows).map(|i| i % 13 != 0)),
    )?
    .into_array();
    Ok(StructArray::from_fields(&[
        ("event_key", ids(rows)),
        ("attributes", attributes),
        ("samples", samples),
    ])?
    .into_array())
}

fn main() -> Result<(), Error> {
    let output = std::env::args_os()
        .nth(1)
        .ok_or("expected a new local-only output directory")?;
    let root = Path::new(&output);
    fs::create_dir(root)?;
    for (name, rows) in [("empty", 0), ("tiny", 7), ("nullable_plain", 16_387)] {
        emit(root, name, plain(rows)?)?;
    }
    let rows = 65_533;
    emit(
        root,
        "all_null",
        StructArray::from_fields(&[
            (
                "absent_text",
                ConstantArray::new(Scalar::null(DType::Utf8(Nullability::Nullable)), rows)
                    .into_array(),
            ),
            (
                "absent_number",
                ConstantArray::new(
                    Scalar::null(DType::Primitive(PType::I64, Nullability::Nullable)),
                    rows,
                )
                .into_array(),
            ),
        ])?
        .into_array(),
    )?;
    emit(
        root,
        "constant",
        StructArray::from_fields(&[
            ("fixed_flag", ConstantArray::new(true, rows).into_array()),
            (
                "fixed_text",
                ConstantArray::new("constant λ\0猫", rows).into_array(),
            ),
            (
                "fixed_number",
                ConstantArray::new(-((1_i64 << 53) + 17), rows).into_array(),
            ),
        ])?
        .into_array(),
    )?;
    for (name, rows, domain, variant) in [
        ("dictionary_small", 4_099, 17, "primary"),
        ("dictionary_large", 65_533, 257, "primary"),
        ("dictionary_other", 65_533, 257, "other"),
    ] {
        emit(root, name, dictionary_record(rows, domain, variant)?)?;
    }
    let mut execution = session().create_execution_ctx();
    emit(
        root,
        "run_end",
        StructArray::from_fields(&[
            ("event_key", ids(rows)),
            (
                "run_value",
                RunEnd::encode(
                    PrimitiveArray::from_option_iter(
                        (0..rows)
                            .map(|i| ((i / 97) % 11 != 0).then_some(((i / 97) % 201) as i64 - 100)),
                    )
                    .into_array(),
                    &mut execution,
                )?
                .into_array(),
            ),
        ])?
        .into_array(),
    )?;
    let packed = PrimitiveArray::from_iter((0..rows).map(|i| ((i * 17) % 31) as i32)).into_array();
    emit(
        root,
        "bitpacked",
        StructArray::from_fields(&[
            ("event_key", ids(rows)),
            (
                "packed_value",
                BitPackedData::encode(&packed, 5, &mut execution)?.into_array(),
            ),
        ])?
        .into_array(),
    )?;
    let rows = 8_195;
    let wide: Vec<_> = (0..rows)
        .map(|i| {
            (i % 17 != 0).then(|| {
                format!(
                    "row-{i:05}-{}-猫",
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".repeat(14)
                )
            })
        })
        .collect();
    emit(
        root,
        "wide_text",
        StructArray::from_fields(&[("event_key", ids(rows)), ("wide_caption", text(&wide))])?
            .into_array(),
    )?;
    emit(root, "nested", nested(4_099)?)?;
    let rows = 16_394;
    let shared = dictionary(rows, 257, "shared")?;
    let sliced = StructArray::from_fields(&[
        ("event_key", ids(rows)),
        ("left_text", shared.clone()),
        ("right_text", shared),
    ])?
    .into_array()
    .slice(7..rows)?;
    emit(root, "sliced_shared", sliced)?;
    Ok(())
}
