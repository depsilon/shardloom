// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the ShardLoom contributors
// Independent fixture producer. No workspace patches or query-engine integrations.

use std::{fs, io::Write, path::Path, time::Instant};
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef, IntoArray, VortexSessionExecute,
        arrays::{
            BoolArray, ConstantArray, DictArray, ListArray, PrimitiveArray, StructArray,
            VarBinArray,
        },
        dtype::{DType, Nullability},
        validity::Validity,
    },
    encodings::{fastlanes::BitPackedData, runend::RunEnd},
    file::WriteOptionsSessionExt,
    io::{
        runtime::{BlockingRuntime, current::CurrentThreadRuntime},
        session::RuntimeSessionExt,
    },
    ipc::messages::{EncoderMessage, MessageEncoder},
    session::VortexSession,
};

type Error = Box<dyn std::error::Error>;

fn file(path: &Path) -> Result<fs::File, Error> {
    Ok(fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?)
}

fn emit(root: &Path, name: &str, array: ArrayRef) -> Result<(), Error> {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut encoder = MessageEncoder::new(session.clone());
    let begin = Instant::now();
    let dtype = encoder.encode(EncoderMessage::DType(array.dtype()))?;
    let encoded = encoder.encode(EncoderMessage::Array(&array))?;
    let export_ns = begin.elapsed().as_nanos();
    let dtype_bytes: usize = dtype.iter().map(|buffer| buffer.len()).sum();
    let array_bytes: usize = encoded.iter().map(|buffer| buffer.len()).sum();
    assert!(dtype_bytes + array_bytes <= 64 << 20);
    let transport = Instant::now();
    let mut dtype_out = file(&root.join(format!("{name}.dtype.vxipc")))?;
    for buffer in &dtype {
        dtype_out.write_all(buffer)?;
    }
    dtype_out.sync_all()?;
    let mut array_out = file(&root.join(format!("{name}.array.vxipc")))?;
    for buffer in &encoded {
        array_out.write_all(buffer)?;
    }
    array_out.sync_all()?;
    let transport_ns = transport.elapsed().as_nanos();
    let native = Instant::now();
    let mut writer = session.write_options().blocking(&runtime).writer(
        file(&root.join(format!("{name}.vortex")))?,
        array.dtype().clone(),
    );
    writer.push(array.clone())?;
    writer.finish()?;
    let native_ns = native.elapsed().as_nanos();
    println!(
        "{name}\t{}\t{dtype_bytes}\t{array_bytes}\t{export_ns}\t{transport_ns}\t{native_ns}",
        array.len()
    );
    Ok(())
}

fn main() -> Result<(), Error> {
    let root = std::env::args_os()
        .nth(1)
        .ok_or("expected a new output directory")?;
    let root = Path::new(&root);
    fs::create_dir(root)?;
    let session = VortexSession::default();
    let mut execution = session.create_execution_ctx();
    println!("fixture\trows\tdtype_bytes\tarray_bytes\texport_ns\ttransport_ns\tnative_file_ns");

    let n = 4099;
    let text: Vec<_> = (0..n)
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
    let mixed = StructArray::from_fields(&[
        (
            "key",
            PrimitiveArray::from_iter((0..n).map(|i| (1i64 << 52) + i as i64)).into_array(),
        ),
        (
            "amount",
            PrimitiveArray::from_option_iter(
                (0..n).map(|i| (i % 7 != 0).then_some(-10.5 + i as f64 / 4.0)),
            )
            .into_array(),
        ),
        (
            "flag",
            BoolArray::from_iter((0..n).map(|i| (i % 11 != 0).then_some(i % 2 == 0))).into_array(),
        ),
        (
            "text",
            VarBinArray::from_iter(
                text.iter().map(Option::as_deref),
                DType::Utf8(Nullability::Nullable),
            )
            .into_array(),
        ),
    ])?
    .into_array();
    emit(root, "mixed", mixed)?;

    let n = 65_539;
    let long = "shared dictionary payload λ ".repeat(128);
    let dictionary = VarBinArray::from_iter(
        [Some(""), Some("λ\0猫"), None, Some(long.as_str())],
        DType::Utf8(Nullability::Nullable),
    )
    .into_array();
    let codes = PrimitiveArray::from_iter((0..n).map(|i| (i % 4) as u16)).into_array();
    let integers = PrimitiveArray::from_iter((0..n).map(|i| (i % 31) as i32)).into_array();
    let encoded = StructArray::from_fields(&[
        ("constant", ConstantArray::new(11i64, n).into_array()),
        (
            "dictionary",
            DictArray::try_new(codes, dictionary)?.into_array(),
        ),
        (
            "run_end",
            RunEnd::encode(
                PrimitiveArray::from_iter((0..n).map(|i| (i / 97) as i64)).into_array(),
                &mut execution,
            )?
            .into_array(),
        ),
        (
            "packed",
            BitPackedData::encode(&integers, 5, &mut execution)?.into_array(),
        ),
    ])?
    .into_array();
    emit(root, "encoded", encoded)?;

    let n = 257;
    let nested = StructArray::from_fields(&[
        (
            "key",
            PrimitiveArray::from_iter(0i64..n as i64).into_array(),
        ),
        (
            "items",
            ListArray::try_new(
                PrimitiveArray::from_option_iter(
                    (0..2 * n).map(|i| (i % 5 != 0).then_some(i as i64 - 200)),
                )
                .into_array(),
                PrimitiveArray::from_iter((0..=n).map(|i| (2 * i) as u64)).into_array(),
                Validity::NonNullable,
            )?
            .into_array(),
        ),
    ])?
    .into_array();
    emit(root, "nested", nested)?;
    emit(
        root,
        "empty",
        StructArray::from_fields(&[
            (
                "key",
                PrimitiveArray::from_iter([] as [i64; 0]).into_array(),
            ),
            (
                "text",
                VarBinArray::from_iter([] as [Option<&str>; 0], DType::Utf8(Nullability::Nullable))
                    .into_array(),
            ),
        ])?
        .into_array(),
    )?;
    Ok(())
}
