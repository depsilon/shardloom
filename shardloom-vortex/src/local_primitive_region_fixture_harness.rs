// Private test child of `local_primitives`; included only with the existing
// `test`, `vortex-write`, and `unix` cfg gates. This source deliberately has
// no query or oracle code: it only constructs and writes the fixed inputs.

use super::native_flat_layout::SequentialNativeFlatLayout;
use std::{fs::OpenOptions, path::PathBuf};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{ConstantArray, DictArray, PrimitiveArray, StructArray, VarBinViewArray},
        dtype::{FieldNames, Nullability},
        scalar::Scalar,
        validity::Validity,
    },
    encodings::runend::RunEnd,
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

const MAX_CHUNK_BYTES: u64 = 128 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone, Copy)]
struct FixtureSpec {
    id: &'static str,
    rows: usize,
    chunks: usize,
}

fn spec(id: &str) -> FixtureSpec {
    match id {
        "signed" => FixtureSpec {
            id: "signed",
            rows: 4_194_304,
            chunks: 64,
        },
        "unsigned" => FixtureSpec {
            id: "unsigned",
            rows: 4_194_304,
            chunks: 64,
        },
        "nullable" => FixtureSpec {
            id: "nullable",
            rows: 4_194_304,
            chunks: 64,
        },
        "skewed" => FixtureSpec {
            id: "skewed",
            rows: 4_194_304,
            chunks: 49,
        },
        "small" => FixtureSpec {
            id: "small",
            rows: 4_096,
            chunks: 4,
        },
        "large_integer" => FixtureSpec {
            id: "large_integer",
            rows: 1_048_576,
            chunks: 16,
        },
        "floating" => FixtureSpec {
            id: "floating",
            rows: 1_048_576,
            chunks: 16,
        },
        "dictionary" => FixtureSpec {
            id: "dictionary",
            rows: 1_048_576,
            chunks: 16,
        },
        "wide_text" => FixtureSpec {
            id: "wide_text",
            rows: 262_144,
            chunks: 16,
        },
        "encoded" => FixtureSpec {
            id: "encoded",
            rows: 4_194_304,
            chunks: 64,
        },
        _ => panic!("unsupported fixed region fixture ID: {id}"),
    }
}

fn chunk_rows(spec: FixtureSpec, ordinal: usize) -> usize {
    match spec.id {
        "small" => 1_024,
        "wide_text" => 16_384,
        "skewed" if ordinal == 0 => 1_048_576,
        _ => 65_536,
    }
}

fn checked_index(start: usize, offset: usize) -> u64 {
    u64::try_from(start.checked_add(offset).expect("row index overflow"))
        .expect("fixed row index fits u64")
}

fn i16_value(value: u64) -> i16 {
    i16::try_from(value).expect("fixed I16 value is in range")
}

fn i8_value(value: u64) -> i8 {
    i8::try_from(value).expect("fixed I8 value is in range")
}

fn u16_value(value: u64) -> u16 {
    u16::try_from(value).expect("fixed U16 value is in range")
}

fn values_for(spec: FixtureSpec, ordinal: usize, start: usize, rows: usize) -> ArrayRef {
    let ix = |offset| checked_index(start, offset);
    let root = match spec.id {
        "signed" | "skewed" | "small" => {
            let x = (0..rows)
                .map(|j| i16_value(ix(j) % 2048) - 1024)
                .collect::<Vec<_>>();
            let y = (0..rows)
                .map(|j| i16_value((ix(j) * 17) % 4096) - 2048)
                .collect::<Vec<_>>();
            StructArray::try_new(
                FieldNames::from(["x", "y"]),
                vec![
                    PrimitiveArray::new(x, Validity::NonNullable).into_array(),
                    PrimitiveArray::new(y, Validity::NonNullable).into_array(),
                ],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }
        "unsigned" => {
            let x = (0..rows)
                .map(|j| u16_value(ix(j) % 65_521))
                .collect::<Vec<_>>();
            let y = (0..rows)
                .map(|j| u16_value((ix(j) * 13) % 257))
                .collect::<Vec<_>>();
            StructArray::try_new(
                FieldNames::from(["x", "y"]),
                vec![
                    PrimitiveArray::new(x, Validity::NonNullable).into_array(),
                    PrimitiveArray::new(y, Validity::NonNullable).into_array(),
                ],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }
        "nullable" => {
            let x = (0..rows)
                .map(|j| {
                    let i = ix(j);
                    (i % 7 != 0).then(|| i16_value(i % 997) - 498)
                })
                .collect::<Vec<_>>();
            let p = (0..rows).map(|j| i8_value(ix(j) % 5)).collect::<Vec<_>>();
            StructArray::try_new(
                FieldNames::from(["x", "p"]),
                vec![
                    PrimitiveArray::from_option_iter(x).into_array(),
                    PrimitiveArray::new(p, Validity::NonNullable).into_array(),
                ],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }
        "large_integer" => {
            let x = (0..rows)
                .map(|j| {
                    let factor = i64::try_from(ix(j) % 1024).unwrap() - 512;
                    factor.checked_mul(18_014_398_509_481_984).unwrap()
                })
                .collect::<Vec<_>>();
            StructArray::try_new(
                FieldNames::from(["x"]),
                vec![PrimitiveArray::new(x, Validity::NonNullable).into_array()],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }
        "floating" => {
            let x = (0..rows)
                .map(|j| match ix(j) % 4 {
                    0 => 1.0e16_f64,
                    1 => 1.0,
                    2 => -1.0e16,
                    _ => 0.1,
                })
                .collect::<Vec<_>>();
            StructArray::try_new(
                FieldNames::from(["x"]),
                vec![PrimitiveArray::new(x, Validity::NonNullable).into_array()],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }
        "dictionary" => {
            let dictionary = (0..257)
                .map(|j| format!("key-é-{j:03}"))
                .collect::<Vec<_>>();
            let codes = (0..rows)
                .map(|j| u16_value((ix(j) * 17) % 257))
                .collect::<Vec<_>>();
            let domain =
                VarBinViewArray::from_iter_str(dictionary.iter().map(String::as_str)).into_array();
            let values = DictArray::try_new(
                PrimitiveArray::new(codes, Validity::NonNullable).into_array(),
                domain,
            )
            .unwrap()
            .into_array();
            StructArray::try_new(
                FieldNames::from(["k"]),
                vec![values],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }
        "wide_text" => {
            let values = (0..rows)
                .map(|j| format!("{:03}{}", ix(j) % 257, "x".repeat(125)))
                .collect::<Vec<_>>();
            assert!(
                values
                    .iter()
                    .all(|value| value.len() == 128 && value.is_ascii())
            );
            let column =
                VarBinViewArray::from_iter_str(values.iter().map(String::as_str)).into_array();
            StructArray::try_new(
                FieldNames::from(["k"]),
                vec![column],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }
        "encoded" if ordinal % 2 == 0 => {
            let scalar = Scalar::primitive(-17_i16, Nullability::Nullable);
            let values = ConstantArray::new(scalar, rows).into_array();
            StructArray::try_new(
                FieldNames::from(["x"]),
                vec![values],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }
        "encoded" => {
            let run_rows = u32::try_from(rows / 4).expect("run end fits u32");
            let ends = PrimitiveArray::new(
                vec![run_rows, run_rows * 2, run_rows * 3, run_rows * 4],
                Validity::NonNullable,
            )
            .into_array();
            let values =
                PrimitiveArray::from_option_iter([Some(-5_i16), None, Some(7_i16), Some(29_i16)])
                    .into_array();
            let encoded = RunEnd::try_new(
                ends,
                values,
                &mut VortexSession::default().create_execution_ctx(),
            )
            .unwrap()
            .into_array();
            StructArray::try_new(
                FieldNames::from(["x"]),
                vec![encoded],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }
        _ => unreachable!("spec whitelist covers all fixed fixtures"),
    };
    assert!(root.dtype().is_struct());
    assert!(!root.dtype().is_nullable());
    root
}

#[test]
#[ignore = "explicitly generate one fixed native region fixture by ID"]
fn region_scalar_fixture_generate() {
    let id = std::env::var("SHARDLOOM_REGION_FIXTURE_ID").expect("fixture ID is required");
    let spec = spec(&id);
    let root = PathBuf::from(
        std::env::var_os("SHARDLOOM_REGION_FIXTURE_ROOT").expect("fixture root is required"),
    );
    assert!(root.is_dir(), "fixture root must already exist");
    let path = root.join(format!("{}.vortex", spec.id));
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());

    let first_rows = chunk_rows(spec, 0);
    let first = values_for(spec, 0, 0, first_rows);
    assert!(first.nbytes() <= MAX_CHUNK_BYTES);
    let dtype = first.dtype().clone();
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(SequentialNativeFlatLayout::strategy(spec.chunks))
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, dtype.clone());
    writer.push(first).unwrap();
    let mut start = first_rows;
    for ordinal in 1..spec.chunks {
        let rows = chunk_rows(spec, ordinal);
        let chunk = values_for(spec, ordinal, start, rows);
        assert_eq!(
            chunk.dtype(),
            &dtype,
            "all fixture chunks must share one dtype"
        );
        assert!(chunk.nbytes() <= MAX_CHUNK_BYTES);
        writer.push(chunk).unwrap();
        start = start.checked_add(rows).expect("row count overflow");
    }
    let summary = writer.finish().unwrap();
    assert_eq!(start, spec.rows);
    assert_eq!(summary.row_count(), u64::try_from(spec.rows).unwrap());
    drop(output);
    let bytes = std::fs::metadata(&path).unwrap().len();
    assert!(bytes <= MAX_FILE_BYTES, "fixture exceeds file-size cap");
    println!(
        "{}",
        serde_json::json!({
            "id": spec.id,
            "rows": spec.rows,
            "chunks": spec.chunks,
            "dtype": dtype.to_string(),
            "path": path.display().to_string(),
            "bytes": bytes,
        })
    );
}
