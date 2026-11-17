//! Original-body parity and private deep typed-value mechanism acceptance.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value as Json, json};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, VortexSessionExecute as _,
        arrays::{
            Bool, FixedSizeList, List, Primitive, Struct, bool::BoolArrayExt as _,
            fixed_size_list::FixedSizeListArraySlotsExt as _, list::ListArraySlotsExt as _,
            struct_::StructArrayExt as _,
        },
        dtype::{DType, FieldNames, Nullability, PType, StructFields},
        memory::{
            HostAllocator, HostAllocatorRef, HostMemoryReservation, HostMemorySessionExt as _,
            WritableHostBuffer,
        },
    },
    buffer::Alignment,
    error::VortexResult,
    session::VortexSession,
};

use super as native_typed_input;
use super::super::{
    native_capacity, native_input_schema, native_json, native_payload, result_batch, vortex_error,
};

// These two files are byte-exact originals from e111f6387. The original input
// builder resolves its Layout import through the original module below.
#[path = "native_input_values_reference.rs"]
mod native_input_values;
#[path = "native_typed_input_reference.rs"]
mod reference;

#[derive(Debug, PartialEq)]
struct Output {
    rows: Vec<Vec<u8>>,
    bytes: usize,
    copied: u64,
}

fn allocator(memory: &LiveMemoryPool) -> HostAllocatorRef {
    Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
        memory.clone(),
    ))
}

#[derive(Debug)]
struct PanicAllocator {
    inner: HostAllocatorRef,
    remaining: AtomicUsize,
}

impl HostAllocator for PanicAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        let allocated = self.inner.allocate(len, alignment)?;
        assert!(
            self.remaining.fetch_sub(1, Ordering::SeqCst) > 0,
            "intentional typed input allocation panic"
        );
        Ok(allocated)
    }

    fn reserve_external(&self, len: usize) -> VortexResult<HostMemoryReservation> {
        self.inner.reserve_external(len)
    }
}

fn check_panic(dtype: &DType, value: &Json, memory: &LiveMemoryPool) {
    let allocator: HostAllocatorRef = Arc::new(PanicAllocator {
        inner: allocator(memory),
        remaining: AtomicUsize::new(3),
    });
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            super::construct::construct(dtype, &[Some(value)], memory, &allocator).unwrap();
        }))
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn observe(dtype: &str, values: &[Option<&str>], old: bool) -> Result<Output, String> {
    let memory = LiveMemoryPool::new(1 << 26).unwrap();
    let allocator = allocator(&memory);
    let session = VortexSession::default().with_host_allocator(allocator.clone());
    crate::native_extensions::register(&session);
    let result = (|| {
        let (array, bytes, copied) = if old {
            let built = reference::build(dtype, values, &memory, &allocator, 1 << 20)?;
            (built.array, built.logical_bytes, built.copied_payload_bytes)
        } else {
            let built = super::build(dtype, values, &memory, &allocator, 1 << 20)?;
            (built.array, built.logical_bytes, built.copied_payload_bytes)
        };
        let mut execution = session.create_execution_ctx();
        let token = CancellationToken::default();
        let column = native_json::Column::new(&array, &mut execution, &memory, &token)?;
        let mut rows = Vec::new();
        for row in 0..array.len() {
            let mut output = Vec::new();
            column.write(row, &mut output, &mut execution, &token)?;
            rows.push(output);
        }
        Ok::<_, shardloom_core::ShardLoomError>(Output {
            rows,
            bytes,
            copied,
        })
    })()
    .map_err(|error| error.to_string());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    result
}

fn compare(dtype: &Json, values: &[Json]) -> bool {
    let dtype = dtype.to_string();
    let values: Vec<String> = values.iter().map(Json::to_string).collect();
    let values: Vec<_> = values.iter().map(|value| Some(value.as_str())).collect();
    let expected = observe(&dtype, &values, true);
    assert_eq!(
        observe(&dtype, &values, false),
        expected,
        "{dtype}, {values:?}"
    );
    expected.is_ok()
}

fn fixtures() -> Vec<(Json, Vec<Json>)> {
    vec![
        (
            json!({"Primitive":["i64",true]}),
            vec![json!(i64::MIN), Json::Null, json!(i64::MAX)],
        ),
        (
            json!({"Primitive":["u64",true]}),
            vec![json!(u64::MAX), Json::Null, json!(0)],
        ),
        (
            json!({"Primitive":["f64",true]}),
            vec![json!(-0.0), Json::Null, json!(1.000_000_000_000_000_2)],
        ),
        (
            json!({"Bool":true}),
            vec![json!(true), Json::Null, json!(false)],
        ),
        (
            json!({"Utf8":true}),
            vec![json!("é\"\n\\"), Json::Null, json!("")],
        ),
        (
            json!({"Binary":true}),
            vec![json!("0aFF"), Json::Null, json!("")],
        ),
        (
            json!({"Decimal":[{"precision":6,"scale":2},true]}),
            vec![
                json!("decimal128(6,2):-123"),
                Json::Null,
                json!("decimal128(6,2):0"),
            ],
        ),
        (
            json!({"Extension":{"id":"vortex.date","metadata":[4],"storage_dtype":{"Primitive":["i32",true]}}}),
            vec![json!(-7), Json::Null, json!(12)],
        ),
        (
            json!({"Extension":{"id":crate::native_embedding::ID,"metadata":br#"{"version":1}"#.as_slice(),
                "storage_dtype":{"FixedSizeList":[{"Primitive":["f64",true]},2,true]}}}),
            vec![json!([1.0, -0.0]), Json::Null, json!([null, 2.0])],
        ),
    ]
}

#[test]
fn typed_value_continuations_match_original_complete_output_sizes_and_errors() {
    for (dtype, values) in fixtures() {
        assert!(compare(&dtype, &values));
        assert!(compare(
            &json!({"List":[dtype.clone(),true]}),
            &[Json::Array(values.clone()), json!([]), Json::Null],
        ));
        assert!(compare(
            &json!({"FixedSizeList":[dtype.clone(),3,true]}),
            &[Json::Array(values.clone()), Json::Null],
        ));
        let record = json!({"Struct":[{"names":["x","tail"],"dtypes":[dtype.clone(),{"List":[dtype.clone(),true]}]},true]});
        assert!(compare(
            &record,
            &[
                json!({"x":values[0],"tail":values.clone()}),
                Json::Null,
                json!({"x":values[2],"tail":[]}),
            ],
        ));
        assert!(!compare(&record, &[json!({"x":values[0]})]));
        assert!(!compare(&record, &[json!({"x":values[0],"wrong":[]})]));
        assert!(!compare(
            &json!({"List":[dtype.clone(),false]}),
            &[json!({"wrong":1})],
        ));
        assert!(!compare(
            &json!({"FixedSizeList":[dtype,2,false]}),
            &[Json::Array(values)],
        ));
    }
    for kind in ["i8", "u8", "f16", "f32"] {
        for value in [
            json!(-257),
            json!(257),
            json!(1.1),
            json!("wrong"),
            Json::Null,
        ] {
            compare(&json!({"Primitive":[kind,false]}), &[value]);
        }
    }
    wide_case();
}

fn wide_case() {
    let names: Vec<String> = (0..257).map(|n| format!("field_{n}")).collect();
    let types: Vec<Json> = names
        .iter()
        .map(|_| json!({"Primitive":["i64",true]}))
        .collect();
    let values: serde_json::Map<String, Json> = names
        .iter()
        .enumerate()
        .map(|(n, name)| (name.clone(), json!(n)))
        .collect();
    assert!(compare(
        &json!({"Struct":[{"names":names,"dtypes":types},true]}),
        &[Json::Object(values), Json::Null],
    ));
}

fn chain(shape: &str, depth: usize, bad: bool) -> (DType, Json) {
    let mut dtype = DType::Primitive(PType::I32, Nullability::NonNullable);
    let mut value = if bad { json!("bad leaf") } else { json!(7) };
    for level in 0..depth {
        if shape == "list" || shape == "variable" || (shape == "mixed" && level % 2 == 0) {
            dtype = if shape == "variable" {
                DType::List(dtype.into(), Nullability::NonNullable)
            } else {
                DType::FixedSizeList(dtype.into(), 1, Nullability::NonNullable)
            };
            value = Json::Array(vec![value]);
        } else {
            let mut fields = vec![dtype];
            let mut names = vec!["x"];
            let mut object = serde_json::Map::new();
            object.insert("x".into(), value);
            if shape == "mixed" {
                fields.push(DType::Bool(Nullability::NonNullable));
                names.push("tail");
                object.insert("tail".into(), Json::Bool(true));
            }
            dtype = DType::Struct(
                StructFields::new(FieldNames::from(names), fields),
                Nullability::NonNullable,
            );
            value = Json::Object(object);
        }
    }
    (dtype, value)
}

// Test fixture cleanup only. Production JSON ownership remains a separate
// depth-bounded parser obligation and is not credited by these mechanism tests.
fn drop_fixture(mut value: Json) {
    loop {
        value = match value {
            Json::Array(mut values) => values.remove(0),
            Json::Object(mut values) => values.remove("x").unwrap(),
            _ => return,
        };
    }
}

fn inspect(mut array: ArrayRef, shape: &str, depth: usize, defaults: bool) {
    for level in (0..depth).rev() {
        assert_eq!(
            array.len(),
            usize::from(shape != "variable" || !defaults || level == depth - 1)
        );
        array = if shape == "variable" {
            array.as_::<List>().elements().clone()
        } else if shape == "list" || (shape == "mixed" && level % 2 == 0) {
            array.as_::<FixedSizeList>().elements().clone()
        } else {
            let array = array.as_::<Struct>();
            if shape == "mixed" {
                assert_eq!(
                    array
                        .unmasked_field(1)
                        .as_::<Bool>()
                        .to_bit_buffer()
                        .value(0),
                    !defaults
                );
            }
            array.unmasked_field(0).clone()
        };
    }
    let expected: &[i32] = if shape == "variable" && defaults {
        &[]
    } else if defaults {
        &[0]
    } else {
        &[7]
    };
    assert_eq!(array.as_::<Primitive>().as_slice::<i32>(), expected);
}

fn deep_child() {
    for shape in ["struct", "list", "variable", "mixed"] {
        let (dtype, value) = chain(shape, 8192, false);
        let memory = LiveMemoryPool::new(1 << 27).unwrap();
        assert!(crate::native_payload_schema::reserve_metadata(&dtype, &memory).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        for present in [true, false] {
            let cells = [present.then_some(&value)];
            let mut layout = super::Layout::new(&dtype, &memory).unwrap();
            layout.observe(&dtype, cells[0]).unwrap();
            let bytes = layout.bytes(&dtype).unwrap();
            let expected = if shape == "variable" {
                if present {
                    4 + 8192 * 16
                } else {
                    16 + 8191 * 8
                }
            } else {
                4 + if shape == "mixed" { 4096 } else { 0 }
            };
            assert_eq!(bytes, expected);
            assert_eq!(layout.copied().unwrap(), if present { 4 } else { 0 });
            drop(layout);
            let grant = memory.reserve(8192 * 4096).unwrap();
            let allocator = crate::owned_buffers::with_credit(allocator(&memory), grant);
            let array = super::construct::construct(&dtype, &cells, &memory, &allocator).unwrap();
            assert_eq!(array.dtype(), &dtype);
            assert_eq!(array.nbytes(), bytes as u64);
            inspect(array.clone(), shape, 8192, !present);
            drop(allocator);
            assert!(memory.snapshot().reserved_bytes >= 8192 * 4096);
            drop(array);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
        if shape == "mixed" {
            check_panic(&dtype, &value, &memory);
        }
        drop_fixture(value);
        drop(dtype);
    }
    let (dtype, value) = chain("mixed", 8192, true);
    let memory = LiveMemoryPool::new(1 << 27).unwrap();
    let mut layout = super::Layout::new(&dtype, &memory).unwrap();
    assert!(layout.observe(&dtype, Some(&value)).is_err());
    drop(layout);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(
        super::construct::construct(&dtype, &[Some(&value)], &memory, &allocator(&memory)).is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    drop_fixture(value);
    drop(dtype);
}

#[test]
fn typed_value_continuations_complete_deep_construction_and_cleanup_on_small_stack() {
    const CHILD: &str = "SHARDLOOM_TYPED_VALUE_DEEP_CHILD";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        println!("typed-value-deep-complete");
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "local_primitives::native_typed_input::tests::typed_value_continuations_complete_deep_construction_and_cleanup_on_small_stack", "--nocapture"])
        .env(CHILD, "1").output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("typed-value-deep-complete"));
}

#[test]
fn typed_value_continuations_refund_denied_construction_and_bound_default_growth() {
    let (dtype, value) = chain("mixed", 20, false);
    let measured = LiveMemoryPool::new(1 << 24).unwrap();
    drop(
        super::construct::construct(&dtype, &[Some(&value)], &measured, &allocator(&measured))
            .unwrap(),
    );
    assert_eq!(measured.snapshot().reserved_bytes, 0);
    let peak = measured.snapshot().peak_reserved_bytes;
    for bytes in [1, peak / 2, peak - 1] {
        let memory = LiveMemoryPool::new(bytes + 17).unwrap();
        let unrelated = memory.reserve(17).unwrap();
        assert!(
            super::construct::construct(&dtype, &[Some(&value)], &memory, &allocator(&memory))
                .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 17);
        drop(unrelated);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut dtype = DType::Primitive(PType::I32, Nullability::NonNullable);
    for _ in 0..3 {
        dtype = DType::FixedSizeList(dtype.into(), u32::MAX, Nullability::NonNullable);
    }
    let mut layout = super::Layout::new(&dtype, &memory).unwrap();
    let error = layout.observe(&dtype, None).unwrap_err().to_string();
    assert!(error.contains("typed input size overflow"));
    drop(layout);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn typed_value_layout_denial_refunds_each_phase_and_retains_scalar_inline_storage() {
    let (dtype, value) = chain("mixed", 20, false);
    let measured = LiveMemoryPool::new(1 << 20).unwrap();
    drop(super::Layout::new(&dtype, &measured).unwrap());
    let peak = measured.snapshot().peak_reserved_bytes;
    for bytes in [1, peak / 2, peak - 1] {
        let memory = LiveMemoryPool::new(bytes).unwrap();
        assert!(super::Layout::new(&dtype, &memory).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let mut layout = super::Layout::new(&dtype, &measured).unwrap();
    let baseline = measured.snapshot().reserved_bytes;
    let denial = measured.reserve((1 << 20) - baseline).unwrap();
    assert!(layout.observe(&dtype, Some(&value)).is_err());
    assert_eq!(measured.snapshot().reserved_bytes, 1 << 20);
    drop(denial);
    drop(layout);
    assert_eq!(measured.snapshot().reserved_bytes, 0);
    let mut layout = super::Layout::new(&dtype, &measured).unwrap();
    layout.observe(&dtype, Some(&value)).unwrap();
    let baseline = measured.snapshot().reserved_bytes;
    let denial = measured.reserve((1 << 20) - baseline).unwrap();
    assert!(layout.bytes(&dtype).is_err());
    assert_eq!(measured.snapshot().reserved_bytes, 1 << 20);
    drop(denial);
    assert_eq!(layout.bytes(&dtype).unwrap(), 14);
    assert_eq!(layout.copied().unwrap(), 4);
    drop(layout);
    assert_eq!(measured.snapshot().reserved_bytes, 0);
    let dtype = DType::Primitive(PType::I32, Nullability::NonNullable);
    let mut scalar = super::Layout::new(&dtype, &measured).unwrap();
    scalar.observe(&dtype, Some(&json!(7))).unwrap();
    assert_eq!(scalar.bytes(&dtype).unwrap(), 4);
    assert_eq!(scalar.copied().unwrap(), 4);
    assert_eq!(measured.snapshot().reserved_bytes, 0);
}
