//! Complete original-wire parity and private deep-copy mechanism checks.

use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value as Json, json};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayContext, ArrayRef, Canonical, IntoArray, VortexSessionExecute as _,
        arrays::{
            Bool, BoolArray, ChunkedArray, ConstantArray, DictArray, FixedSizeList,
            FixedSizeListArray, List, ListArray, MaskedArray, Primitive, PrimitiveArray,
            SliceArray, Struct, StructArray, VarBinArray, VariantArray, bool::BoolArrayExt as _,
            fixed_size_list::FixedSizeListArraySlotsExt as _, list::ListArraySlotsExt as _,
            struct_::StructArrayExt as _,
        },
        dtype::{DType, FieldNames, Nullability},
        memory::{
            HostAllocator, HostAllocatorRef, HostMemoryReservation, HostMemorySessionExt as _,
            WritableHostBuffer,
        },
        optimizer::ArrayOptimizer as _,
        scalar::Scalar,
        serde::SerializeOptions,
        validity::Validity,
    },
    buffer::Alignment,
    encodings::parquet_variant::ParquetVariant,
    error::VortexResult,
};

use super::super::{CopyPolicy, CopyState};
#[cfg(debug_assertions)]
use crate::local_primitives::native_relational_observation;
use crate::{
    local_primitives::{native_spatial_input, native_typed_input, vortex_error},
    resident_session::ResidentVortexSession,
};

#[path = "native_payload_copy_reference.rs"]
mod reference;

#[derive(Debug, PartialEq)]
struct Output {
    schema: Vec<u8>,
    wire: Vec<u8>,
    encodings: Vec<String>,
    rows: usize,
    scalars: u64,
    retained_bytes: u64,
}

fn observe<const RETAIN: bool>(
    source: Option<&ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    policy: CopyPolicy,
    old: bool,
    counter: u64,
) -> Result<Output, String> {
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let count = Cell::new(counter);
            let copy = CopyState {
                policy,
                scalars: Some(&count),
                unobserved_fields: &[],
            };
            let metadata = crate::native_payload_schema::reserve_metadata(dtype, context)?;
            let allocator = crate::owned_buffers::with_credit(
                context.native_session().host_allocator(),
                metadata,
            );
            let array = if old {
                reference::copy_rows::<RETAIN>(source, rows, dtype, copy, context, &allocator)?
            } else {
                super::copy_rows::<RETAIN>(source, rows, dtype, copy, context, &allocator)?
            };
            drop(allocator);
            let retained_bytes = context.memory().snapshot().reserved_bytes;
            let array_context = ArrayContext::empty();
            let wire = array
                .serialize(
                    &array_context,
                    context.native_session(),
                    &SerializeOptions::default(),
                )
                .map_err(vortex_error)?
                .into_iter()
                .flat_map(IntoIterator::into_iter)
                .collect();
            Ok(Output {
                schema: serde_json::to_vec(array.dtype()).unwrap(),
                wire,
                encodings: array_context
                    .to_ids()
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                rows: array.len(),
                scalars: count.get(),
                retained_bytes,
            })
        })
        .map_err(|error| error.to_string());
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    output
}

fn compare<const RETAIN: bool>(
    source: Option<&ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    policy: CopyPolicy,
) -> bool {
    let expected = observe::<RETAIN>(source, rows, dtype, policy, true, 0);
    let actual = observe::<RETAIN>(source, rows, dtype, policy, false, 0);
    assert_eq!(
        actual, expected,
        "dtype={dtype} rows={rows:?} retained={RETAIN}"
    );
    actual.is_ok()
}

fn typed(dtype: &Json, values: &[Json]) -> ArrayRef {
    let memory = LiveMemoryPool::new(1 << 26).unwrap();
    let allocator: HostAllocatorRef = Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
        memory.clone(),
    ));
    let raw: Vec<String> = values.iter().map(Json::to_string).collect();
    let values: Vec<_> = raw.iter().map(|raw| Some(raw.as_str())).collect();
    native_typed_input::build(&dtype.to_string(), &values, &memory, &allocator, 1 << 20)
        .unwrap()
        .array
}

fn fixtures() -> Vec<ArrayRef> {
    let mut result = Vec::new();
    for (dtype, values) in [
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
            json!({"Extension":{"id":crate::native_embedding::ID,"metadata":br#"{"version":1}"#.as_slice(),"storage_dtype":{"FixedSizeList":[{"Primitive":["f64",true]},2,true]}}}),
            vec![json!([1.0, -0.0]), Json::Null, json!([null, 2.0])],
        ),
    ] {
        result.push(typed(&dtype, &values));
        result.push(typed(
            &json!({"List":[dtype,true]}),
            &[Json::Array(values.clone()), Json::Null, json!([])],
        ));
        result.push(typed(
            &json!({"FixedSizeList":[dtype,3,true]}),
            &[
                Json::Array(values.clone()),
                Json::Null,
                Json::Array(values.clone()),
            ],
        ));
        result.push(typed(
            &json!({"Struct":[{"names":["x","tail"],"dtypes":[dtype,{"List":[dtype,true]}]},true]}),
            &[
                json!({"x":values[0],"tail":values}),
                Json::Null,
                json!({"x":values[2],"tail":[]}),
            ],
        ));
    }
    result.push(
        StructArray::try_new(
            FieldNames::from_iter((0..257).map(|index| format!("field_{index}"))),
            (0_i64..257).map(|index| PrimitiveArray::from_iter([index, -1, 7]).into_array()),
            3,
            Validity::from_iter([true, false, true]),
        )
        .unwrap()
        .into_array(),
    );
    result
}

#[test]
fn payload_copy_continuations_match_original_complete_wire_counts_and_errors() {
    for array in fixtures() {
        for policy in [CopyPolicy::ValidateValues, CopyPolicy::PreserveUnobserved] {
            for rows in [
                vec![],
                vec![Some(0), Some(1), Some(2)],
                vec![Some(2), None, Some(0), Some(2)],
                vec![None, None],
            ] {
                let valid = rows
                    .iter()
                    .all(|row| row.is_none_or(|row| row < array.len()));
                assert_eq!(
                    compare::<true>(Some(&array), &rows, array.dtype(), policy),
                    valid
                );
                assert_eq!(
                    compare::<false>(Some(&array), &rows, array.dtype(), policy),
                    valid
                );
            }
            assert!(compare::<true>(None, &[None], array.dtype(), policy));
            assert!(!compare::<true>(None, &[Some(0)], array.dtype(), policy));
        }
        let expected = observe::<true>(
            Some(&array),
            &[Some(0)],
            array.dtype(),
            CopyPolicy::ValidateValues,
            true,
            u64::MAX,
        );
        assert_eq!(
            observe::<true>(
                Some(&array),
                &[Some(0)],
                array.dtype(),
                CopyPolicy::ValidateValues,
                false,
                u64::MAX
            ),
            expected
        );
    }
    for value in [
        f64::from_bits(0x7ff0_0000_0000_0001),
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.0,
    ] {
        let array = PrimitiveArray::from_iter([value]).into_array();
        compare::<true>(
            Some(&array),
            &[Some(0)],
            array.dtype(),
            CopyPolicy::ValidateValues,
        );
        assert!(compare::<true>(
            Some(&array),
            &[Some(0)],
            array.dtype(),
            CopyPolicy::PreserveUnobserved
        ));
    }
}

fn raw_variant(values: &[parquet_variant::Variant<'_, '_>]) -> ArrayRef {
    let (metadata, values): (Vec<_>, Vec<_>) = values
        .iter()
        .map(|value| {
            parquet_variant::VariantBuilder::new()
                .with_value(value.clone())
                .finish()
        })
        .unzip();
    ParquetVariant::try_new(
        Validity::AllValid,
        VarBinArray::from_vec(metadata, DType::Binary(Nullability::NonNullable)).into_array(),
        Some(VarBinArray::from_vec(values, DType::Binary(Nullability::NonNullable)).into_array()),
        None,
    )
    .unwrap()
    .into_array()
}

#[test]
#[allow(clippy::too_many_lines)] // One parity matrix keeps every wrapper under the same oracle.
fn payload_copy_continuations_match_original_variant_wrappers_and_shredding() {
    use parquet_variant::Variant as V;
    let raw = raw_variant(&[V::Int64(7), V::Null, V::from("tail")]);
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    let mut variants = vec![raw.clone()];
    variants.push(
        DictArray::try_new(
            PrimitiveArray::from_option_iter([Some(2_u64), None, Some(0)]).into_array(),
            raw.clone(),
        )
        .unwrap()
        .into_array(),
    );
    variants.push(SliceArray::try_new(raw.clone(), 1..3).unwrap().into_array());
    variants.push(
        MaskedArray::try_new(raw.clone(), Validity::AllInvalid, &mut execution)
            .unwrap()
            .into_array(),
    );
    variants.push(
        MaskedArray::try_new(
            raw.clone(),
            Validity::from_iter([true, false, true]),
            &mut execution,
        )
        .unwrap()
        .into_array(),
    );
    variants.push(
        ChunkedArray::try_new(vec![raw.clone(), raw.clone()], raw.dtype().clone())
            .unwrap()
            .into_array(),
    );
    variants.push(
        VariantArray::try_new(raw.clone(), None)
            .unwrap()
            .into_array(),
    );
    variants.push(ConstantArray::new(Scalar::null(raw.dtype().clone()), 3).into_array());
    variants
        .push(ConstantArray::new(raw.execute_scalar(1, &mut execution).unwrap(), 3).into_array());
    let shell = StructArray::try_new(
        FieldNames::from(["typed_value"]),
        vec![PrimitiveArray::from_iter([1_i64, -7]).into_array()],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let typed = StructArray::try_new(
        FieldNames::from(["score"]),
        vec![shell],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let (metadata, _) = parquet_variant::VariantBuilder::new()
        .with_field_names(["score"])
        .with_value(V::Null)
        .finish();
    let shredded = ParquetVariant::try_new(
        Validity::AllValid,
        VarBinArray::from_vec(vec![metadata; 2], DType::Binary(Nullability::NonNullable))
            .into_array(),
        None,
        Some(typed),
    )
    .unwrap()
    .into_array();
    variants.push(shredded.clone());
    variants.push(
        shredded
            .execute::<Canonical>(&mut execution)
            .unwrap()
            .into_array(),
    );
    for array in variants {
        for policy in [CopyPolicy::ValidateValues, CopyPolicy::PreserveUnobserved] {
            for rows in [
                vec![],
                vec![Some(array.len() - 1), None, Some(0), Some(array.len() - 1)],
                vec![None, None],
            ] {
                assert!(compare::<true>(Some(&array), &rows, array.dtype(), policy));
                assert!(compare::<false>(Some(&array), &rows, array.dtype(), policy));
            }
        }
    }
    let conflict = VariantArray::try_new(
        raw_variant(&[V::Int64(1)]),
        Some(PrimitiveArray::from_iter([2_i64]).into_array()),
    )
    .unwrap()
    .into_array();
    assert!(!compare::<true>(
        Some(&conflict),
        &[Some(0)],
        conflict.dtype(),
        CopyPolicy::ValidateValues
    ));
    for depth in [63, 64, 65] {
        let mut array = raw_variant(&[V::Int64(7)]);
        for _ in 0..depth {
            array = SliceArray::try_new(array, 0..1).unwrap().into_array();
        }
        // The native optimizer collapses these identity slice chains before
        // matching Variant storage. They prove complete original parity, not
        // that all constructed wrappers consume the copy driver's depth budget.
        assert!(compare::<true>(
            Some(&array),
            &[Some(0)],
            array.dtype(),
            CopyPolicy::ValidateValues
        ));
        assert!(compare::<false>(
            Some(&array),
            &[Some(0)],
            array.dtype(),
            CopyPolicy::ValidateValues
        ));
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let rows = [Some(0)];
                let allocator = context.native_session().host_allocator();
                let expected = reference::variant::copy::<true>(
                    Some(&raw),
                    &rows,
                    raw.dtype(),
                    CopyState::unmeasured(CopyPolicy::ValidateValues),
                    context,
                    &allocator,
                    depth,
                )
                .map(drop)
                .map_err(|error| error.to_string());
                let mut inputs = super::Inputs {
                    root: &rows,
                    temporary: super::ReservedVec::new(context.memory())?,
                };
                let visit = super::Visit {
                    source: Some(raw.clone()),
                    dtype: raw.dtype().clone(),
                    rows: None,
                    copy: CopyState::unmeasured(CopyPolicy::ValidateValues),
                    kind: super::Kind::Variant(depth),
                    allocator,
                };
                let actual = super::variant::enter::<true>(visit, depth, &mut inputs, context)
                    .map(drop)
                    .map_err(|error| error.to_string());
                assert_eq!(actual, expected);
                assert_eq!(actual.is_ok(), depth <= 64);
                Ok(())
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

fn kind(shape: usize, level: usize) -> usize {
    if shape == 3 { level % 3 } else { shape }
}

fn deep_array(shape: usize, depth: usize, invalid: bool) -> ArrayRef {
    let session = vortex::session::VortexSession::default();
    let mut execution = session.create_execution_ctx();
    let mut array = if invalid {
        PrimitiveArray::from_iter([f64::NAN]).into_array()
    } else {
        PrimitiveArray::from_iter([7_i32]).into_array()
    };
    for level in 0..depth {
        array = match kind(shape, level) {
            0 => ListArray::try_new(
                array,
                PrimitiveArray::from_iter([0_u64, 1]).into_array(),
                Validity::NonNullable,
                &mut execution,
            )
            .unwrap()
            .into_array(),
            1 => FixedSizeListArray::try_new(array, 1, Validity::NonNullable, 1)
                .unwrap()
                .into_array(),
            _ => StructArray::try_new(
                FieldNames::from(["x", "tail"]),
                vec![array, BoolArray::from_iter([true]).into_array()],
                1,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array(),
        };
        // Prepare each fixture layer while its children are already prepared.
        // This isolates the copier's stack behavior. The provider optimizer's
        // unprepared deep traversal remains a separate, recorded blocker.
        array = array.optimize_ctx(&session).unwrap();
    }
    array
}

fn inspect_defaults(
    mut array: ArrayRef,
    shape: usize,
    depth: usize,
    expected_rows: usize,
) -> ArrayRef {
    let mut rows = expected_rows;
    for level in (0..depth).rev() {
        assert_eq!(array.len(), rows);
        array = match kind(shape, level) {
            0 => {
                let list = array.as_::<List>();
                let offsets = list.offsets().as_::<Primitive>().to_buffer::<u64>();
                assert_eq!(offsets.as_slice(), vec![0; rows + 1].as_slice());
                rows = 0;
                list.elements().clone()
            }
            1 => array.as_::<FixedSizeList>().elements().clone(),
            _ => {
                let record = array.as_::<Struct>();
                assert_eq!(
                    record.names().iter().map(AsRef::as_ref).collect::<Vec<_>>(),
                    ["x", "tail"]
                );
                let tail = record.unmasked_field(1).as_::<Bool>();
                for index in 0..rows {
                    assert!(!tail.bit_buffer_view().value(index));
                }
                record.unmasked_field(0).clone()
            }
        };
    }
    let primitive = array.as_::<Primitive>().to_buffer::<i32>();
    assert_eq!(primitive.as_slice(), vec![0; rows].as_slice());
    array
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
            "intentional payload allocation panic"
        );
        Ok(allocated)
    }
    fn reserve_external(&self, len: usize) -> VortexResult<HostMemoryReservation> {
        self.inner.reserve_external(len)
    }
}

#[derive(Debug)]
struct CancellingAllocator {
    inner: HostAllocatorRef,
    token: CancellationToken,
}

impl HostAllocator for CancellingAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        let allocated = self.inner.allocate(len, alignment)?;
        self.token.cancel();
        Ok(allocated)
    }

    fn reserve_external(&self, len: usize) -> VortexResult<HostMemoryReservation> {
        self.inner.reserve_external(len)
    }
}

fn deep_child() {
    const DEPTH: usize = 8192;
    for shape in 0..4 {
        let source = deep_array(shape, DEPTH, false);
        let session = ResidentVortexSession::new(512 << 20, 1).unwrap();
        let retained = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let credit = context.memory().reserve((DEPTH as u64 + 1) * 2048)?;
                let allocator = crate::owned_buffers::with_credit(
                    context.native_session().host_allocator(),
                    credit,
                );
                let copied = super::copy_rows::<true>(
                    None,
                    &[None],
                    source.dtype(),
                    CopyState::unmeasured(CopyPolicy::ValidateValues),
                    context,
                    &allocator,
                )?;
                assert_eq!(copied.dtype(), source.dtype());
                let leaf = inspect_defaults(copied, shape, DEPTH, 1);
                let validated = super::copy_rows::<false>(
                    Some(&source),
                    &[Some(0)],
                    source.dtype(),
                    CopyState::unmeasured(CopyPolicy::ValidateValues),
                    context,
                    &allocator,
                )?;
                assert!(validated.is_empty());
                assert_eq!(validated.dtype(), source.dtype());
                drop(validated);
                if shape == 2 {
                    let before = context.memory().snapshot().reserved_bytes;
                    let panic_allocator: HostAllocatorRef = Arc::new(PanicAllocator {
                        inner: allocator.clone(),
                        remaining: AtomicUsize::new(DEPTH / 2),
                    });
                    assert!(
                        catch_unwind(AssertUnwindSafe(|| {
                            super::copy_rows::<true>(
                                None,
                                &[None],
                                source.dtype(),
                                CopyState::unmeasured(CopyPolicy::ValidateValues),
                                context,
                                &panic_allocator,
                            )
                            .unwrap();
                        }))
                        .is_err()
                    );
                    drop(panic_allocator);
                    assert_eq!(context.memory().snapshot().reserved_bytes, before);
                }
                Ok(leaf)
            })
            .unwrap();
        assert!(session.memory().snapshot().reserved_bytes > 0);
        drop(retained);
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        drop(source);
    }
    let invalid = deep_array(3, DEPTH, true);
    let late = StructArray::try_new(
        FieldNames::from(["complete", "bad"]),
        vec![
            deep_array(2, DEPTH, false),
            PrimitiveArray::from_iter([f64::NAN]).into_array(),
        ],
        1,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(512 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let baseline = context.memory().snapshot().reserved_bytes;
            for invalid in [&invalid, &late] {
                let error = super::copy_rows::<false>(
                    Some(invalid),
                    &[Some(0)],
                    invalid.dtype(),
                    CopyState::unmeasured(CopyPolicy::ValidateValues),
                    context,
                    &context.native_session().host_allocator(),
                )
                .unwrap_err();
                assert!(error.to_string().contains("finite"), "{error}");
                assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            }
            Ok(())
        })
        .unwrap();
    drop(invalid);
    drop(late);
    println!("PAYLOAD_COPY_DEEP_COMPLETE");
}

#[test]
fn payload_copy_continuations_complete_private_defaults_validation_and_cleanup_on_small_stack() {
    const ENV: &str = "SHARDLOOM_PAYLOAD_COPY_DEEP_CHILD";
    if std::env::var_os(ENV).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap()).args([
        "--exact", "local_primitives::native_payload::copying::tests::payload_copy_continuations_complete_private_defaults_validation_and_cleanup_on_small_stack", "--nocapture",
    ]).env(ENV,"1").output().unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("PAYLOAD_COPY_DEEP_COMPLETE"));
}

#[test]
fn payload_copy_continuations_refund_denial_and_preserve_cancellation() {
    let source = deep_array(2, 16, false);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let token = CancellationToken::default();
    let cancelled = session
        .with_native_execution_context(&token, |context| {
            let baseline = context.memory().snapshot().reserved_bytes;
            let hold = context.memory().reserve((1 << 20) - 1024)?;
            let before = context.memory().snapshot().reserved_bytes;
            let error = super::copy_rows::<true>(
                None,
                &[None],
                source.dtype(),
                CopyState::unmeasured(CopyPolicy::ValidateValues),
                context,
                &context.native_session().host_allocator(),
            )
            .unwrap_err();
            assert!(error.to_string().contains("reservation denied"));
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            drop(hold);
            let allocator: HostAllocatorRef = Arc::new(CancellingAllocator {
                inner: context.native_session().host_allocator(),
                token: token.clone(),
            });
            let error = super::copy_rows::<true>(
                None,
                &[None],
                source.dtype(),
                CopyState::unmeasured(CopyPolicy::ValidateValues),
                context,
                &allocator,
            )
            .unwrap_err();
            assert!(error.to_string().contains("cancelled"), "{error}");
            drop(allocator);
            assert!(
                super::copy_rows::<true>(
                    None,
                    &[None],
                    source.dtype(),
                    CopyState::unmeasured(CopyPolicy::ValidateValues),
                    context,
                    &context.native_session().host_allocator()
                )
                .is_err()
            );
            assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            Ok(())
        })
        .unwrap_err();
    assert!(cancelled.to_string().contains("cancelled"));
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
