//! Actual native operation ownership; provider reuse never stores row results.

use super::*;
use vortex::{
    array::{
        IntoArray as _,
        arrays::{BoolArray, ConstantArray, PrimitiveArray, VarBinArray, bool::BoolArrayExt as _},
        dtype::Nullability,
        scalar_fn::fns::like::{LikeKernel, LikeOptions},
    },
    buffer::Buffer,
    encodings::fsst::FSST,
};

fn fixture(values: &[Option<&str>]) -> ArrayRef {
    let codes = values
        .iter()
        .map(|value| {
            value.map(|value| {
                value
                    .bytes()
                    .flat_map(|byte| [255, byte])
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    FSST::try_new(
        DType::Utf8(Nullability::Nullable),
        Buffer::empty(),
        Buffer::empty(),
        VarBinArray::from_iter(
            codes.iter().map(Option::as_deref),
            DType::Binary(Nullability::Nullable),
        ),
        PrimitiveArray::from_iter(
            values
                .iter()
                .map(|value| u32::try_from(value.map_or(0, str::len)).unwrap()),
        )
        .into_array(),
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array()
}

fn evaluate(
    context: &NativeExecutionContext<'_>,
    array: &ArrayRef,
    pattern: &str,
) -> Result<Vec<Option<bool>>> {
    context.enable_fsst_matcher_reuse();
    let mut ctx = context.native_session().create_execution_ctx();
    let output = <FSST as LikeKernel>::like(
        array.as_opt::<FSST>().unwrap(),
        &ConstantArray::new(pattern, array.len()).into_array(),
        LikeOptions::default(),
        &mut ctx,
    )
    .map_err(native_error)?
    .unwrap();
    let output = output
        .execute::<BoolArray>(&mut ctx)
        .map_err(native_error)?;
    let validity = output
        .validity()
        .map_err(native_error)?
        .execute_mask(array.len(), &mut ctx)
        .map_err(native_error)?;
    let bits = output.to_bit_buffer();
    Ok((0..array.len())
        .map(|row| validity.value(row).then(|| bits.value(row)))
        .collect())
}

fn proof(context: &NativeExecutionContext<'_>) -> String {
    let mut proof = String::new();
    context.append_fsst_matcher_proof(&mut proof);
    proof
}

fn field(proof: &str, name: &str) -> u64 {
    let key = format!("native_fsst_matcher_{name}=");
    proof
        .split_whitespace()
        .find_map(|part| part.strip_prefix(&key))
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn native_fsst_matcher_successive_operations_start_empty_and_preserve_complete_values() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let first = fixture(&[Some("alpha"), Some("雪"), None, Some("")]);
    let second = fixture(&[Some("beta"), Some("xy"), Some("éa"), None]);
    for _ in 0..2 {
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                assert_eq!(
                    evaluate(context, &first, "%a%")?,
                    [Some(true), Some(false), None, Some(false)]
                );
                assert_eq!(
                    evaluate(context, &second, "%a%")?,
                    [Some(true), Some(false), Some(true), None]
                );
                assert_eq!(
                    evaluate(context, &second, "%é%")?,
                    [Some(false), Some(false), Some(true), None]
                );
                let work = proof(context);
                assert_eq!(field(&work, "lookups"), 3);
                assert_eq!(field(&work, "hits"), 1);
                assert_eq!(field(&work, "misses"), 2);
                assert_eq!(field(&work, "admitted_constructions"), 1);
                assert_eq!(field(&work, "denied"), 0);
                assert_eq!(field(&work, "retained_capacity_bytes"), 1024);
                Ok(())
            })
            .unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn native_fsst_matcher_errors_and_cancellation_refund_before_the_next_operation() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let array = fixture(&[Some("a"), Some("b")]);
    for cancel in [false, true] {
        let cancellation = CancellationToken::default();
        let error = session
            .with_native_execution_context(&cancellation, |context| {
                assert_eq!(evaluate(context, &array, "%a%")?, [Some(true), Some(false)]);
                assert_eq!(field(&proof(context), "admitted_constructions"), 1);
                if cancel {
                    cancellation.cancel();
                    context.check_cancelled()?;
                }
                Err::<(), _>(resident_error("controlled post-matcher failure"))
            })
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancel { "cancel" } else { "controlled" })
        );
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                context.enable_fsst_matcher_reuse();
                assert_eq!(field(&proof(context), "lookups"), 0);
                assert_eq!(evaluate(context, &array, "%a%")?, [Some(true), Some(false)]);
                assert_eq!(field(&proof(context), "hits"), 0);
                assert_eq!(field(&proof(context), "admitted_constructions"), 1);
                Ok(())
            })
            .unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn native_fsst_matcher_scope_and_table_denials_preserve_existing_provider_execution() {
    let array = fixture(&[Some("a"), Some("b")]);
    for limit in [1, 512, 5_000] {
        let session = ResidentVortexSession::new(limit, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                assert_eq!(evaluate(context, &array, "%a%")?, [Some(true), Some(false)]);
                let work = proof(context);
                if limit == 5_000 {
                    assert_eq!(field(&work, "admitted_constructions"), 0);
                    assert_eq!(field(&work, "denied"), 1);
                    assert_eq!(field(&work, "retained_capacity_bytes"), 0);
                } else {
                    assert!(work.contains("native_fsst_matcher_scope=unavailable"));
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        assert!(session.snapshot().memory.peak_reserved_bytes <= limit);
    }
}

#[test]
fn native_fsst_matcher_panic_unwind_refunds_while_admission_keeps_its_poison_contract() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let array = fixture(&[Some("a")]);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _execution: Result<()> =
            session.with_native_execution_context(&CancellationToken::default(), |context| {
                assert_eq!(evaluate(context, &array, "%a%")?, [Some(true)]);
                panic!("controlled native matcher unwind");
            });
    }));
    assert!(result.is_err());
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    assert!(
        session
            .with_native_execution_context(&CancellationToken::default(), |_| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("admission poisoned")
    );
}

#[test]
fn native_fsst_matcher_metadata_and_concurrent_serving_never_install_reuse() {
    let exclusive = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let metadata = exclusive
        .0
        .enter(CallClass::Metadata, CancellationToken::default())
        .unwrap();
    metadata.enable_fsst_matcher_reuse();
    assert!(proof(&metadata).contains("native_fsst_matcher_scope=unavailable"));
    drop(metadata);
    assert_eq!(exclusive.snapshot().memory.reserved_bytes, 0);
    let session = ResidentVortexSession::with_serving_policy(
        8 << 20,
        2,
        ResidentServingPolicy {
            reserve_metadata_lane: false,
            ..Default::default()
        },
    )
    .unwrap();
    let baseline = session.snapshot().memory.reserved_bytes;
    let array = fixture(&[Some("a"), Some("b"), None]);
    let entered = AtomicUsize::new(0);
    let simultaneous = std::thread::available_parallelism().unwrap().get() >= 2;
    std::thread::scope(|threads| {
        let mut calls = Vec::new();
        for _ in 0..2 {
            calls.push(threads.spawn(|| {
                session.with_native_execution_context(&CancellationToken::default(), |context| {
                    entered.fetch_add(1, Ordering::SeqCst);
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                    while simultaneous && entered.load(Ordering::SeqCst) < 2 {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "serving calls must overlap"
                        );
                        std::thread::yield_now();
                    }
                    assert_eq!(
                        evaluate(context, &array, "%a%")?,
                        [Some(true), Some(false), None]
                    );
                    assert!(proof(context).contains("native_fsst_matcher_scope=unavailable"));
                    Ok(())
                })
            }));
        }
        for call in calls {
            call.join().unwrap().unwrap();
        }
    });
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(session.admission_snapshot().unwrap().active_calls, 0);
    if simultaneous {
        assert_eq!(session.admission_snapshot().unwrap().peak_active_calls, 2);
    }
}
