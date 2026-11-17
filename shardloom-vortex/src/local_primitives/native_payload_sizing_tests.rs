use std::process::Command;

use super::{DType, Frame, LiveMemoryPool, Result, failed, gather_scratch, gather_scratch_per_row};
use vortex::array::dtype::{Nullability, PType, StructFields};

#[path = "native_payload_sizing_reference.rs"]
mod reference;

fn chain(shape: usize, depth: usize) -> DType {
    let mut dtype = DType::Primitive(PType::I32, Nullability::NonNullable);
    for index in 0..depth {
        dtype = match if shape == 3 { index % 3 } else { shape } {
            0 => DType::List(dtype.into(), Nullability::NonNullable),
            1 => DType::FixedSizeList(dtype.into(), 1, Nullability::NonNullable),
            _ => DType::Struct(
                StructFields::from_iter([("x", dtype)]),
                Nullability::NonNullable,
            ),
        };
    }
    dtype
}

#[test]
fn payload_sizing_continuations_match_original_arithmetic_and_limits() {
    let memory = LiveMemoryPool::new(1 << 24).unwrap();
    for shape in 0..4 {
        for depth in [0, 1, 24, 63, 64, 65] {
            let dtype = chain(shape, depth);
            assert_eq!(
                gather_scratch_per_row(&dtype, &memory).map_err(|e| e.to_string()),
                reference::gather_scratch_per_row(&dtype).map_err(|e| e.to_string())
            );
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
    for size in [0, 1, 2, u32::MAX] {
        let dtype = DType::FixedSizeList(
            DType::FixedSizeList(chain(0, 2).into(), u32::MAX, Nullability::Nullable).into(),
            size,
            Nullability::Nullable,
        );
        assert_eq!(
            gather_scratch_per_row(&dtype, &memory).map_err(|e| e.to_string()),
            reference::gather_scratch_per_row(&dtype).map_err(|e| e.to_string())
        );
    }
    for width in [0, 1, 257] {
        let dtype = DType::Struct(
            StructFields::from_iter((0..width).map(|n| (format!("f{n}"), chain(0, n % 3)))),
            Nullability::Nullable,
        );
        assert_eq!(
            gather_scratch_per_row(&dtype, &memory).unwrap(),
            reference::gather_scratch_per_row(&dtype).unwrap()
        );
    }
    let extension = vortex::array::extension::datetime::Timestamp::new(
        vortex::array::extension::datetime::TimeUnit::Nanoseconds,
        Nullability::Nullable,
    )
    .erased();
    assert_eq!(
        gather_scratch_per_row(&DType::Extension(extension), &memory).unwrap(),
        32
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn payload_sizing_continuations_admit_frames_and_refund_denial() {
    let memory = LiveMemoryPool::new(1).unwrap();
    assert_eq!(
        gather_scratch_per_row(&DType::Bool(Nullability::Nullable), &memory).unwrap(),
        32
    );
    assert!(
        gather_scratch_per_row(&chain(0, 1), &memory)
            .unwrap_err()
            .to_string()
            .contains("memory reservation denied")
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let memory = LiveMemoryPool::new((24 * std::mem::size_of::<Frame<'_>>() - 1) as u64).unwrap();
    assert!(
        gather_scratch_per_row(&chain(0, 16), &memory)
            .unwrap_err()
            .to_string()
            .contains("memory reservation denied")
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[test]
fn payload_sizing_continuations_complete_deep_schemas_on_small_stack() {
    const ENV: &str = "SHARDLOOM_PAYLOAD_SIZING_DEEP_CHILD";
    if std::env::var_os(ENV).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let memory = LiveMemoryPool::new(1 << 26).unwrap();
                for shape in 0..4 {
                    let dtype = chain(shape, 8192);
                    let expected = (0..8192).fold(32_u64, |bytes, index| {
                        bytes
                            + match if shape == 3 { index % 3 } else { shape } {
                                0 => 32 + 3 * std::mem::size_of::<Option<usize>>() as u64,
                                1 => 32 + 24 + 3 * std::mem::size_of::<Option<usize>>() as u64,
                                _ => 32,
                            }
                    });
                    assert_eq!(gather_scratch(&dtype, &memory, 8192).unwrap(), expected);
                    assert!(gather_scratch_per_row(&dtype, &memory).is_err());
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                    drop(dtype);
                }
                println!("PAYLOAD_SIZING_DEEP_COMPLETE");
            })
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap()).args([
        "--exact", "local_primitives::native_payload::sizing::tests::payload_sizing_continuations_complete_deep_schemas_on_small_stack", "--nocapture",
    ]).env(ENV, "1").output().unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("PAYLOAD_SIZING_DEEP_COMPLETE"));
}
