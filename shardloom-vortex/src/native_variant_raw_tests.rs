use super::*;
use parquet_variant::VariantBuilder;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};

fn observe(
    metadata: &[u8],
    bytes: &[u8],
    old: bool,
    cancel: usize,
    panic: bool,
) -> (std::result::Result<String, String>, usize) {
    let calls = Cell::new(0);
    let callback = || {
        let next = calls.get() + 1;
        calls.set(next);
        if next == cancel {
            assert!(!panic, "injected validation callback panic");
            return Err(failed("injected cancellation"));
        }
        Ok(())
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        if old {
            reference::value(metadata, bytes, &callback)
        } else {
            value(metadata, bytes, &callback)
        }
        .map(|value| format!("{value:?}"))
        .map_err(|error| error.to_string())
    }))
    .unwrap_or_else(|panic| {
        Err(format!(
            "panic: {}",
            panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown")
        ))
    });
    (result, calls.get())
}

fn chain(shape: usize, depth: usize, leaf: &[u8]) -> Vec<u8> {
    let object = |level| shape == 1 || (shape == 2 && level % 2 == 0);
    let mut widths = vec![0; depth];
    let mut length = leaf.len();
    for level in (0..depth).rev() {
        let width = if u8::try_from(length).is_ok() {
            1
        } else if u16::try_from(length).is_ok() {
            2
        } else {
            4
        };
        widths[level] = width;
        length += 2 + usize::from(object(level)) + 2 * width;
    }
    let mut bytes = Vec::with_capacity(length);
    for (level, width) in widths.into_iter().enumerate() {
        let is_object = object(level);
        length -= 2 + usize::from(is_object) + 2 * width;
        bytes.push((if is_object { 2 } else { 3 }) | (u8::try_from(width - 1).unwrap() << 2));
        bytes.push(1);
        if is_object {
            bytes.push(0);
        }
        bytes.extend_from_slice(&0_u32.to_le_bytes()[..width]);
        bytes.extend_from_slice(&u32::try_from(length).unwrap().to_le_bytes()[..width]);
    }
    bytes.extend_from_slice(leaf);
    bytes
}

fn corpus() -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut cases = Vec::new();
    for value in [
        Variant::Null,
        Variant::Int64(i64::MIN),
        Variant::from("text"),
        Variant::Binary(&[0, 255]),
        Variant::Double(f64::from_bits(0x7ff8_0000_0000_1234)),
    ] {
        cases.push(VariantBuilder::new().with_value(value).finish());
    }
    for shape in 0..3 {
        for depth in [0, 1, 2, 24, 25] {
            cases.push((vec![1, 1, 0, 1, b'x'], chain(shape, depth, &[0])));
        }
    }
    cases.extend([
        (vec![1, 0, 0], vec![]),
        (vec![1, 0, 0], vec![3]),
        (vec![1, 0, 0], vec![3, 1, 0, 1]),
        (vec![1, 0, 0], vec![3, 2, 0, 1, 3, 252, 0]),
        (
            vec![1, 2, 0, 1, 2, b'a', b'a'],
            vec![2, 2, 0, 1, 0, 1, 2, 0, 0],
        ),
        (
            vec![1, 2, 0, 1, 2, b'a', b'b'],
            vec![2, 2, 0, 1, 1, 0, 2, 0, 0],
        ),
        (vec![255], vec![3]),
    ]);
    // Repeated object offsets amplify a small byte tree. The existing shared
    // work counter must reject it before provider validation or name checks.
    let mut aliased = vec![0];
    for _ in 0..10 {
        let mut parent = vec![14, 2, 0, 1];
        parent.extend_from_slice(&[0; 8]);
        parent.extend_from_slice(&u32::try_from(aliased.len()).unwrap().to_le_bytes());
        parent.extend(aliased);
        aliased = parent;
    }
    let alias_metadata = vec![1, 2, 0, 1, 2, b'a', b'b'];
    assert!(
        value(&alias_metadata, &aliased, &|| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("validation work exceeds its encoded byte envelope")
    );
    cases.push((alias_metadata, aliased));
    let originals = cases.clone();
    for (metadata, value) in originals {
        for cut in 0..value.len().min(20) {
            cases.push((metadata.clone(), value[..cut].to_vec()));
        }
        for index in 0..value.len().min(20) {
            let mut bad = value.clone();
            bad[index] = 255;
            cases.push((metadata.clone(), bad));
        }
    }
    cases
}

#[test]
fn variant_raw_continuations_match_original_errors_values_and_callback_order() {
    for (metadata, bytes) in corpus() {
        let original = observe(&metadata, &bytes, true, usize::MAX, false);
        assert_eq!(
            observe(&metadata, &bytes, false, usize::MAX, false),
            original
        );
        for cancel in [1, 2, 3, original.1, original.1 + 1] {
            assert_eq!(
                observe(&metadata, &bytes, false, cancel, false),
                observe(&metadata, &bytes, true, cancel, false)
            );
        }
    }
    let bytes = chain(2, 4, &[0]);
    for cancel in [1, 4, 6, 9, 10] {
        assert_eq!(
            observe(&[1, 1, 0, 1, b'x'], &bytes, false, cancel, true),
            observe(&[1, 1, 0, 1, b'x'], &bytes, true, cancel, true)
        );
    }
}

fn inspect_deep(mut value: Variant<'_, '_>, depth: usize) {
    for _ in 0..depth {
        value = match value {
            Variant::List(list) => {
                assert_eq!(list.len(), 1);
                list.get(0).unwrap()
            }
            Variant::Object(object) => {
                assert_eq!(object.len(), 1);
                assert_eq!(object.field_name(0), Some("x"));
                object.field(0).unwrap()
            }
            _ => panic!("missing deep container"),
        };
    }
    assert!(matches!(value, Variant::Null));
}

fn deep_child() {
    let metadata = [1, 1, 0, 1, b'x'];
    for shape in 0..3 {
        let bytes = chain(shape, 8192, &[0]);
        // Also covers the later two-child wrapper and its malformed sibling.
        let allowance = workspace(bytes.len() + 15).unwrap();
        let memory = LiveMemoryPool::new(allowance).unwrap();
        {
            let _credit = memory.reserve(allowance).unwrap();
            let value = raw::value(&metadata, &bytes, &|| Ok(()), usize::MAX).unwrap();
            inspect_deep(value, 8192);
            for fail in [1, 4096, 8193, 8194, 12288, 16386] {
                let calls = Cell::new(0);
                let check = || {
                    calls.set(calls.get() + 1);
                    if calls.get() == fail {
                        return Err(failed("injected cancellation"));
                    }
                    Ok(())
                };
                assert!(raw::value(&metadata, &bytes, &check, usize::MAX).is_err());
                assert_eq!(calls.get(), fail);
            }
            let bad = chain(shape, 8192, &[252]);
            assert!(raw::value(&metadata, &bad, &|| Ok(()), usize::MAX).is_err());
            let mut late = vec![15, 2];
            for offset in [0, bytes.len(), bytes.len() + 1] {
                late.extend_from_slice(&u32::try_from(offset).unwrap().to_le_bytes());
            }
            late.extend_from_slice(&bytes);
            late.push(252);
            assert!(raw::value(&metadata, &late, &|| Ok(()), usize::MAX).is_err());
            let calls = Cell::new(0);
            let result = catch_unwind(AssertUnwindSafe(|| {
                raw::value(
                    &metadata,
                    &bytes,
                    &|| {
                        calls.set(calls.get() + 1);
                        assert!(calls.get() != 12288, "injected deep callback panic");
                        Ok(())
                    },
                    usize::MAX,
                )
            }));
            assert!(result.is_err());
            assert_eq!(calls.get(), 12288);
            assert_eq!(memory.snapshot().reserved_bytes, allowance);
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert!(
            LiveMemoryPool::new(allowance - 1)
                .unwrap()
                .reserve(allowance)
                .is_err()
        );
    }
}

#[test]
fn variant_raw_continuations_complete_deep_bytes_and_cancel_on_small_stack() {
    const CHILD: &str = "SHARDLOOM_DEEP_RAW_VARIANT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            concat!(
                module_path!(),
                "::variant_raw_continuations_complete_deep_bytes_and_cancel_on_small_stack"
            )
            .strip_prefix("shardloom_vortex::")
            .unwrap(),
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
