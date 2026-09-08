use super::*;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        arrays::{DictArray, PrimitiveArray, StructArray},
        dtype::FieldNames,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{runtime::single::SingleThreadRuntime, session::RuntimeSessionExt as _},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-artifact-compare-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn text_value(index: usize) -> Option<&'static str> {
    [
        Some("東京 / café / 🦊"),
        None,
        Some(""),
        Some("different long unicode string λ"),
    ][index % 4]
}

fn values(rows: Range<usize>, dictionary: bool, mismatch: Option<usize>) -> ArrayRef {
    let signed = PrimitiveArray::from_option_iter(rows.clone().map(|row| {
        (row % 7 != 3).then_some(
            (1_i64 << 60) + i64::try_from(row).unwrap() + i64::from(mismatch == Some(row)),
        )
    }))
    .into_array();
    let unsigned = PrimitiveArray::from_option_iter(
        rows.clone()
            .map(|row| (row % 5 != 2).then_some(u64::MAX - u64::try_from(row).unwrap())),
    )
    .into_array();
    let text = if dictionary {
        let dictionary = VarBinViewArray::from_iter_nullable_str([
            Some("different long unicode string λ"),
            Some(""),
            None,
            Some("東京 / café / 🦊"),
        ])
        .into_array();
        let codes = PrimitiveArray::new(
            rows.clone()
                .map(|row| [3_u8, 2, 1, 0][row % 4])
                .collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array();
        DictArray::try_new(codes, dictionary).unwrap().into_array()
    } else {
        VarBinViewArray::from_iter_nullable_str(rows.clone().map(text_value)).into_array()
    };
    let floats = PrimitiveArray::from_option_iter(rows.clone().map(|row| {
        (row % 6 != 4).then_some(match row % 4 {
            0 => -0.0_f64,
            1 => f64::from_bits(0x7ff8_0000_0000_0123),
            2 => f64::INFINITY,
            _ => -123.5,
        })
    }))
    .into_array();
    StructArray::try_new(
        FieldNames::from(["exact_signed", "exact_unsigned", "文字", "float_bits"]),
        vec![signed, unsigned, text, floats],
        rows.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn write(path: &Path, rows: usize, chunk: usize, dictionary: bool, mismatch: Option<usize>) {
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let dtype = values(0..rows.min(1), dictionary, mismatch).dtype().clone();
    let mut output = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(
                rows.div_ceil(chunk),
            ),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, dtype);
    for start in (0..rows).step_by(chunk) {
        writer
            .push(values(
                start..(start + chunk).min(rows),
                dictionary,
                mismatch,
            ))
            .unwrap();
    }
    assert_eq!(writer.finish().unwrap().row_count(), rows as u64);
}

fn limits() -> NativeArtifactComparisonLimits {
    NativeArtifactComparisonLimits {
        memory_bytes_per_source: 32 << 20,
        parallelism_per_source: 2,
        left_batch_rows: 3,
        right_batch_rows: 5,
        window_rows: 11,
        arrow_batch_bytes: 1 << 20,
        max_value_bytes: 4096,
        max_columns: 8,
    }
}

#[test]
fn complete_comparison_ignores_dictionary_codes_chunk_boundaries_and_null_payloads() {
    let fixture = Fixture::new();
    let left = fixture.path("left.vortex");
    let right = fixture.path("right.vortex");
    write(&left, 29, 4, false, None);
    write(&right, 29, 7, true, None);
    assert_ne!(fs::read(&left).unwrap(), fs::read(&right).unwrap());
    let report = compare_native_artifacts(&left, &right, limits()).unwrap();
    assert_eq!(report["complete_value_equality"], true);
    assert_eq!(report["compared_rows"], 29);
    assert_eq!(report["compared_columns"], 4);
    assert_eq!(report["compared_values"], 116);
    assert_ne!(
        report["left_native_batches"],
        report["right_native_batches"]
    );
    assert_eq!(report["left_prepared_source_opens"], 1);
    assert_eq!(report["right_prepared_source_opens"], 1);
    assert_eq!(report["left_completed_executions"], 1);
    assert_eq!(report["right_completed_executions"], 1);
    assert_eq!(report["source_generations_validated"], true);
    assert_ne!(report["left"]["inode"], report["right"]["inode"]);
    assert_eq!(report["memory"]["arrow_final_reserved_bytes"], 0);
    for side in ["left_final_reserved_bytes", "right_final_reserved_bytes"] {
        assert_eq!(report["memory"][side], 0);
    }
}

#[test]
fn complete_comparison_detects_exact_integer_mismatch_beyond_f64_precision() {
    let fixture = Fixture::new();
    let left = fixture.path("left.vortex");
    let right = fixture.path("right.vortex");
    write(&left, 29, 4, false, None);
    write(&right, 29, 7, true, Some(23));
    let error = compare_native_artifacts(&left, &right, limits())
        .unwrap_err()
        .to_string();
    assert!(error.contains("column 'exact_signed' row 23"), "{error}");
}

#[test]
fn complete_comparison_native_task_concurrency_is_bounded_without_host_multiplier() {
    let fixture = Fixture::new();
    let left = fixture.path("left.vortex");
    let right = fixture.path("right.vortex");
    write(&left, 97, 4, false, None);
    write(&right, 97, 7, true, None);
    for parallelism in [1, 2, 4] {
        let mut policy = limits();
        policy.parallelism_per_source = parallelism;
        policy.window_rows = 97;
        let report = compare_native_artifacts(&left, &right, policy).unwrap();
        assert_eq!(report["compared_rows"], 97);
        assert_eq!(report["compared_values"], 388);
        let limit = report["native_task_concurrency_per_source"]
            .as_u64()
            .unwrap();
        assert!(limit > 0 && limit <= parallelism as u64);
        for key in [
            "left_peak_native_split_tasks",
            "right_peak_native_split_tasks",
        ] {
            let observed = report[key].as_u64().unwrap();
            assert!(observed > 0 && observed <= limit, "{key}: {report}");
        }
        assert_eq!(report["native_split_tasks_after_drain"], 0);
        assert!(
            report["native_scan_scheduler"]
                .as_str()
                .unwrap()
                .contains("no_host_core_multiplier")
        );
        assert_eq!(report["memory"]["arrow_final_reserved_bytes"], 0);
    }
}

#[test]
fn complete_comparison_rejects_replaced_source_after_equal_spans() {
    for replace_left in [false, true] {
        let fixture = Fixture::new();
        let left = fixture.path("left.vortex");
        let right = fixture.path("right.vortex");
        let replacement = fixture.path("replacement.vortex");
        write(&left, 29, 4, false, None);
        write(&right, 29, 7, true, None);
        let target = if replace_left {
            left.clone()
        } else {
            right.clone()
        };
        fs::copy(&target, &replacement).unwrap();
        AFTER_EQUAL_SPAN.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::rename(replacement, target).unwrap();
            }));
        });
        let error = compare_native_artifacts(&left, &right, limits())
            .unwrap_err()
            .to_string();
        assert!(error.contains("prepared source changed"), "{error}");
        assert!(AFTER_EQUAL_SPAN.with(|hook| hook.borrow().is_none()));
    }
}

#[test]
fn complete_comparison_handles_actual_empty_sources_and_rejects_row_and_budget_mismatch() {
    let fixture = Fixture::new();
    let left = fixture.path("left.vortex");
    let right = fixture.path("right.vortex");
    write(&left, 0, 4, false, None);
    write(&right, 0, 7, true, None);
    let report = compare_native_artifacts(&left, &right, limits()).unwrap();
    assert_eq!(report["compared_rows"], 0);
    assert_eq!(report["compared_columns"], 4);
    assert_eq!(report["compared_values"], 0);
    let nonempty = fixture.path("nonempty.vortex");
    write(&nonempty, 5, 3, false, None);
    assert!(
        compare_native_artifacts(&left, &nonempty, limits())
            .unwrap_err()
            .to_string()
            .contains("row counts differ")
    );
    let mut small = limits();
    small.arrow_batch_bytes = 16;
    assert!(
        compare_native_artifacts(&nonempty, &nonempty, small)
            .unwrap_err()
            .to_string()
            .contains("Arrow expansion admission")
    );
    small = limits();
    small.max_columns = 1;
    assert!(
        compare_native_artifacts(&nonempty, &nonempty, small)
            .unwrap_err()
            .to_string()
            .contains("column count")
    );
}

#[test]
fn complete_comparison_preserves_float_bits_and_rejects_schema_changes() {
    let fixture = Fixture::new();
    let write_single = |path: &Path, name: &str, value: f64| {
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let array = StructArray::try_new(
            FieldNames::from([name]),
            vec![PrimitiveArray::new(vec![value], Validity::NonNullable).into_array()],
            1,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let mut output = fs::File::create(path).unwrap();
        let mut writer = session
            .write_options()
            .blocking(&runtime)
            .writer(&mut output, array.dtype().clone());
        writer.push(array).unwrap();
        writer.finish().unwrap();
    };
    let left = fixture.path("negative-zero.vortex");
    let right = fixture.path("positive-zero.vortex");
    write_single(&left, "f", -0.0);
    write_single(&right, "f", 0.0);
    assert!(
        compare_native_artifacts(&left, &right, limits())
            .unwrap_err()
            .to_string()
            .contains("value mismatch at column 'f' row 0")
    );
    write_single(&right, "renamed", -0.0);
    assert!(
        compare_native_artifacts(&left, &right, limits())
            .unwrap_err()
            .to_string()
            .contains("native schemas differ")
    );
}
