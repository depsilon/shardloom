//! Independent shallow writer parity and admitted deep-view mechanism checks.

use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{Column, Tree, build, write};
// The frozen reference's original `super` paths resolve through these aliases.
use super::super::{
    collect, native_list, native_payload, native_relational_batch, result_batch, vortex_error,
};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, ExecutionCtx, IntoArray as _, VortexSessionExecute as _,
        arrays::{
            BoolArray, ConstantArray, ExtensionArray, FixedSizeListArray, ListArray,
            PrimitiveArray, StructArray, VarBinArray,
        },
        dtype::{DType, FieldNames, Nullability, extension::ExtDType},
        extension::datetime::{Date, TimeUnit},
        scalar::Scalar,
        validity::Validity,
    },
    buffer::buffer,
    encodings::uuid::{Uuid, UuidMetadata},
    session::VortexSession,
};

// Byte-exact native_json.rs from b83306a32. Production never selects it.
#[path = "native_json_reference.rs"]
mod reference;

fn session() -> VortexSession {
    let session = VortexSession::default();
    crate::native_extensions::register(&session);
    session
}

fn fixtures(session: &VortexSession) -> Vec<ArrayRef> {
    let text = VarBinArray::from_iter(
        [Some("é\"\n\\"), None, Some("")],
        DType::Utf8(Nullability::Nullable),
    )
    .into_array();
    let boolean = BoolArray::from_iter([Some(true), None, Some(false)]).into_array();
    let number =
        PrimitiveArray::from_option_iter([Some(i64::MIN), None, Some(i64::MAX)]).into_array();
    let record = StructArray::try_new(
        FieldNames::from(["t\"", "b", "n"]),
        vec![text.clone(), boolean.clone(), number.clone()],
        3,
        Validity::from_iter([true, false, true]),
    )
    .unwrap()
    .into_array();
    let fixed = FixedSizeListArray::try_new(record.clone(), 1, Validity::NonNullable, 3)
        .unwrap()
        .into_array();
    let list = ListArray::try_new(
        record.clone(),
        buffer![0_u64, 2, 2, 3].into_array(),
        Validity::from_iter([true, true, false]),
        &mut session.create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let empty = StructArray::try_new(
        FieldNames::from(Vec::<&str>::new()),
        vec![],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let date = ExtensionArray::new(
        Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
        PrimitiveArray::from_option_iter([Some(-3_i32), None, Some(7)]).into_array(),
    )
    .into_array();
    let variant = ConstantArray::new(Scalar::variant("variant é".into()), 3).into_array();
    let null =
        ConstantArray::new(Scalar::null(DType::Variant(Nullability::Nullable)), 3).into_array();
    let mut values = vec![
        text, boolean, number, record, fixed, list, empty, date, variant, null,
    ];
    values.extend(extension_fixtures());
    values
}

fn extension_fixtures() -> [ArrayRef; 2] {
    let storage = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter(0_u8..48).into_array(),
        16,
        Validity::from_iter([true, false, true]),
        3,
    )
    .unwrap()
    .into_array();
    let dtype =
        ExtDType::try_with_vtable(Uuid, UuidMetadata::default(), storage.dtype().clone()).unwrap();
    let uuid = ExtensionArray::new(dtype.erased(), storage).into_array();
    let storage = FixedSizeListArray::try_new(
        PrimitiveArray::from_option_iter([
            Some(-1.5_f64),
            Some(0.0),
            None,
            Some(1.0),
            Some(2.5),
            None,
        ])
        .into_array(),
        2,
        Validity::from_iter([true, false, true]),
        3,
    )
    .unwrap()
    .into_array();
    let dtype = ExtDType::<crate::native_embedding::Embedding>::try_new(
        crate::native_embedding::Metadata::parse(r#"{"version":1}"#).unwrap(),
        storage.dtype().clone(),
    )
    .unwrap();
    [
        uuid,
        ExtensionArray::new(dtype.erased(), storage).into_array(),
    ]
}

struct Sink {
    bytes: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    remaining: usize,
    cancel_at: Option<(usize, CancellationToken)>,
}

impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.chunks.push(bytes.to_vec());
        if let Some((count, token)) = &self.cancel_at
            && self.chunks.len() == *count
        {
            token.cancel();
        }
        if self.remaining == 0 {
            return Err(std::io::Error::other("terminal JSON test sink full"));
        }
        let count = self.remaining.min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..count]);
        self.remaining -= count;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
struct Outcome {
    bytes: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    result: std::result::Result<(u64, u64), String>,
}

fn observe(
    array: &ArrayRef,
    row: usize,
    remaining: usize,
    cancel_at: Option<usize>,
    old: bool,
) -> Outcome {
    let session = session();
    let mut execution = session.create_execution_ctx();
    let memory = LiveMemoryPool::new(1 << 24).unwrap();
    let token = CancellationToken::default();
    let mut sink = Sink {
        bytes: Vec::new(),
        chunks: Vec::new(),
        remaining,
        cancel_at: cancel_at.map(|count| (count, token.clone())),
    };
    let result = if old {
        reference::Column::new(array, &mut execution, &memory, &token)
            .and_then(|column| column.write(row, &mut sink, &mut execution, &token))
            .map(|counts| (counts.scalars, counts.utf8_bytes))
    } else {
        Column::new(array, &mut execution, &memory, &token)
            .and_then(|column| column.write(row, &mut sink, &mut execution, &token))
            .map(|counts| (counts.scalars, counts.utf8_bytes))
    }
    .map_err(|error| error.to_string());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    Outcome {
        bytes: sink.bytes,
        chunks: sink.chunks,
        result,
    }
}

#[test]
fn terminal_json_preserves_complete_values_counts_and_every_failed_write() {
    let session = session();
    for (fixture, array) in fixtures(&session).into_iter().enumerate() {
        for row in 0..array.len() {
            let expected = observe(&array, row, usize::MAX, None, true);
            let empty_struct =
                matches!(array.dtype(), DType::Struct(fields, _) if fields.nfields() == 0);
            assert_eq!(
                expected.result.is_err(),
                empty_struct,
                "fixture {fixture}, row {row}: {expected:?}"
            );
            for remaining in 0..=expected.bytes.len() + 1 {
                assert_eq!(
                    observe(&array, row, remaining, None, false),
                    observe(&array, row, remaining, None, true),
                    "fixture {fixture}, row {row}, write boundary {remaining}"
                );
            }
            for call in 1..=expected.chunks.len() + 1 {
                assert_eq!(
                    observe(&array, row, usize::MAX, Some(call), false),
                    observe(&array, row, usize::MAX, Some(call), true),
                    "fixture {fixture}, row {row}, cancellation call {call}"
                );
            }
        }
    }
}

fn chain(shape: &str, depth: usize) -> ArrayRef {
    let mut array = buffer![7_i32].into_array();
    for level in 0..depth {
        array = if shape == "list" || (shape == "mixed" && level % 2 == 0) {
            FixedSizeListArray::try_new(array, 1, Validity::NonNullable, 1)
                .unwrap()
                .into_array()
        } else if shape == "mixed" {
            StructArray::try_new(
                FieldNames::from(["x", "tail"]),
                vec![array, BoolArray::from_iter([true]).into_array()],
                1,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        } else {
            StructArray::try_new(
                FieldNames::from(["x"]),
                vec![array],
                1,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        };
    }
    array
}

fn expected_chain(shape: &str, depth: usize) -> (Vec<u8>, u64) {
    let mut bytes = Vec::new();
    let mut scalars = 1;
    for level in (0..depth).rev() {
        bytes.extend_from_slice(if shape == "list" || (shape == "mixed" && level % 2 == 0) {
            b"["
        } else {
            b"{\"x\":"
        });
    }
    bytes.push(b'7');
    for level in 0..depth {
        if shape == "list" || (shape == "mixed" && level % 2 == 0) {
            bytes.push(b']');
        } else if shape == "mixed" {
            bytes.extend_from_slice(b",\"tail\":true}");
            scalars += 1;
        } else {
            bytes.push(b'}');
        }
    }
    (bytes, scalars)
}

fn deep_interruption(
    tree: &Tree,
    execution: &mut ExecutionCtx,
    memory: &LiveMemoryPool,
    expected: &[u8],
) {
    let baseline = memory.snapshot().reserved_bytes;
    let leaf = expected.iter().position(|&byte| byte == b'7').unwrap();
    let mut failed = Sink {
        bytes: Vec::new(),
        chunks: Vec::new(),
        remaining: leaf,
        cancel_at: None,
    };
    assert!(
        write::write(
            tree,
            0,
            &mut failed,
            execution,
            &CancellationToken::default()
        )
        .is_err()
    );
    assert_eq!(failed.bytes, expected[..leaf]);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    // The failed attempt includes the scalar write. Cancel at the preceding
    // completed delimiter so the next node-entry check stops before that leaf.
    let before_leaf = failed.chunks.len() - 1;
    let token = CancellationToken::default();
    let mut cancelled = Sink {
        bytes: Vec::new(),
        chunks: Vec::new(),
        remaining: usize::MAX,
        cancel_at: Some((before_leaf, token.clone())),
    };
    assert!(write::write(tree, 0, &mut cancelled, execution, &token).is_err());
    assert_eq!(cancelled.bytes, expected[..leaf]);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
}

fn deep_child() {
    let session = session();
    for shape in ["struct", "list", "mixed"] {
        let array = chain(shape, 8192);
        let memory = LiveMemoryPool::new(1 << 26).unwrap();
        let token = CancellationToken::default();
        let mut execution = session.create_execution_ctx();
        assert!(
            Column::new(&array, &mut execution, &memory, &token).is_err(),
            "private mechanism proof must not change public depth24 admission"
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        // Test-only explicit model allowance. Public schema admission remains
        // bounded; this proves the new reserved view containers and traversal.
        let model = memory.reserve(8192 * 4096).unwrap();
        let tree = build::build(&array, &mut execution, &memory, &token).unwrap();
        let (expected, scalars) = expected_chain(shape, 8192);
        deep_interruption(&tree, &mut execution, &memory, &expected);
        let mut actual = Vec::new();
        let counts = write::write(&tree, 0, &mut actual, &mut execution, &token).unwrap();
        assert_eq!(actual, expected, "{shape}");
        assert_eq!(counts.scalars, scalars);
        assert_eq!(counts.utf8_bytes, 0);
        drop(array);
        drop(tree);
        assert_eq!(memory.snapshot().reserved_bytes, model.bytes());
        drop(model);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn terminal_json_deep_views_write_and_release_on_a_small_stack() {
    const CHILD: &str = "SHARDLOOM_TERMINAL_JSON_DEEP_CHILD";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        println!("terminal-json-deep-complete");
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "local_primitives::native_json::tests::terminal_json_deep_views_write_and_release_on_a_small_stack", "--nocapture"])
        .env(CHILD, "1").output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("terminal-json-deep-complete"));
}

#[test]
fn terminal_json_wide_siblings_preserve_reference_order_and_counts() {
    let names: Vec<Arc<str>> = (0..257)
        .map(|index| format!("field_{index}").into())
        .collect();
    let fields: Vec<ArrayRef> = (0_usize..257)
        .map(|index| {
            let value = i32::try_from(index).unwrap();
            PrimitiveArray::from_option_iter([Some(value), None, Some(-value)]).into_array()
        })
        .collect();
    let array = StructArray::try_new(FieldNames::from(names), fields, 3, Validity::NonNullable)
        .unwrap()
        .into_array();
    for row in 0..3 {
        let expected = observe(&array, row, usize::MAX, None, true);
        assert_eq!(expected.result, Ok((257, 0)));
        assert_eq!(observe(&array, row, usize::MAX, None, false), expected);
        for remaining in [0, 1, expected.bytes.len() / 2, expected.bytes.len() - 1] {
            assert_eq!(
                observe(&array, row, remaining, None, false),
                observe(&array, row, remaining, None, true)
            );
        }
    }
}

#[test]
fn terminal_json_denial_cancellation_and_panic_release_temporary_capacity() {
    struct PanicSink(usize);
    impl Write for PanicSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += 1;
            assert!(self.0 < 10, "intentional writer panic");
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let array = chain("mixed", 20);
    let session = session();
    let token = CancellationToken::default();
    let mut execution = session.create_execution_ctx();
    let measured = LiveMemoryPool::new(1 << 24).unwrap();
    drop(build::build(&array, &mut execution, &measured, &token).unwrap());
    let peak = measured.snapshot().peak_reserved_bytes;
    for bytes in [1, peak / 2, peak - 1] {
        let memory = LiveMemoryPool::new(bytes + 17).unwrap();
        let retained = memory.reserve(17).unwrap();
        assert!(build::build(&array, &mut execution, &memory, &token).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, retained.bytes());
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let memory = LiveMemoryPool::new(1 << 24).unwrap();
    let tree = build::build(&array, &mut execution, &memory, &token).unwrap();
    let baseline = memory.snapshot().reserved_bytes;
    let denial = memory.reserve((1 << 24) - baseline).unwrap();
    let mut bytes = Vec::new();
    assert!(write::write(&tree, 0, &mut bytes, &mut execution, &token).is_err());
    assert_eq!(bytes, [] as [u8; 0]);
    drop(denial);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(write::write(&tree, 0, &mut bytes, &mut execution, &cancelled).is_err());
    assert!(build::build(&array, &mut execution, &memory, &cancelled).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            write::write(&tree, 0, &mut PanicSink(0), &mut execution, &token).unwrap();
        }))
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    write::write(&tree, 0, &mut bytes, &mut execution, &token).unwrap();
    assert_eq!(bytes, expected_chain("mixed", 20).0);
    drop(tree);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

struct OwnerProbe {
    memory: LiveMemoryPool,
    drops: Arc<AtomicUsize>,
}

impl Drop for OwnerProbe {
    fn drop(&mut self) {
        assert!(
            self.memory.snapshot().reserved_bytes >= 1024,
            "schema owner must outlive its native view"
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn terminal_json_view_retains_input_and_releases_it_before_schema_credit() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let array = buffer![7_i32]
        .into_array()
        .with_retained_owner(Arc::new(OwnerProbe {
            memory: memory.clone(),
            drops: Arc::clone(&drops),
        }))
        .unwrap();
    let session = session();
    let mut execution = session.create_execution_ctx();
    let token = CancellationToken::default();
    let column = Column::new(&array, &mut execution, &memory, &token).unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 1024);
    drop(array);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let mut output = Vec::new();
    column
        .write(0, &mut output, &mut execution, &token)
        .unwrap();
    assert_eq!(output, b"7");
    assert_eq!(memory.snapshot().reserved_bytes, 1024);
    drop(column);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
