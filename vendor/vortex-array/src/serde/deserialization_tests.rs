// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use flatbuffers::{FlatBufferBuilder, VerifierOptions, root_with_opts};
use parking_lot::Mutex;
use vortex_buffer::{ByteBuffer, buffer};
use vortex_session::registry::CachedId;

use super::*;
use crate::array::ArrayId;
use crate::arrays::{FixedSizeListArray, StructArray};
use crate::buffer::BufferHandle;
use crate::dtype::{FieldNames, Nullability};
use crate::flatbuffers::{FlatBuffer, array as fba};
use crate::serde::ArrayNodeFlatBuffer;
use crate::validity::Validity;
use crate::{ArrayContext, ArrayPlugin, ArraySerialization, IntoArray};

fn id() -> ArrayId {
    static ID: CachedId = CachedId::new("test.ordered_array_decode");
    *ID
}

// Test-only explicit physical verification. Production defaults stay unchanged.
fn serialized(array: &ArrayRef, session: &VortexSession) -> (SerializedArray, ReadContext) {
    let ctx = ArrayContext::empty();
    let plan = ArrayNodeFlatBuffer::try_new(&ctx, session, array).unwrap();
    let buffers = plan
        .array
        .buffers()
        .into_iter()
        .map(BufferHandle::new_host)
        .collect::<Vec<_>>();
    let mut builder = FlatBufferBuilder::new();
    let root = plan.try_write_flatbuffer(&mut builder).unwrap();
    let array = fba::Array::create(
        &mut builder,
        &fba::ArrayArgs {
            root: Some(root),
            buffers: None,
        },
    );
    builder.finish_minimal(array);
    let flatbuffer = FlatBuffer::align_from(ByteBuffer::from(builder.finished_data().to_vec()));
    let options = VerifierOptions {
        max_depth: 20_000,
        ..Default::default()
    };
    let root = root_with_opts::<fba::Array>(&options, flatbuffer.as_ref()).unwrap();
    let flatbuffer_loc = root.root().unwrap()._tab.loc();
    (
        SerializedArray {
            flatbuffer,
            flatbuffer_loc,
            buffers: buffers.into(),
        },
        ReadContext::new(ctx.to_ids()),
    )
}

fn node(label: usize, children: Vec<ArrayRef>) -> ArrayRef {
    let array = new_foreign_array(
        id(),
        DType::Bool(Nullability::NonNullable),
        label,
        vec![],
        vec![BufferHandle::new_host(ByteBuffer::from(
            label.to_le_bytes().to_vec(),
        ))],
        children.into_iter().map(Some).collect(),
    )
    .unwrap();
    if label <= 1 {
        array
            .statistics()
            .set(Stat::IsSorted, Precision::Exact(true.into()));
        array
            .statistics()
            .set(Stat::IsStrictSorted, Precision::Exact(true.into()));
    }
    array
}

struct CallbackGuard(usize, Arc<Mutex<Vec<String>>>);

impl Drop for CallbackGuard {
    fn drop(&mut self) {
        self.1.lock().push(format!("leave:{}", self.0));
    }
}

#[derive(Clone, Copy, Debug, Default)]
enum Mode {
    #[default]
    Ordered,
    Synchronous,
    ExternalWait,
    AbandonedChild,
}

#[derive(Debug, Default)]
struct Plugin {
    events: Arc<Mutex<Vec<String>>>,
    fail: Option<usize>,
    panic: Option<usize>,
    reverse_repeat: bool,
    catch_child_error: bool,
    wrong_length: bool,
    skip_last: bool,
    mode: Mode,
}

impl Plugin {
    fn body<'a>(&'a self, parts: ArrayDeserialization<'a>) -> DeserializeFuture<'a, ArrayRef> {
        Box::pin(async move {
            self.events.lock().push(format!("enter:{}", parts.len));
            let _guard = CallbackGuard(parts.len, Arc::clone(&self.events));
            assert_ne!(self.panic, Some(parts.len), "injected decoder panic");
            if self.fail == Some(parts.len) {
                vortex_bail!("injected decoder error at {}", parts.len);
            }
            let order = if self.skip_last && parts.children.len() == 2 {
                vec![0]
            } else if self.reverse_repeat && parts.children.len() == 2 {
                vec![1, 0, 1]
            } else {
                (0..parts.children.len()).collect()
            };
            let mut slots = ArraySlots::new();
            for index in order {
                let begin = index * size_of::<usize>();
                let child_len = usize::from_le_bytes(
                    parts.metadata[begin..begin + size_of::<usize>()]
                        .try_into()
                        .unwrap(),
                );
                self.events
                    .lock()
                    .push(format!("request:{}:{index}", parts.len));
                match parts
                    .children
                    .get_async(index, parts.dtype, child_len)
                    .await
                {
                    Ok(child) => {
                        self.events
                            .lock()
                            .push(format!("resume:{}:{index}", parts.len));
                        slots.push(Some(child));
                    }
                    Err(error) if self.catch_child_error => {
                        self.events
                            .lock()
                            .push(format!("caught:{}:{index}:{error}", parts.len));
                    }
                    Err(error) => return Err(error),
                }
            }
            self.events.lock().push(format!("done:{}", parts.len));
            new_foreign_array(
                id(),
                parts.dtype.clone(),
                parts.len + usize::from(self.wrong_length),
                parts.metadata.to_vec(),
                parts.buffers.to_vec(),
                slots,
            )
        })
    }
}

impl ArrayPlugin for Plugin {
    fn id(&self) -> ArrayId {
        id()
    }
    fn read_ids(&self) -> Vec<ArrayId> {
        vec![id()]
    }
    fn write_ids(&self) -> Vec<ArrayId> {
        vec![id()]
    }
    fn serialize(
        &self,
        array: &ArrayRef,
        _session: &VortexSession,
    ) -> VortexResult<Option<ArraySerialization>> {
        let children = array.children();
        let metadata = children
            .iter()
            .flat_map(|child| child.len().to_le_bytes())
            .collect();
        Ok(Some(ArraySerialization::new(
            id(),
            metadata,
            array.buffers(),
            children,
        )))
    }
    fn deserialize(
        &self,
        parts: ArrayDeserialization<'_>,
        _session: &VortexSession,
    ) -> VortexResult<ArrayRef> {
        let children = SynchronousArrayChildren::new(parts.children);
        finish_deserialize(self.body(ArrayDeserialization {
            children: &children,
            ..parts
        }))
    }
    fn deserialize_async<'a>(
        &'a self,
        parts: ArrayDeserialization<'a>,
        session: &'a VortexSession,
    ) -> DeserializeFuture<'a, ArrayRef> {
        match self.mode {
            Mode::Ordered => self.body(parts),
            Mode::Synchronous => Box::pin(async move { self.deserialize(parts, session) }),
            Mode::ExternalWait => Box::pin(std::future::pending()),
            Mode::AbandonedChild => Box::pin(async move {
                let mut child = parts.children.get_async(0, parts.dtype, 2);
                poll_fn(|context| {
                    assert!(child.as_mut().poll(context).is_pending());
                    Poll::Ready(Err(vortex_err!("deliberately abandoned pending child")))
                })
                .await
            }),
        }
    }
}

fn session(plugin: Plugin) -> (VortexSession, Arc<Mutex<Vec<String>>>) {
    let session = VortexSession::empty();
    let events = Arc::clone(&plugin.events);
    session.arrays().register(plugin);
    (session, events)
}

fn compare_arrays(actual: &ArrayRef, expected: &ArrayRef) {
    let mut pending = vec![(actual.clone(), expected.clone())];
    while let Some((actual, expected)) = pending.pop() {
        assert_eq!(actual.dtype(), expected.dtype());
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual.encoding_id(), expected.encoding_id());
        let actual_stats = actual.statistics().to_owned();
        let expected_stats = expected.statistics().to_owned();
        for stat in enum_iterator::all::<Stat>() {
            assert_eq!(actual_stats.get(stat), expected_stats.get(stat), "{stat}");
        }
        let a = actual.buffers();
        let b = expected.buffers();
        assert_eq!(a.len(), b.len());
        for (a, b) in a.into_iter().zip(b) {
            assert_eq!(a.as_ref(), b.as_ref());
            assert_eq!(a.alignment(), b.alignment());
        }
        let a = actual.children();
        let b = expected.children();
        assert_eq!(a.len(), b.len());
        pending.extend(a.into_iter().zip(b));
    }
}

#[test]
fn ordered_conditional_repeated_and_caught_child_results_match_synchronous_decode() {
    let root = node(4, vec![node(2, vec![]), node(3, vec![])]);
    for reverse_repeat in [false, true] {
        for fail in [None, Some(2), Some(3), Some(4)] {
            for catch_child_error in [false, true] {
                let (session, events) = session(Plugin {
                    fail,
                    reverse_repeat,
                    catch_child_error,
                    ..Default::default()
                });
                let (serialized, ctx) = serialized(&root, &session);
                let expected = serialized.decode(root.dtype(), root.len(), &ctx, &session);
                let expected_events = mem::take(&mut *events.lock());
                let actual = serialized.decode_iterative(root.dtype(), root.len(), &ctx, &session);
                assert_eq!(*events.lock(), expected_events);
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) => compare_arrays(&actual, &expected),
                    (Err(actual), Err(expected)) => {
                        assert_eq!(actual.to_string(), expected.to_string())
                    }
                    _ => panic!("decode outcome differs"),
                }
                if reverse_repeat && fail.is_none() {
                    assert_eq!(
                        events
                            .lock()
                            .iter()
                            .filter(|event| *event == "enter:3")
                            .count(),
                        2
                    );
                }
            }
        }
    }
}

#[test]
fn unsupported_suspensions_refuse_without_replaying_or_decoding_children() {
    let root = node(4, vec![node(2, vec![])]);
    for (mode, error) in [
        (Mode::Synchronous, "requires a continuation for child 0"),
        (Mode::ExternalWait, "suspended without a child request"),
        (
            Mode::AbandonedChild,
            "completed with an unconsumed child request",
        ),
    ] {
        let (session, events) = session(Plugin {
            mode,
            ..Default::default()
        });
        let (serialized, ctx) = serialized(&root, &session);
        let error_message = serialized
            .decode_iterative(root.dtype(), root.len(), &ctx, &session)
            .unwrap_err()
            .to_string();
        assert!(error_message.contains(error), "{error_message}");
        assert!(!events.lock().iter().any(|event| event == "enter:2"));
        assert!(
            events
                .lock()
                .iter()
                .filter(|event| *event == "enter:4")
                .count()
                <= 1
        );
    }
}

#[test]
fn malformed_buffers_unknown_encodings_and_callback_panics_preserve_boundaries() {
    let root = node(4, vec![node(2, vec![]), node(3, vec![])]);
    for (panic, wrong_length) in [(Some(2), false), (Some(3), false), (None, true)] {
        let (session, events) = session(Plugin {
            panic,
            wrong_length,
            ..Default::default()
        });
        let (serialized, ctx) = serialized(&root, &session);
        assert!(
            catch_unwind(AssertUnwindSafe(|| serialized.decode(
                root.dtype(),
                root.len(),
                &ctx,
                &session
            )))
            .is_err()
        );
        let expected = mem::take(&mut *events.lock());
        assert!(
            catch_unwind(AssertUnwindSafe(|| serialized.decode_iterative(
                root.dtype(),
                root.len(),
                &ctx,
                &session
            )))
            .is_err()
        );
        assert_eq!(*events.lock(), expected);
    }
    let (session, events) = session(Plugin::default());
    let (mut serialized, ctx) = serialized(&root, &session);
    serialized.buffers = Arc::new([]);
    let expected = serialized
        .decode(root.dtype(), root.len(), &ctx, &session)
        .unwrap_err()
        .to_string();
    let actual = serialized
        .decode_iterative(root.dtype(), root.len(), &ctx, &session)
        .unwrap_err()
        .to_string();
    assert_eq!(actual, expected);
    assert!(events.lock().is_empty());
    let empty = ReadContext::new([]);
    let expected = serialized
        .decode(root.dtype(), root.len(), &empty, &session)
        .unwrap_err()
        .to_string();
    let actual = serialized
        .decode_iterative(root.dtype(), root.len(), &empty, &session)
        .unwrap_err()
        .to_string();
    assert_eq!(actual, expected);
}

fn raw_tree(child: bool, invalid_stats: bool, invalid_buffers: bool) -> SerializedArray {
    let mut builder = FlatBufferBuilder::new();
    let min = invalid_stats.then(|| builder.create_vector(&[0x80u8]));
    let stats = fba::ArrayStats::create(
        &mut builder,
        &fba::ArrayStatsArgs {
            min,
            min_precision: fba::Precision::Exact,
            is_sorted: Some(false),
            is_strict_sorted: Some(false),
            ..Default::default()
        },
    );
    let children = child.then(|| {
        let leaf = fba::ArrayNode::create(
            &mut builder,
            &fba::ArrayNodeArgs {
                encoding: 1,
                stats: Some(stats),
                ..Default::default()
            },
        );
        builder.create_vector(&[leaf])
    });
    let buffers = invalid_buffers.then(|| builder.create_vector(&[7u16]));
    let root = fba::ArrayNode::create(
        &mut builder,
        &fba::ArrayNodeArgs {
            children,
            buffers,
            stats: Some(stats),
            ..Default::default()
        },
    );
    let array = fba::Array::create(
        &mut builder,
        &fba::ArrayArgs {
            root: Some(root),
            buffers: None,
        },
    );
    builder.finish_minimal(array);
    SerializedArray::from_flatbuffer_with_buffers(
        ByteBuffer::from(builder.finished_data().to_vec()),
        vec![],
    )
    .unwrap()
}

#[test]
fn foreign_descendants_skip_registered_plugins_and_stats_in_original_order() {
    static UNKNOWN: CachedId = CachedId::new("test.ordered_array_decode.unknown");
    let (session, events) = session(Plugin {
        fail: Some(1),
        ..Default::default()
    });
    session.allow_unknown();
    let dtype = DType::Bool(Nullability::NonNullable);
    let ctx = ReadContext::new([*UNKNOWN, id()]);
    let serialized = raw_tree(true, true, false);
    let actual = serialized
        .decode_iterative(&dtype, 1, &ctx, &session)
        .unwrap();
    let expected = serialized.decode(&dtype, 1, &ctx, &session).unwrap();
    compare_arrays(&actual, &expected);
    assert_eq!(actual.children()[0].encoding_id(), id());
    assert!(events.lock().is_empty());
    assert!(actual.statistics().to_owned().is_empty());
    assert!(actual.children()[0].statistics().to_owned().is_empty());

    // Resolve the child before collecting a foreign parent's invalid buffer.
    let serialized = raw_tree(true, false, true);
    let short_ctx = ReadContext::new([*UNKNOWN]);
    for (is_short, ctx) in [(true, &short_ctx), (false, &ctx)] {
        let expected = serialized
            .decode(&dtype, 1, ctx, &session)
            .unwrap_err()
            .to_string();
        let actual = serialized
            .decode_iterative(&dtype, 1, ctx, &session)
            .unwrap_err()
            .to_string();
        assert_eq!(actual, expected);
        if is_short {
            assert!(actual.contains("Unknown encoding index: 1"));
        }
    }
}

#[test]
fn stats_follow_callback_completion_and_conditional_children_are_not_predecoded() {
    let dtype = DType::Bool(Nullability::NonNullable);
    let (session, events) = session(Plugin::default());
    let ctx = ReadContext::new([id()]);
    let malformed = raw_tree(false, true, false);
    let expected = malformed
        .decode(&dtype, 1, &ctx, &session)
        .unwrap_err()
        .to_string();
    let expected_events = mem::take(&mut *events.lock());
    let actual = malformed
        .decode_iterative(&dtype, 1, &ctx, &session)
        .unwrap_err()
        .to_string();
    assert_eq!(actual, expected);
    assert_eq!(*events.lock(), expected_events);
    assert_eq!(expected_events, ["enter:1", "done:1", "leave:1"]);
    let valid = raw_tree(false, false, false);
    let decoded = valid.decode_iterative(&dtype, 1, &ctx, &session).unwrap();
    assert_eq!(
        decoded.statistics().to_owned().get(Stat::IsSorted),
        Precision::Exact(true.into())
    );
    assert_eq!(
        decoded.statistics().to_owned().get(Stat::IsStrictSorted),
        Precision::Exact(true.into())
    );

    let root = node(4, vec![node(2, vec![]), node(3, vec![])]);
    let (session, events) = self::session(Plugin {
        fail: Some(3),
        skip_last: true,
        ..Default::default()
    });
    let (serialized, ctx) = serialized(&root, &session);
    let expected = serialized
        .decode(root.dtype(), root.len(), &ctx, &session)
        .unwrap();
    let expected_events = mem::take(&mut *events.lock());
    let actual = serialized
        .decode_iterative(root.dtype(), root.len(), &ctx, &session)
        .unwrap();
    compare_arrays(&actual, &expected);
    assert_eq!(*events.lock(), expected_events);
    assert!(!events.lock().iter().any(|event| event == "enter:3"));
}

struct Owner(Arc<AtomicUsize>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn deep_case() {
    const DEPTH: usize = 8192;
    let (session, events) = session(Plugin::default());
    let mut root = node(0, vec![]);
    for depth in 1..=DEPTH {
        root = node(depth, vec![root]);
    }
    let (serialized, ctx) = serialized(&root, &session);
    let decoded = serialized
        .decode_iterative(root.dtype(), root.len(), &ctx, &session)
        .unwrap();
    compare_arrays(&decoded, &root);
    assert_eq!(
        events
            .lock()
            .iter()
            .filter(|event| event.starts_with("enter:"))
            .count(),
        DEPTH + 1
    );
    drop(decoded);
    events.lock().clear();
    let (failing, failures) = self::session(Plugin {
        fail: Some(0),
        ..Default::default()
    });
    assert!(
        serialized
            .decode_iterative(root.dtype(), root.len(), &ctx, &failing)
            .is_err()
    );
    let leaves = failures
        .lock()
        .iter()
        .filter(|event| event.starts_with("leave:"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        leaves,
        (0..=DEPTH)
            .map(|depth| format!("leave:{depth}"))
            .collect::<Vec<_>>()
    );
    let (panicking, panics) = self::session(Plugin {
        panic: Some(0),
        ..Default::default()
    });
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            serialized.decode_iterative(root.dtype(), root.len(), &ctx, &panicking)
        }))
        .is_err()
    );
    let unwound = panics
        .lock()
        .iter()
        .filter(|event| event.starts_with("leave:"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(unwound, leaves);
    let foreign = VortexSession::empty();
    foreign.allow_unknown();
    let raw = serialized
        .decode_iterative(root.dtype(), root.len(), &ctx, &foreign)
        .unwrap();
    let mut child = raw.clone();
    for _ in 0..DEPTH {
        assert_eq!(child.dtype(), root.dtype());
        assert_eq!(child.len(), root.len());
        assert!(child.statistics().to_owned().is_empty());
        child = child.children().remove(0);
    }
    assert_eq!(child.buffers()[0].as_ref(), 0usize.to_le_bytes());
    drop(raw);
    drop(root);

    let native = VortexSession::empty();
    let released = Arc::new(AtomicUsize::new(0));
    let mut root = buffer![17i32, -3]
        .into_array()
        .with_retained_owner(Arc::new(Owner(Arc::clone(&released))))
        .unwrap();
    for depth in 0..DEPTH {
        root = if depth.is_multiple_of(2) {
            StructArray::new(
                FieldNames::from(["value"]),
                vec![root],
                2,
                Validity::NonNullable,
            )
            .into_array()
        } else {
            FixedSizeListArray::new(root, 1, Validity::NonNullable, 2).into_array()
        };
    }
    let (serialized, ctx) = self::serialized(&root, &native);
    let reopened = serialized
        .decode_iterative(root.dtype(), root.len(), &ctx, &native)
        .unwrap();
    compare_arrays(&reopened, &root);
    let mut leaf = reopened.clone();
    for _ in 0..DEPTH {
        leaf = leaf.children().remove(0);
    }
    assert_eq!(
        leaf.as_::<crate::arrays::Primitive>()
            .to_buffer::<i32>()
            .as_slice(),
        &[17, -3]
    );
    drop(root);
    assert_eq!(released.load(Ordering::SeqCst), 1);
    drop(reopened);
    assert_eq!(leaf.len(), 2);
}

#[test]
fn deep_array_decode_failure_and_retained_cleanup_use_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_DEEP_ARRAY_DECODE";
    const COMPLETE: &str = "deep array decoding completed";
    if env::var_os(SELECTOR).is_some() {
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_case)
            .unwrap()
            .join()
            .unwrap();
        println!("\n{COMPLETE}");
        return;
    }
    let output = Command::new(env::current_exe().unwrap()).args([
        "--exact", "serde::deserialization::tests::deep_array_decode_failure_and_retained_cleanup_use_small_stack",
        "--test-threads=1", "--nocapture",
    ]).env(SELECTOR, "1").output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line == COMPLETE)
    );
}
