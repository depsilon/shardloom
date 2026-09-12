//! Complete-result differential checks of the existing native file-statistics
//! consumer. Flat controls omit file statistics, so no hidden zone map can prune.
//! Segment requests are observed after file preparation; these are provider
//! payload requests, not OS/device read bytes or an ingest benchmark.

use super::*;
use std::{path::PathBuf, sync::atomic::AtomicUsize};
use vortex::{
    array::{
        IntoArray as _,
        arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray},
        dtype::FieldNames,
        expr::{
            Expression, and, byte_length, eq, get_item, gt, gt_eq, is_not_null, is_null, like, lit,
            lt, lt_eq, root,
        },
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    layout::{
        layouts::flat::writer::FlatLayoutStrategy,
        segments::{SegmentFuture, SegmentId, SegmentSource},
    },
};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    directory: PathBuf,
    numbers: Vec<Option<i64>>,
    texts: Vec<Option<&'static str>>,
    expected: ArrayRef,
}

impl Fixture {
    fn new(profile: &str, dictionary: bool) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-file-pruning-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&directory).unwrap();
        let (numbers, texts) = match profile {
            "mixed" => (
                vec![
                    Some(i64::MIN),
                    Some(i64::MAX),
                    Some(-1),
                    Some(0),
                    Some(1),
                    None,
                    Some(42),
                ],
                vec![
                    Some(""),
                    Some("a"),
                    Some("港"),
                    Some("AA"),
                    Some("aa"),
                    None,
                    Some("%_\\"),
                ],
            ),
            "all_null" => (vec![None; 7], vec![None; 7]),
            "constant" => (vec![Some(7); 7], vec![Some("港"); 7]),
            "empty" => (Vec::new(), Vec::new()),
            _ => unreachable!(),
        };
        let ids = PrimitiveArray::new(
            (0..numbers.len())
                .map(|row| u64::try_from(row).unwrap())
                .collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array();
        let numeric = PrimitiveArray::from_option_iter(numbers.clone()).into_array();
        let text = VarBinViewArray::from_iter_nullable_str(texts.iter().copied()).into_array();
        let fields = FieldNames::from(["renamed_row_id", "renamed_numeric", "renamed_text"]);
        let expected = StructArray::try_new(
            fields.clone(),
            vec![ids.clone(), numeric.clone(), text.clone()],
            numbers.len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let represented_text = if dictionary {
            let mut values = Vec::new();
            let codes = texts
                .iter()
                .map(|value| {
                    let code = values
                        .iter()
                        .position(|candidate| candidate == value)
                        .unwrap_or_else(|| {
                            values.push(*value);
                            values.len() - 1
                        });
                    u32::try_from(code).unwrap()
                })
                .collect::<Vec<_>>();
            DictArray::try_new(
                PrimitiveArray::new(codes, Validity::NonNullable).into_array(),
                VarBinViewArray::from_iter_nullable_str(values).into_array(),
            )
            .unwrap()
            .into_array()
        } else {
            text
        };
        let represented = StructArray::try_new(
            fields,
            vec![ids, numeric, represented_text],
            numbers.len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        for statistics in [false, true] {
            let mut options = session
                .write_options()
                .with_strategy(Arc::new(FlatLayoutStrategy::default()));
            if !statistics {
                options = options.with_file_statistics(Vec::new());
            }
            options
                .blocking(&runtime)
                .write(
                    std::fs::File::create(directory.join(format!("stats-{statistics}.vortex")))
                        .unwrap(),
                    represented.to_array_iterator(),
                )
                .unwrap();
        }
        Self {
            directory,
            numbers,
            texts,
            expected,
        }
    }

    fn path(&self, statistics: bool) -> PathBuf {
        self.directory.join(format!("stats-{statistics}.vortex"))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[derive(Clone, Copy, Debug)]
enum Predicate {
    BelowIntegerMinimum,
    AboveIntegerMaximum,
    EqualIntegerMaximum,
    NumericRange,
    IsNull,
    IsNotNull,
    EqualEmpty,
    EqualAbsentHigh,
    EqualAbsentWithinRange,
    Utf8Range,
    ContainsAscii,
    ContainsUnicode,
    ContainsAbsent,
    ByteLengthTwo,
    ByteLengthAbsent,
}

impl Predicate {
    fn expression(self) -> Expression {
        let number = get_item("renamed_numeric", root());
        let text = get_item("renamed_text", root());
        match self {
            Self::BelowIntegerMinimum => lt(number, lit(i64::MIN)),
            Self::AboveIntegerMaximum => gt(number, lit(i64::MAX)),
            Self::EqualIntegerMaximum => eq(number, lit(i64::MAX)),
            Self::NumericRange => and(
                gt_eq(number.clone(), lit(0_i64)),
                lt_eq(number, lit(42_i64)),
            ),
            Self::IsNull => is_null(text),
            Self::IsNotNull => is_not_null(text),
            Self::EqualEmpty => eq(text, lit("")),
            Self::EqualAbsentHigh => eq(text, lit("\u{10ffff}")),
            Self::EqualAbsentWithinRange => eq(text, lit("bb")),
            Self::Utf8Range => gt(text, lit("aa")),
            Self::ContainsAscii => like(text, lit("%a%")),
            Self::ContainsUnicode => like(text, lit("%港%")),
            Self::ContainsAbsent => like(text, lit("%not-present%")),
            Self::ByteLengthTwo => eq(byte_length(text), lit(2_u64)),
            Self::ByteLengthAbsent => gt(byte_length(text), lit(9000_u64)),
        }
    }

    fn matches(self, number: Option<i64>, text: Option<&str>) -> bool {
        match self {
            Self::BelowIntegerMinimum | Self::AboveIntegerMaximum => false,
            Self::EqualIntegerMaximum => number == Some(i64::MAX),
            Self::NumericRange => number.is_some_and(|value| (0..=42).contains(&value)),
            Self::IsNull => text.is_none(),
            Self::IsNotNull => text.is_some(),
            Self::EqualEmpty => text == Some(""),
            Self::EqualAbsentHigh
            | Self::EqualAbsentWithinRange
            | Self::ContainsAbsent
            | Self::ByteLengthAbsent => false,
            Self::Utf8Range => text.is_some_and(|value| value > "aa"),
            Self::ContainsAscii => text.is_some_and(|value| value.contains('a')),
            Self::ContainsUnicode => text.is_some_and(|value| value.contains('港')),
            Self::ByteLengthTwo => text.is_some_and(|value| value.len() == 2),
        }
    }
}

struct ObservedSegments {
    inner: Arc<dyn SegmentSource>,
    requested: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

impl SegmentSource for ObservedSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let inner = Arc::clone(&self.inner);
        let requested = Arc::clone(&self.requested);
        let completed = Arc::clone(&self.completed);
        async move {
            requested.fetch_add(1, Ordering::SeqCst);
            let buffer = inner.request(id).await?;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(buffer)
        }
        .boxed()
    }
}

fn observed_source(
    source: &PreparedVortexSource,
) -> (PreparedVortexSource, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let requested = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    // Retain the public prepare_file identity and session; decorate only its
    // existing segment source. This adds no file open or alternate executor.
    let file = source
        .0
        .file
        .clone()
        .with_segment_source(Arc::new(ObservedSegments {
            inner: source.0.file.segment_source(),
            requested: Arc::clone(&requested),
            completed: Arc::clone(&completed),
        }));
    (
        PreparedVortexSource(Arc::new(PreparedSourceOwner {
            file,
            identity: source.0.identity.clone(),
            runtime: Arc::clone(&source.0.runtime),
        })),
        requested,
        completed,
    )
}

fn check_predicate(
    source: &PreparedVortexSource,
    fixture: &Fixture,
    predicate: Predicate,
) -> (bool, usize) {
    let (source, requested, completed) = observed_source(source);
    let expression = predicate.expression();
    let can_prune = source
        .with_native_execution(|file, _, _| file.can_prune(&expression).map_err(native_error))
        .unwrap();
    assert_eq!(
        requested.load(Ordering::SeqCst),
        0,
        "file-stat proof must not request payload"
    );
    let filter = expression.bind(source.dtype()).unwrap();
    let result = source
        .prepare_projection(
            &["renamed_row_id", "renamed_numeric", "renamed_text"],
            u64::try_from(fixture.numbers.len().max(1)).unwrap(),
            1 << 20,
        )
        .unwrap()
        .with_filter(Some(filter))
        .execute()
        .unwrap();
    let expected = fixture
        .numbers
        .iter()
        .zip(&fixture.texts)
        .enumerate()
        .filter_map(|(row, (number, text))| predicate.matches(*number, *text).then_some(row))
        .collect::<Vec<_>>();
    let mut context = source.0.runtime.session.create_execution_ctx();
    let mut selected = expected.iter();
    for array in result.arrays() {
        for row in 0..array.len() {
            let expected_row = *selected.next().expect("no extra selected row");
            assert_eq!(
                array.execute_scalar(row, &mut context).unwrap(),
                fixture
                    .expected
                    .execute_scalar(expected_row, &mut context)
                    .unwrap(),
                "{predicate:?}"
            );
        }
    }
    assert!(
        selected.next().is_none(),
        "missing selected row for {predicate:?}"
    );
    assert_eq!(result.row_count(), u64::try_from(expected.len()).unwrap());
    let requests = requested.load(Ordering::SeqCst);
    assert_eq!(requests, completed.load(Ordering::SeqCst));
    if can_prune {
        assert!(expected.is_empty());
        assert_eq!(
            requests, 0,
            "native file statistics must prevent payload requests"
        );
    }
    (can_prune, requests)
}

#[test]
fn resident_file_pruning_stats_and_no_stats_match_complete_nullable_encoded_results() {
    for profile in ["mixed", "all_null", "constant", "empty"] {
        for dictionary in [false, true] {
            let fixture = Fixture::new(profile, dictionary);
            let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
            let memory = session.memory().clone();
            let with_stats = session.prepare_file(fixture.path(true)).unwrap();
            let without_stats = session.prepare_file(fixture.path(false)).unwrap();
            assert!(with_stats.0.file.footer().statistics().is_some());
            assert!(without_stats.0.file.footer().statistics().is_none());
            let mut pruned = 0;
            for predicate in [
                Predicate::BelowIntegerMinimum,
                Predicate::AboveIntegerMaximum,
                Predicate::EqualIntegerMaximum,
                Predicate::NumericRange,
                Predicate::IsNull,
                Predicate::IsNotNull,
                Predicate::EqualEmpty,
                Predicate::EqualAbsentHigh,
                Predicate::EqualAbsentWithinRange,
                Predicate::Utf8Range,
                Predicate::ContainsAscii,
                Predicate::ContainsUnicode,
                Predicate::ContainsAbsent,
                Predicate::ByteLengthTwo,
                Predicate::ByteLengthAbsent,
            ] {
                let (candidate_pruned, _) = check_predicate(&with_stats, &fixture, predicate);
                let (control_pruned, control_requests) =
                    check_predicate(&without_stats, &fixture, predicate);
                assert!(
                    !control_pruned,
                    "Flat no-stat control must have no metadata proof"
                );
                pruned += usize::from(candidate_pruned);
                if profile == "mixed"
                    && matches!(
                        predicate,
                        Predicate::ContainsAbsent
                            | Predicate::ByteLengthAbsent
                            | Predicate::EqualAbsentWithinRange
                    )
                {
                    // Unsupported pruning expressions and absent values within
                    // min/max must read through, even when the final result is
                    // empty. No ngram or exact membership synopsis is present.
                    assert!(!candidate_pruned);
                    assert!(control_requests > 0);
                }
            }
            if profile != "empty" {
                assert!(pruned > 0, "matrix must execute real metadata pruning");
            }
            assert_eq!(session.snapshot().prepared_source_opens, 2);
            assert!(memory.snapshot().peak_reserved_bytes <= memory.snapshot().limit_bytes);
            drop((with_stats, without_stats, session));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}
