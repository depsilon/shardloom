// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::fmt;
use std::sync::Arc;

use crate::datatype_structural::tests::{cases, deep};
use crate::structural_reference as original;
use crate::traversal_test_utils::small_stack;
use crate::{DataType, Field, Fields};

#[test]
fn schema_formatting_matches_original_text_and_flags() {
    let mut fixtures = cases();
    let escaped = Arc::new(Field::new("a\n'\"🦊", DataType::Utf8, true).with_metadata(
        std::collections::HashMap::from([
            ("a\n🦊".into(), "x\t\"\\".into()),
            ("z".into(), "last".into()),
        ]),
    ));
    fixtures.push(DataType::List(escaped.clone()));
    fixtures.push(DataType::Struct(Fields::from([escaped.clone(), escaped])));
    fixtures.push(deep(
        DataType::Timestamp(crate::TimeUnit::Second, Some("UTC\n🦊".into())),
        16,
    ));
    for (index, dtype) in fixtures.iter().enumerate() {
        let old = original::DataType::from(dtype);
        macro_rules! compare {
            ($spec:literal) => {
                assert_eq!(
                    format!($spec, dtype),
                    format!($spec, old),
                    "format {} fixture {}",
                    $spec,
                    index
                );
            };
        }
        compare!("{:?}");
        compare!("{:#?}");
        compare!("{:>16?}");
        compare!("{:+08?}");
        compare!("{:.3?}");
        compare!("{:^24.7?}");
        compare!("{:🦊^24?}");
        compare!("{:x?}");
        compare!("{:#x?}");
        compare!("{:X?}");
        compare!("{:#X?}");
        compare!("{}");
        compare!("{:#}");
        compare!("{:+010.2}");
        compare!("{:🦊^24}");
        // Standard outer Debug adapters must still add exactly their own indent.
        assert_eq!(
            format!("{:#?}", (Some(dtype), vec![dtype])),
            format!("{:#?}", (Some(&old), vec![&old]))
        );
        let field = Field::new("root", dtype.clone(), true);
        let old_field = original::Field::from_current(&field);
        assert_eq!(format!("{:#?}", field), format!("{:#?}", old_field));
        assert_eq!(format!("{:+08?}", field), format!("{:+08?}", old_field));
    }
}

struct LimitedWriter {
    prefix: String,
    remaining: usize,
    failed: bool,
    called_after_failure: bool,
}

impl LimitedWriter {
    fn new(limit: usize) -> Self {
        Self {
            prefix: String::new(),
            remaining: limit,
            failed: false,
            called_after_failure: false,
        }
    }
}

impl fmt::Write for LimitedWriter {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.failed {
            self.called_after_failure = true;
            return Err(fmt::Error);
        }
        if text.len() <= self.remaining {
            self.prefix.push_str(text);
            self.remaining -= text.len();
            return Ok(());
        }
        let mut keep = self.remaining;
        while !text.is_char_boundary(keep) {
            keep -= 1;
        }
        self.prefix.push_str(&text[..keep]);
        self.remaining -= keep;
        self.failed = true;
        Err(fmt::Error)
    }
}

#[test]
fn failing_schema_formatter_keeps_prefix_and_stops_writing() {
    let dtype = deep(
        DataType::Timestamp(crate::TimeUnit::Second, Some("🦊".into())),
        8,
    );
    let old = original::DataType::from(&dtype);
    macro_rules! check {
        ($spec:literal) => {{
            let expected = format!($spec, old);
            for limit in (0..64).chain([
                expected.len() / 2,
                expected.len() - 1,
                expected.len(),
                expected.len() + 1,
            ]) {
                let mut writer = LimitedWriter::new(limit);
                let result = fmt::write(&mut writer, format_args!($spec, dtype));
                assert_eq!(result.is_err(), limit < expected.len());
                assert!(!writer.called_after_failure);
                let mut end = limit.min(expected.len());
                while !expected.is_char_boundary(end) {
                    end -= 1;
                }
                assert_eq!(writer.prefix, expected[..end]);
            }
        }};
    }
    check!("{:?}");
    check!("{:#?}");
    check!("{}");
}

#[test]
fn deep_schema_formatting_and_bounded_pretty_error() {
    small_stack(
        "datatype_format_tests::deep_schema_formatting_and_bounded_pretty_error",
        || {
            let mut dtype = DataType::Int32;
            for _ in 0..8_192 {
                dtype = DataType::new_list(dtype, false);
            }
            assert_eq!(
                format!("{dtype}"),
                "List(non-null ".repeat(8_192) + "Int32" + &")".repeat(8_192)
            );
            assert_eq!(
                format!("{dtype:?}"),
                "List(Field { data_type: ".repeat(8_192) + "Int32" + &" })".repeat(8_192)
            );
            let mixed = deep(DataType::Int64, 8_192);
            assert!(format!("{mixed}").contains("Int64"));
            assert!(format!("{mixed:?}").contains("Int64"));
            // Pretty indentation grows with output size. Reject bounded output rather
            // than generating a quadratic-sized String just to exercise failure.
            let mut writer = LimitedWriter::new(8_192);
            assert!(fmt::write(&mut writer, format_args!("{mixed:#?}")).is_err());
            assert_eq!(writer.prefix.len(), 8_192);
            assert!(!writer.called_after_failure);
            let wide = DataType::Struct(
                (0..4_096)
                    .map(|i| Field::new(format!("f{i}"), DataType::Int32, false))
                    .collect(),
            );
            assert!(format!("{wide}").ends_with("\"f4095\": non-null Int32)"));
            assert!(format!("{wide:?}").contains("f4095"));
        },
    );
}
