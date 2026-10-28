use super::*;
use vortex::array::dtype::{Nullability, PType};

fn dtype(name: &str) -> DType {
    crate::native_temporal::vortex_dtype(
        &TemporalDType::parse(name).unwrap(),
        Nullability::Nullable,
    )
    .unwrap()
}

fn convert(from: &str, to: &str, ticks: i64) -> Result<i64> {
    let cast = Cast::bind(&dtype(from), &dtype(to))?.unwrap();
    match cast.evaluate(&Cell::Temporal(ticks))? {
        Value::Int(value) => Ok(value),
        _ => panic!("temporal conversion must produce signed ticks"),
    }
}

fn parse(to: &str, text: &str) -> Result<i64> {
    let cast = Cast::bind(&DType::Utf8(Nullability::Nullable), &dtype(to))?.unwrap();
    match cast.evaluate(&Cell::Utf8(vortex::buffer::ByteBuffer::copy_from(
        text.as_bytes(),
    )))? {
        Value::Int(value) => Ok(value),
        _ => panic!("temporal parsing must produce signed ticks"),
    }
}

fn format(from: &str, ticks: i64) -> Result<String> {
    let cast = Cast::bind(&dtype(from), &DType::Utf8(Nullability::Nullable))?.unwrap();
    match cast.evaluate(&Cell::Temporal(ticks))? {
        Value::Text(value) => Ok(value.into_owned()),
        _ => panic!("temporal formatting must produce text"),
    }
}

#[test]
fn temporal_casts_scale_ticks_exactly_and_reject_domain_reinterpretation() {
    assert_eq!(
        convert("timestamp(s)", "timestamp(ns)", -2).unwrap(),
        -2_000_000_000
    );
    assert_eq!(
        convert("timestamp(ns)", "timestamp_micros", -1_000).unwrap(),
        -1
    );
    assert_eq!(
        convert("duration(ms)", "duration(ns)", -2).unwrap(),
        -2_000_000
    );
    assert_eq!(convert("time(s)", "time(ms)", 86_399).unwrap(), 86_399_000);
    for (from, to, value) in [
        ("timestamp(ns)", "timestamp_micros", -1),
        ("timestamp(s)", "timestamp(ns)", i64::MAX),
        ("duration(ns)", "duration(ms)", 1),
        ("time(ms)", "time(s)", 1),
        ("time(s)", "time(ns)", -1),
        ("date64", "date32", 1),
    ] {
        assert!(convert(from, to, value).is_err(), "{from} to {to}: {value}");
    }
    for (from, to) in [
        ("timestamp(s)", "timestamp(s,UTC)"),
        ("timestamp(s,UTC)", "timestamp(s)"),
        ("date32", "timestamp(s,UTC)"),
        ("duration(s)", "timestamp(s)"),
        ("time(s)", "duration(s)"),
    ] {
        assert!(Cast::bind(&dtype(from), &dtype(to)).is_err());
    }
    let integer = DType::Primitive(PType::I64, Nullability::Nullable);
    assert!(Cast::bind(&integer, &dtype("timestamp(s)")).is_err());
    assert!(Cast::bind(&dtype("timestamp(s)"), &integer).is_err());
}

#[test]
fn temporal_casts_floor_negative_dates_and_preserve_zoned_instants() {
    assert_eq!(convert("timestamp(ns)", "date32", -1).unwrap(), -1);
    assert_eq!(convert("timestamp(ns)", "date64", -1).unwrap(), -86_400_000);
    assert_eq!(convert("date32", "timestamp(s)", -1).unwrap(), -86_400);
    assert_eq!(convert("date64", "date32", -86_400_000).unwrap(), -1);
    assert_eq!(
        convert("timestamp(s,America/New_York)", "date32", 0).unwrap(),
        -1
    );
    for value in [i64::MIN, 0, i64::MAX] {
        assert_eq!(
            convert(
                "timestamp(s,America/New_York)",
                "timestamp(s,Asia/Tokyo)",
                value
            )
            .unwrap(),
            value
        );
    }
    assert!(convert("timestamp(s,UTC)", "date32", i64::MAX).is_err());
    assert!(format("timestamp(s,UTC)", i64::MAX).is_err());
}

#[test]
fn temporal_text_has_explicit_units_offsets_and_ambiguity_policy() {
    for (kind, text, expected) in [
        ("time(ns)", "23:59:59.999999999", 86_399_999_999_999),
        ("date64", "1969-12-31", -86_400_000),
        ("duration(ns)", "-17ns", -17),
        ("timestamp(ns)", "1969-12-31T23:59:59.999999999", -1),
        ("timestamp(ms)", "1970-01-01T01:00:00+01:00", 0),
        (
            "timestamp(s,America/New_York)",
            "1970-01-01T00:00:00",
            18_000,
        ),
        ("timestamp(s,America/New_York)", "1970-01-01T00:00:00Z", 0),
        (
            "timestamp(s,America/New_York)",
            "2024-11-03T01:30:00-04:00",
            1_730_611_800,
        ),
        (
            "timestamp(s,America/New_York)",
            "2024-11-03T01:30:00-05:00",
            1_730_615_400,
        ),
    ] {
        assert_eq!(parse(kind, text).unwrap(), expected, "{kind}: {text}");
    }
    for (kind, text) in [
        ("time(s)", "24:00:00"),
        ("time(s)", "00:00:00.1"),
        ("duration(ms)", "1ns"),
        ("date64", "2023-02-29"),
        ("timestamp(s)", "2016-12-31T23:59:60Z"),
        ("timestamp(s,America/New_York)", "2024-03-10T02:30:00"),
        ("timestamp(s,America/New_York)", "2024-11-03T01:30:00"),
    ] {
        assert!(parse(kind, text).is_err(), "{kind}: {text}");
    }
    assert_eq!(
        format("timestamp(s,America/New_York)", 0).unwrap(),
        "1969-12-31T19:00:00-05:00"
    );
    assert_eq!(
        format("timestamp_micros", -1).unwrap(),
        "1969-12-31T23:59:59.999999Z"
    );
    assert_eq!(
        format("duration(ns)", i64::MIN).unwrap(),
        "-9223372036854775808ns"
    );
    for unit in ["s", "ms", "us", "ns"] {
        let kind = format!("timestamp({unit})");
        for tick in [i64::MIN, -1, 0, 1, i64::MAX] {
            let text = format(&kind, tick).unwrap();
            assert_eq!(parse(&kind, &text).unwrap(), tick);
        }
    }
}
