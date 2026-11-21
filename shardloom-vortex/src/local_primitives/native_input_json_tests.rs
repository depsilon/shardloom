//! Independent original-parser parity and deep ownership acceptance.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;

use serde_json::Value;
use shardloom_exec::live_memory::LiveMemoryPool;

use super::super::super::native_input_schema_reference as reference;
use super::super::reserve_json;
use super::{Building, Children, Frame, ParsedJson, ReservedVec, parse, parse_with_depth};

fn parity(raw: &str) {
    let memory = LiveMemoryPool::new(1 << 28).unwrap();
    let workspace = reserve_json(&memory, raw.len()).unwrap();
    let actual = parse(raw, &memory).map_err(|error| error.to_string());
    let expected = reference::parse_json(raw).map_err(|error| error.to_string());
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            assert_eq!(actual.value(), &expected, "{raw}");
            // Number equality does not distinguish both zero signs. The exact
            // serializer and explicit float bits below preserve that contract.
            assert_eq!(
                serde_json::to_vec(actual.value()).unwrap(),
                serde_json::to_vec(&expected).unwrap(),
                "{raw}"
            );
            let mut pending = vec![(actual.value(), &expected)];
            while let Some((actual, expected)) = pending.pop() {
                match (actual, expected) {
                    (Value::Number(a), Value::Number(b)) if a.is_f64() => {
                        assert_eq!(
                            a.as_f64().unwrap().to_bits(),
                            b.as_f64().unwrap().to_bits(),
                            "{raw}"
                        );
                    }
                    (Value::Array(a), Value::Array(b)) => pending.extend(a.iter().zip(b)),
                    (Value::Object(a), Value::Object(b)) => {
                        pending.extend(a.values().zip(b.values()));
                    }
                    _ => {}
                }
            }
        }
        (Err(actual), Err(expected)) => assert_eq!(actual, expected, "{raw}"),
        (Ok(_), Err(error)) => panic!("new parser accepted {raw}: expected {error}"),
        (Err(error), Ok(_)) => panic!("new parser rejected {raw}: {error}"),
    }
    drop(workspace);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn typed_json_continuations_match_original_values_numbers_and_errors() {
    let cases = [
        "null",
        "true",
        "false",
        "0",
        "-0",
        "0.0",
        "-0.0",
        "1.0000000000000002",
        "9007199254740993",
        "18446744073709551615",
        "-9223372036854775808",
        "18446744073709551616",
        "-9223372036854775809",
        "1e400",
        "-1e400",
        "2.2250738585072014e-308",
        "5e-324",
        "1e-400",
        "1.7976931348623157e308",
        "1.7976931348623159e308",
        "1.0e+12",
        "-0e99",
        "[1,2,3]",
        "[]",
        "{}",
        r#"{"a":[null,true,"héllo🦀",-0],"z":{"b":4}}"#,
        r#"{"x":0,"x":1}"#,
        r#"{"x":0,"\u0078":1}"#,
        r#"{"x":0,"x":18446744073709551616}"#,
        r#"{"first":18446744073709551616,"x":0,"x":1}"#,
        r#"{"x":18446744073709551616,"later":"\ud800"}"#,
        r#"{"x":18446744073709551616,"later":1,}"#,
        r#"{"x":0,"\ud800":1}"#,
        r#"{"é🦀":0,"\udc00":1}"#,
        r#""\ud800""#,
        r#""\udc00""#,
        r#""\ud800\udc00""#,
        r#"["\ud800"]"#,
        r#"{"x":["\ud800"]}"#,
        "[18446744073709551616]",
        "[18446744073709551616, \n 1]",
        "{\"x\":18446744073709551616 \n }",
        "{\"x\":0,\n \"x\" \n : 1}",
        "{\"é🦀\":0,\n \"\\ud800\":1}",
        "[\n18446744073709551616\n]",
        " \n 18446744073709551616 trailing",
        "[18446744073709551616] false",
        "[1] false",
        " [1] \r\n\t ",
        "",
        " ",
        "+1",
        "01",
        "1.",
        "1e",
        "[",
        "{",
        "[1,]",
        "{\"x\":}",
        "[1 2]",
        "{\"x\" 1}",
        "{1:2}",
        "truex",
        "[\"bad\nstring\"]",
    ];
    for raw in cases {
        parity(raw);
    }
    // All proper UTF-8 prefixes exercise the provider's syntax-first behavior.
    for raw in [
        r#"{"x":[1,{"é":"🦀","z":true}],"tail":null}"#,
        r#"[1.2e+30,"\u1234",false]"#,
    ] {
        for end in 0..=raw.len() {
            if raw.is_char_boundary(end) {
                parity(&raw[..end]);
            }
        }
    }
    for bad in [
        "18446744073709551616",
        "1e400",
        r#""\ud800""#,
        r#"{"x":0,"x":1}"#,
    ] {
        for prefix in ["[", "[0, \n", "{\"é\":", "{\"first\":0,\n\"bad\" : \n"] {
            let close = if prefix.starts_with('[') { "]" } else { "}" };
            for suffix in [close.to_string(), format!(",\n{bad}{close}")] {
                let raw = format!("{prefix}{bad}{suffix}");
                parity(&raw);
                parity(&format!("[{{\"outer\":{raw}}}]"));
            }
        }
    }
}

fn chain(shape: &str, depth: usize, leaf: &str, siblings: bool) -> String {
    let object = |index: usize| shape == "object" || (shape == "mixed" && index.is_multiple_of(2));
    let mut raw = String::new();
    for index in 0..depth {
        raw.push_str(if object(index) { "{\"x\":" } else { "[" });
    }
    raw.push_str(leaf);
    for index in (0..depth).rev() {
        if object(index) {
            raw.push_str(if siblings { ",\"z\":7}" } else { "}" });
        } else {
            raw.push_str(if siblings { ",7]" } else { "]" });
        }
    }
    raw
}

#[test]
fn typed_json_continuations_keep_original_depth_and_error_positions() {
    for depth in [0, 1, 2, 94, 95, 96, 97, 98, 100] {
        for shape in ["array", "object", "mixed"] {
            for leaf in ["0", "[]", "{}", "18446744073709551616"] {
                parity(&chain(shape, depth, leaf, false));
            }
        }
    }
    let nested = chain("mixed", 96, "0", false);
    parity(&format!("{{\"x\" : \n {nested}}}"));
    parity(&format!("[\n {nested}]"));
}

fn inspect(mut value: &Value, shape: &str, depth: usize, siblings: bool) {
    for index in 0..depth {
        if shape == "object" || (shape == "mixed" && index.is_multiple_of(2)) {
            let fields = value.as_object().unwrap();
            assert_eq!(fields.len(), if siblings { 2 } else { 1 });
            if siblings {
                assert_eq!(fields["z"].as_u64(), Some(7));
            }
            value = &fields["x"];
        } else {
            let values = value.as_array().unwrap();
            assert_eq!(values.len(), if siblings { 2 } else { 1 });
            if siblings {
                assert_eq!(values[1].as_u64(), Some(7));
            }
            value = &values[0];
        }
    }
    assert_eq!(value.as_u64(), Some(42));
}

fn take(parsed: &mut ParsedJson, memory: &LiveMemoryPool) -> (Value, ReservedVec<Children>) {
    (
        std::mem::replace(&mut parsed.value, Value::Null),
        std::mem::replace(&mut parsed.cleanup, ReservedVec::new(memory).unwrap()),
    )
}

fn deep_child() {
    const DEPTH: usize = 8192;
    let memory = LiveMemoryPool::new(1 << 29).unwrap();
    for shape in ["array", "object", "mixed"] {
        let siblings = shape == "mixed";
        let raw = chain(shape, DEPTH, "42", siblings);
        let workspace = reserve_json(&memory, raw.len()).unwrap();
        let mut parsed = parse_with_depth(&raw, &memory, DEPTH).unwrap();
        inspect(parsed.value(), shape, DEPTH, siblings);
        assert!(memory.snapshot().reserved_bytes > workspace.bytes());
        if shape == "mixed" {
            // Exercise the same production building owner's unwind path with a
            // completed deep current value plus a partial container. Both came
            // from the actual parser and retain its prepaid cleanup capacity.
            let (value, cleanup) = take(&mut parsed, &memory);
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    let mut building = Building {
                        current: Some(value),
                        frames: ReservedVec::new(&memory).unwrap(),
                        cleanup: Some(cleanup),
                    };
                    building.frames.push(Frame::new("[0]")).unwrap();
                    building.frames.values[0].attach(Value::from(7));
                    panic!("intentional typed JSON building panic");
                }))
                .is_err()
            );
        }
        drop(parsed);
        assert_eq!(memory.snapshot().reserved_bytes, workspace.bytes());
        drop(workspace);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let good = chain("array", DEPTH, "42", false);
    for raw in [
        format!("[{good},18446744073709551616]"),
        format!("{{\"x\":{good},\"x\":0}}"),
        format!("{good} false"),
        chain("mixed", DEPTH, "1e400", false),
    ] {
        let workspace = reserve_json(&memory, raw.len()).unwrap();
        assert!(parse_with_depth(&raw, &memory, DEPTH + 1).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, workspace.bytes());
        drop(workspace);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    println!("TYPED_JSON_DEEP_COMPLETE");
}

#[test]
fn typed_json_continuations_parse_and_release_deep_values_on_small_stack() {
    const ENV: &str = "SHARDLOOM_TYPED_JSON_DEEP_CHILD";
    if std::env::var_os(ENV).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "local_primitives::native_input_schema::json::tests::typed_json_continuations_parse_and_release_deep_values_on_small_stack", "--nocapture"])
        .env(ENV, "1").output().unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("TYPED_JSON_DEEP_COMPLETE"));
}

#[test]
fn typed_json_continuations_admit_cleanup_and_frames_without_width_growth() {
    let child_bytes = std::mem::size_of::<Children>() as u64;
    let frame_bytes = std::mem::size_of::<Frame<'_>>() as u64;
    // Initial cleanup denial, initial frame denial, and both replacement-overlap
    // refusals after eight active containers. Raw-tree admission is separate.
    let partial_then_deep = format!(
        "[{},{}]",
        chain("mixed", 5, "42", true),
        chain("array", 16, "42", false)
    );
    for (raw, limit, peak) in [
        ("[0]".to_owned(), 8 * child_bytes - 1, 0),
        (
            "[0]".to_owned(),
            8 * child_bytes + 8 * frame_bytes - 1,
            8 * child_bytes,
        ),
        (
            partial_then_deep.clone(),
            24 * child_bytes + 8 * frame_bytes - 1,
            8 * child_bytes + 8 * frame_bytes,
        ),
        (
            partial_then_deep,
            16 * child_bytes + 24 * frame_bytes - 1,
            24 * child_bytes + 8 * frame_bytes,
        ),
    ] {
        let memory = LiveMemoryPool::new(limit).unwrap();
        let error = parse_with_depth(&raw, &memory, 17).err().unwrap();
        assert!(error.to_string().contains("memory reservation denied"));
        let snapshot = memory.snapshot();
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.denied_reservations, 1);
        assert_eq!(snapshot.peak_reserved_bytes, peak);
    }
    let memory = LiveMemoryPool::new(1 << 25).unwrap();
    for width in [1, 4096] {
        let raw = format!("[{}]", vec!["42"; width].join(","));
        let workspace = reserve_json(&memory, raw.len()).unwrap();
        let parsed = parse(&raw, &memory).unwrap();
        assert_eq!(parsed.value().as_array().unwrap().len(), width);
        assert!(
            parsed
                .value()
                .as_array()
                .unwrap()
                .iter()
                .all(|value| value.as_u64() == Some(42))
        );
        assert_eq!(parsed.cleanup.values.capacity(), 8);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            workspace.bytes() + 8 * child_bytes
        );
        drop(parsed);
        drop(workspace);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let scalar = parse("42", &memory).unwrap();
    assert_eq!(scalar.cleanup.values.capacity(), 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    drop(scalar);
}
