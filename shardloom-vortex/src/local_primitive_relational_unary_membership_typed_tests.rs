//! Exact native typed keys, payloads and source generations across spill.

use super::*;
use crate::local_primitives::native_relational_spill::replacement::AFTER_COUNT_PASS;
use std::{cell::Cell, rc::Rc};
use vortex::array::{
    arrays::{
        DecimalArray, DictArray, ExtensionArray, FixedSizeListArray, ListArray, VarBinArray,
        VarBinViewArray,
    },
    dtype::DecimalDType,
    extension::datetime::{Date, TimeUnit, Timestamp},
};

const FIELDS: [&str; 10] = [
    "id",
    "binary",
    "decimal",
    "day",
    "instant",
    "list",
    "fixed",
    "required_fixed",
    "nested",
    "pad",
];
const CLASSES: [usize; 6] = [0, 1, 0, 2, 1, 3];

fn fixed_values(validity: Validity) -> ArrayRef {
    FixedSizeListArray::try_new(
        PrimitiveArray::from_option_iter(
            CLASSES
                .into_iter()
                .flat_map(|value| [Some(i16::try_from(value).unwrap()), None]),
        )
        .into_array(),
        2,
        validity,
        CLASSES.len(),
    )
    .unwrap()
    .into_array()
}

fn expected_typed_rows(
    decimals: &[Option<i128>],
    days: &[Option<i32>],
    instants: &[Option<i64>],
    pad: &str,
) -> Vec<Value> {
    (0..CLASSES.len()).map(|row| {
        let value = CLASSES[row];
        let binary_json = match value { 0 => json!("00ff"), 1 => json!(""), 2 => Value::Null, _ => json!("c3a9") };
        let list_json = match value { 0 => json!([7,null]), 1 => json!([]), 2 => Value::Null, _ => json!([null]) };
        json!({"id":row, "binary":binary_json,
            "decimal":decimals[row].map(|raw| format!("decimal128(38,6):{raw}")),
            "day":days[row], "instant":instants[row],
            "list":list_json,
            "fixed":if value == 2 {Value::Null} else {json!([value,null])},
            "required_fixed":[value,null],
            "nested":if value == 2 {Value::Null} else {json!({"value":if value == 3 {3.0} else {f64::from(u32::try_from(value).unwrap()) + 1.0},"fixed":[value,null]})},
            "pad":pad})
    }).collect()
}

fn typed_source(dictionary: bool) -> (ArrayRef, Vec<Value>) {
    let edge = 10_i128.pow(38) - 1;
    let binary: [Option<&[u8]>; 6] = [
        Some(b"\0\xff"),
        Some(b""),
        Some(b"\0\xff"),
        None,
        Some(b""),
        Some(b"\xc3\xa9"),
    ];
    let decimals = [
        Some(edge),
        Some(-edge),
        Some(edge),
        None,
        Some(-edge),
        Some(0),
    ];
    let days = [
        Some(i32::MIN),
        Some(i32::MAX),
        Some(i32::MIN),
        None,
        Some(i32::MAX),
        Some(0),
    ];
    let instants = [
        Some(i64::MIN),
        Some(i64::MAX),
        Some(i64::MIN),
        None,
        Some(i64::MAX),
        Some(0),
    ];
    let list = ListArray::new(
        PrimitiveArray::from_option_iter([Some(7_i16), None, Some(7), None, None]).into_array(),
        PrimitiveArray::from_iter([0_u64, 2, 2, 4, 4, 4, 5]).into_array(),
        Validity::from_iter([true, true, true, false, true, true]),
    )
    .into_array();
    let required_fixed = fixed_values(Validity::NonNullable);
    let fixed = fixed_values(Validity::from_iter([true, true, true, false, true, true]));
    let nested = StructArray::new(
        FieldNames::from(["value", "fixed"]),
        vec![
            PrimitiveArray::from_iter([1_f64, 2.0, 1.0, f64::NAN, 2.0, 3.0]).into_array(),
            required_fixed.clone(),
        ],
        CLASSES.len(),
        Validity::from_iter([true, true, true, false, true, true]),
    )
    .into_array();
    let pad = "t".repeat(600 << 10);
    let mut fields = vec![
        PrimitiveArray::from_iter(0..CLASSES.len() as u64).into_array(),
        VarBinArray::from(binary.to_vec()).into_array(),
        DecimalArray::from_option_iter(decimals, DecimalDType::new(38, 6)).into_array(),
        ExtensionArray::new(
            Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
            PrimitiveArray::from_option_iter(days).into_array(),
        )
        .into_array(),
        ExtensionArray::new(
            Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
            PrimitiveArray::from_option_iter(instants).into_array(),
        )
        .into_array(),
        list,
        fixed,
        required_fixed,
        nested,
        VarBinViewArray::from_iter_str(std::iter::repeat_n(pad.as_str(), CLASSES.len()))
            .into_array(),
    ];
    if dictionary {
        for field in &mut fields[1..] {
            *field = DictArray::try_new(
                PrimitiveArray::from_iter([0_u8, 1, 0, 3, 1, 5]).into_array(),
                field.clone(),
            )
            .unwrap()
            .into_array();
        }
    }
    let expected = expected_typed_rows(&decimals, &days, &instants, &pad);
    (
        StructArray::new(
            FieldNames::from(FIELDS),
            fields,
            CLASSES.len(),
            Validity::NonNullable,
        )
        .into_array(),
        expected,
    )
}

fn requests(keys: &[&str]) -> Vec<VortexQueryPrimitiveRequest> {
    let mut requests = vec![request(Kind::DistinctRows, keys)];
    for keep in [Keep::First, Keep::Last, Keep::AllDuplicates] {
        let mut req = request(Kind::DropDuplicateRows, &FIELDS);
        req.deduplicate_key_projection = Some(columns(keys));
        req.duplicate_keep = keep;
        requests.push(req);
        let mut req = request(Kind::DuplicateMaskRows, keys);
        req.duplicate_keep = keep;
        requests.push(req);
    }
    requests
}

fn expected(req: &VortexQueryPrimitiveRequest, keys: &[&str], rows: &[Value]) -> Vec<Value> {
    expected_positions(&CLASSES, req, 0)
        .into_iter()
        .map(|(index, duplicate)| match req.kind {
            Kind::DuplicateMaskRows => json!({"duplicated":duplicate}),
            Kind::DistinctRows => Value::Object(
                keys.iter()
                    .map(|&key| (key.to_owned(), rows[index][key].clone()))
                    .collect(),
            ),
            _ => rows[index].clone(),
        })
        .collect()
}

#[test]
fn unary_membership_spilled_typed_and_dictionary_keys_preserve_values_and_null_parents() {
    for dictionary in [false, true] {
        let (array, rows) = typed_source(dictionary);
        for keys in FIELDS[1..9]
            .iter()
            .map(|&field| vec![field, "pad"])
            .chain(std::iter::once(FIELDS[1..].to_vec()))
        {
            for req in requests(&keys) {
                let wanted = expected(&req, &keys, &rows);
                for spill in [false, true] {
                    assert_eq!(
                        raw_membership(&array, &req, spill, true).unwrap(),
                        wanted,
                        "{dictionary}/{keys:?}/{:?}/{:?}/{spill}",
                        req.kind,
                        req.duplicate_keep
                    );
                }
            }
        }
    }
}

#[test]
fn unary_membership_native_file_generation_failure_cleans_then_fresh_preparation_completes() {
    let keys = &FIELDS[1..];
    for req in requests(keys) {
        let (array, rows) = typed_source(true);
        let fixture = Fixture::new(array, 2);
        let directory = fixture.0.join("unary-runs");
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("foreign"), b"preserve").unwrap();
        let mut policy = policy();
        policy.resource_envelope.memory_budget_bytes = 64 << 20;
        let plan = unary(fixture.scan(), req.clone());
        let prepared = with_spill(prepare_relational(&plan, policy).unwrap(), &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let fired = Rc::new(Cell::new(false));
        let observed = Rc::clone(&fired);
        let source = fixture.path();
        AFTER_COUNT_PASS.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move |_| {
                let replacement = source.with_extension("replacement");
                fs::copy(&source, &replacement).unwrap();
                fs::rename(replacement, source).unwrap();
                observed.set(true);
            }));
        });
        let error = prepared
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
            .err()
            .unwrap()
            .to_string();
        assert!(fired.get());
        assert!(error.contains("changed"), "{error}");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert!(AFTER_COUNT_PASS.with(|slot| slot.borrow().is_none()));
        clean(&directory);
        drop(prepared);

        let prepared = with_spill(prepare_relational(&plan, policy).unwrap(), &directory);
        let memory = prepared.session.memory().clone();
        let dtype = prepared.output_dtype().unwrap();
        let mut actual = Vec::new();
        let mut retained = Vec::new();
        let report = prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                assert_eq!(array.dtype(), &dtype);
                actual.extend(values(&array, context)?);
                retained.push(array);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected(&req, keys, &rows));
        assert_eq!(report.runtime.completed_executions, 1);
        assert!(report.spill.as_ref().unwrap().runs_written > 0);
        assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.native_io_certificate.side_effects.fallback_attempted);
        clean(&directory);
        drop((report, prepared));
        assert!(memory.snapshot().reserved_bytes > 0);
        // A cloned nested DType can independently retain the credited file
        // schema flatbuffer; release that explicit owner as well as the arrays.
        drop(dtype);
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
