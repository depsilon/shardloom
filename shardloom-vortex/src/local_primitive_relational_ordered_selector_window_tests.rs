use super::*;
use crate::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameExclusion as Exclusion,
    VortexRelationalFrameFunction as Function, VortexRelationalFrameOffset as Offset,
    VortexRelationalFrameUnit as Unit, VortexRelationalWindow as Window,
    VortexRelationalWindowExpression as Expression, VortexRelationalWindowFrame as Frame,
    VortexRelationalWindowFunction as Framed,
};
use serde_json::{Value, json};
use shardloom_core::ScalarValue;

#[path = "local_primitive_relational_ordered_selector_lifecycle_tests.rs"]
mod lifecycle;

#[path = "local_primitive_relational_aggregate_filter_tests.rs"]
mod aggregate_filters;
#[path = "local_primitive_relational_collection_tests.rs"]
mod collections;

const IDS: [u64; 8] = [0, 1, 2, 3, 4, 5, 6, 7];
const WINDOW_KEYS: [i32; 8] = [3, 1, 3, 0, 1, 4, 0, 4];
const VALUES: [Option<&str>; 8] = [
    Some("c"),
    None,
    Some("b"),
    Some("d"),
    Some("a"),
    Some("z"),
    Some("e"),
    None,
];
const ARG_KEYS: [Option<u64>; 8] = [
    Some(u64::MAX),
    Some(7),
    Some(u64::MAX - 1),
    Some(2),
    Some(2),
    Some(4),
    None,
    Some(9),
];
const LOCAL_ORDER: [Option<i32>; 8] = [
    None,
    Some(2),
    Some(1),
    Some(3),
    Some(1),
    Some(2),
    Some(0),
    Some(1),
];
const PARTITION_ORDER: [usize; 8] = [3, 6, 1, 4, 0, 2, 5, 7];

fn window_plan(fixture: &Fixture, frame: &Frame, measures: Vec<Measure>) -> VortexRelationalPlan {
    VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![ColumnRef::new("id").unwrap()],
        expressions: measures
            .into_iter()
            .map(|measure| Expression {
                output_column: measure.alias.clone(),
                function: Framed::Framed(Function::Aggregate(Box::new(measure))),
                partition_by: vec![],
                order_by: vec![VortexRelationalOrderKey {
                    column: ColumnRef::new("window_key").unwrap(),
                    descending: false,
                    nulls: None,
                }],
                frame: Some(frame.clone()),
            })
            .collect(),
    }))
}

fn ordered_selectors(local_order: bool, descending: bool) -> Vec<Measure> {
    selectors("value", "arg_key")
        .into_iter()
        .map(|measure| {
            if local_order {
                measure.with_order_by(order("local_order", descending))
            } else {
                measure
            }
        })
        .collect()
}

fn frame_members(unit: Unit, exclusion: Exclusion, row: usize) -> Vec<usize> {
    let current_position = PARTITION_ORDER
        .iter()
        .position(|&candidate| candidate == row)
        .unwrap();
    let peer_group = |ordinal: usize| match WINDOW_KEYS[ordinal] {
        0 => 0usize,
        1 => 1,
        3 => 2,
        4 => 3,
        _ => unreachable!(),
    };
    let current_group = peer_group(row);
    (0..IDS.len())
        .filter(|&candidate| {
            let position = PARTITION_ORDER
                .iter()
                .position(|&ordinal| ordinal == candidate)
                .unwrap();
            let in_frame = match unit {
                Unit::Rows => position.abs_diff(current_position) <= 2,
                Unit::Groups => peer_group(candidate).abs_diff(current_group) <= 1,
                Unit::Range => WINDOW_KEYS[candidate].abs_diff(WINDOW_KEYS[row]) <= 1,
            };
            let included = match exclusion {
                Exclusion::NoOthers => true,
                Exclusion::CurrentRow => candidate != row,
                Exclusion::Group => WINDOW_KEYS[candidate] != WINDOW_KEYS[row],
                Exclusion::Ties => candidate == row || WINDOW_KEYS[candidate] != WINDOW_KEYS[row],
            };
            in_frame && included
        })
        .collect()
}

fn local_ordered(mut selected: Vec<usize>, descending: bool) -> Vec<usize> {
    selected.sort_by_key(|&ordinal| {
        let null_rank = usize::from(LOCAL_ORDER[ordinal].is_none());
        let local = LOCAL_ORDER[ordinal].unwrap_or_default();
        let value_rank = if descending { -local } else { local };
        let position = PARTITION_ORDER
            .iter()
            .position(|&candidate| candidate == ordinal)
            .unwrap();
        (null_rank, value_rank, position)
    });
    selected
}

fn selected_value(
    function: &str,
    mut selected: Vec<usize>,
    order: Option<bool>,
) -> Option<&'static str> {
    if let Some(descending) = order {
        selected = local_ordered(selected, descending);
    } else {
        selected.sort_by_key(|&ordinal| {
            PARTITION_ORDER
                .iter()
                .position(|&candidate| candidate == ordinal)
                .unwrap()
        });
    }
    match function {
        "first" => selected.first().and_then(|&row| VALUES[row]),
        "last" => selected.last().and_then(|&row| VALUES[row]),
        "arg_min" | "arg_max" => {
            let mut best: Option<usize> = None;
            for row in selected {
                if VALUES[row].is_none() || ARG_KEYS[row].is_none() {
                    continue;
                }
                let Some(prior) = best else {
                    best = Some(row);
                    continue;
                };
                let current_key = ARG_KEYS[row].unwrap();
                let prior_key = ARG_KEYS[prior].unwrap();
                let improves = if function == "arg_min" {
                    current_key < prior_key
                } else {
                    current_key > prior_key
                };
                if improves {
                    best = Some(row);
                }
            }
            best.and_then(|row| VALUES[row])
        }
        _ => unreachable!(),
    }
}

fn expected(unit: Unit, exclusion: Exclusion, local_order: Option<bool>) -> Vec<Value> {
    IDS.iter()
        .enumerate()
        .map(|(row, &id)| {
            let selected = frame_members(unit, exclusion, row);
            json!({
                "id": id,
                "first": selected_value("first", selected.clone(), local_order),
                "last": selected_value("last", selected.clone(), local_order),
                "arg_min": selected_value("arg_min", selected.clone(), local_order),
                "arg_max": selected_value("arg_max", selected, local_order),
            })
        })
        .collect()
}

#[test]
fn ordered_selector_windows_match_independent_frame_and_tie_references() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "window_key", "value", "arg_key", "local_order"]),
            vec![
                PrimitiveArray::from_iter(IDS).into_array(),
                PrimitiveArray::from_iter(WINDOW_KEYS).into_array(),
                VarBinViewArray::from_iter_nullable_str(VALUES).into_array(),
                PrimitiveArray::from_option_iter(ARG_KEYS).into_array(),
                PrimitiveArray::from_option_iter(LOCAL_ORDER).into_array(),
            ],
            IDS.len(),
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );

    for unit in [Unit::Rows, Unit::Groups, Unit::Range] {
        for exclusion in [
            Exclusion::NoOthers,
            Exclusion::CurrentRow,
            Exclusion::Group,
            Exclusion::Ties,
        ] {
            let distance = if unit == Unit::Rows { 2 } else { 1 };
            let frame = Frame {
                unit,
                exclusion,
                start: Bound::Preceding(Offset::Number(ScalarValue::UInt64(distance))),
                end: Bound::Following(Offset::Number(ScalarValue::UInt64(distance))),
            };
            for local_order in [None, Some(false), Some(true)] {
                let measures =
                    ordered_selectors(local_order.is_some(), local_order.unwrap_or(false));
                let plan = window_plan(&fixture, &frame, measures);
                check(&fixture, &plan, &expected(unit, exclusion, local_order));
                let prepared = prepare_relational(&plan, policy()).unwrap();
                let dtype = prepared.output_dtype().unwrap();
                let fields = dtype.as_struct_fields_opt().unwrap();
                for name in ["first", "last", "arg_min", "arg_max"] {
                    assert_eq!(
                        fields.field(name).unwrap(),
                        DType::Utf8(Nullability::Nullable),
                        "unit={unit:?} exclusion={exclusion:?} local_order={local_order:?} field={name}"
                    );
                }
            }
        }
    }
}
