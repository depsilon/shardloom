use super::*;
use crate::local_primitives::query_run_store::operator_recovery_tests as recovery;
use std::path::Path;
use vortex::array::arrays::VarBinViewArray;

const ROWS: usize = 50_000;
const OFFSET: usize = 45_003;

fn request(source: &Path, workspace: &Path) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(source.display().to_string()).unwrap(),
        ProjectionRequest::All,
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("priority", true)])
            .with_offset(OFFSET)
            .with_tie_policy(VortexSortTiePolicy::Last)
            .with_spill(VortexSortSpillPolicy::new(workspace, 32 << 20, 4 << 20).unwrap()),
        7,
    )
}

#[test]
fn numeric_sort_recovery_process_child() {
    let Some(input) = recovery::child_input() else {
        return;
    };
    assert_eq!(input.case, "sort");
    execute_vortex_local_primitive_with_policy(
        &request(&input.source, &input.workspace),
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap(),
    )
    .unwrap();
    panic!("sort completed without parking at validated native runs");
}

#[test]
fn numeric_sort_spill_process_death_cleanup_and_exact_restart() {
    let fixture = Workspace::new();
    let workspace = Workspace::new();
    let path = fixture.0.join("shipping.vortex");
    let keys = (0..ROWS)
        .map(|index| i64::try_from((index * 37) % 997).unwrap() - 498)
        .collect::<Vec<_>>();
    let labels = (0..ROWS)
        .map(|index| format!("港-{index}-shipment"))
        .collect::<Vec<_>>();
    let array = StructArray::new(
        ["priority", "shipment_sequence", "destination"].into(),
        vec![
            PrimitiveArray::from_iter(keys.iter().copied()).into_array(),
            PrimitiveArray::from_iter(0..u64::try_from(ROWS).unwrap()).into_array(),
            VarBinViewArray::from_iter_str(labels.iter().map(String::as_str)).into_array(),
        ],
        ROWS,
        Validity::NonNullable,
    )
    .into_array();
    let runtime = local_vortex_runtime(
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap(),
    );
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut output = File::create(&path).unwrap();
    session
        .write_options()
        .blocking(&runtime)
        .write(
            &mut output,
            ArrayIteratorAdapter::new(array.dtype().clone(), [Ok(array)].into_iter()),
        )
        .unwrap();
    drop(output);
    let policy = workspace.policy();
    recovery::recover_operator(
        concat!(module_path!(), "::numeric_sort_recovery_process_child"),
        &workspace.0,
        &path,
        "sort",
        |directory| policy.cleanup_abandoned(directory),
    );
    let request = request(&path, &workspace.0);
    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap(),
    )
    .unwrap();
    let values: serde_json::Value = serde_json::from_str(
        report
            .result_summary
            .as_ref()
            .unwrap()
            .split_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap();
    let mut expected = (0..ROWS).collect::<Vec<_>>();
    expected.sort_unstable_by(|left, right| keys[*right].cmp(&keys[*left]).then(right.cmp(left)));
    let expected = expected[OFFSET..OFFSET + 7]
        .iter()
        .map(|index| {
            serde_json::json!({
                "priority": keys[*index], "shipment_sequence": index, "destination": labels[*index],
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(values["values"], serde_json::json!(expected));
    assert_eq!(values["retained_candidate_rows"], 7);
    let evidence = report.state_budget.native_sort_spill.as_ref().unwrap();
    assert!(evidence.runs_written > 8 && evidence.merge_passes > 0);
    assert_eq!(evidence.runs_written, evidence.runs_validated);
    assert!(evidence.peak_reserved_bytes <= evidence.memory_bytes);
    assert!(evidence.peak_disk_bytes <= evidence.quota_bytes);
    assert!(evidence.owned_cleanup_completed && report.write_io && report.spill_io_performed);
    assert!(!report.arrow_converted && !report.fallback_execution_allowed);
    assert!(local_primitive_native_io_safe(&request, &report));
    workspace.assert_empty();
}
