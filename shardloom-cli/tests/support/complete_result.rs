use serde_json::Value;

/// Read the complete structured result rows from a successful CLI report.
///
/// Core collect producers currently expose either JSONL text or a JSON array
/// of rows in report fields. Human-readable summaries are deliberately not a
/// result payload source.
pub(super) fn rows(report: &Value) -> Vec<Value> {
    assert_eq!(report["status"], "success", "{report}");
    assert_eq!(field_value(report, "result_payload_complete"), "true");
    let fields = report["fields"].as_array().expect("report fields");

    if let Some(field) = fields.iter().find(|field| field["key"] == "result_jsonl") {
        let jsonl = field["value"].as_str().expect("result_jsonl field value");
        return jsonl
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("result_jsonl row"))
            .collect();
    }

    let values_json = fields
        .iter()
        .find(|field| field["key"] == "result_values_json")
        .and_then(|field| field["value"].as_str())
        .expect("complete structured result payload field");
    serde_json::from_str::<Value>(values_json)
        .expect("result_values_json payload")
        .as_array()
        .expect("result_values_json row array")
        .clone()
}

pub(super) fn field_value<'a>(report: &'a Value, name: &str) -> &'a str {
    report["fields"]
        .as_array()
        .expect("report fields")
        .iter()
        .find(|field| field["key"] == name)
        .unwrap_or_else(|| panic!("missing report field {name}: {report}"))["value"]
        .as_str()
        .unwrap_or_else(|| panic!("report field {name} is not a string: {report}"))
}
