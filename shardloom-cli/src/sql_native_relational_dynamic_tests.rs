use super::*;

fn pivot_sql(input: &str) -> String {
    let options = r#"{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}"#;
    format!("SELECT * FROM PIVOT(({input}), '{options}') AS p")
}

#[test]
fn native_relational_sql_dynamic_pivot_binds_wildcards_aliases_and_downstream_stages() {
    let path = fixture();
    let input = format!(
        "SELECT value AS entity,'a' AS category,metric AS amount FROM '{path}' ORDER BY value DESC"
    );
    let pivot = pivot_sql(&input);
    verify(
        &format!(
            "SELECT p.entity,p.pivot_a FROM ({pivot}) AS p WHERE pivot_a >= 40.0 ORDER BY entity DESC"
        ),
        &json!([{"p.entity":5,"p.pivot_a":50.0},{"p.entity":4,"p.pivot_a":40.0}]),
    );
    verify(
        &format!("SELECT SUM(pivot_a) AS total FROM ({pivot}) AS p"),
        &json!([{"total":150.0}]),
    );
    verify(
        &format!("SELECT * FROM TAIL(({pivot}), 2) AS t"),
        &json!([{"entity":4,"pivot_a":40.0},{"entity":5,"pivot_a":50.0}]),
    );
    let empty = pivot_sql(&format!("{input} LIMIT 0"));
    verify(&empty, &json!([]));
}

#[test]
fn native_relational_sql_dynamic_pivot_repeated_reshape_uses_actual_schema() {
    let path = fixture();
    let pivot = pivot_sql(&format!(
        "SELECT value AS entity,'a' AS category,metric AS amount FROM '{path}'"
    ));
    let melt = r#"{"id_columns":["entity"],"value_columns":["pivot_a"],"variable_column":"category","value_column":"amount"}"#;
    let second = pivot_sql(&format!("SELECT * FROM MELT(({pivot}), '{melt}') AS m"));
    verify(
        &format!("SELECT entity,pivot_pivot_a FROM ({second}) AS p ORDER BY entity LIMIT 2"),
        &json!([{"entity":1,"pivot_pivot_a":10.0},{"entity":2,"pivot_pivot_a":20.0}]),
    );
}

#[test]
fn native_relational_sql_dynamic_pivot_correlated_domains_are_scoped_to_each_outer_row() {
    let path = fixture();
    let input = format!(
        "SELECT value AS entity,CASE WHEN value <= 3 THEN 'a' ELSE 'b' END AS category,metric AS amount FROM '{path}' WHERE value <= outer.value"
    );
    let pivot = pivot_sql(&input);
    verify(
        &format!(
            "SELECT value FROM '{path}' WHERE value IN (SELECT COUNT(*) AS n FROM ({pivot}) AS p) ORDER BY value"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
    let empty_first = pivot_sql(&input.replace("value <= outer.value", "value < outer.value"));
    verify(
        &format!(
            "SELECT value FROM '{path}' WHERE EXISTS (SELECT 1 FROM ({empty_first}) AS p) ORDER BY value"
        ),
        &json!([{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{path}' WHERE value >= ALL (SELECT entity FROM ({pivot}) AS p) ORDER BY value"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
}

#[test]
fn native_relational_sql_dynamic_pivot_composes_with_join_set_window_and_membership() {
    let path = fixture();
    let pivot = pivot_sql(&format!(
        "SELECT value AS entity,'a' AS category,metric AS amount FROM '{path}'"
    ));
    verify(
        &format!(
            "SELECT p.entity,p.pivot_a FROM ({pivot}) AS p JOIN '{path}' AS r ON p.entity = r.value WHERE r.metric >= 40 ORDER BY p.entity"
        ),
        &json!([{"p.entity":4,"p.pivot_a":40.0},{"p.entity":5,"p.pivot_a":50.0}]),
    );
    verify(
        &format!(
            "SELECT entity FROM ({pivot}) AS p UNION SELECT entity FROM ({pivot}) AS q ORDER BY entity LIMIT 2"
        ),
        &json!([{"entity":1},{"entity":2}]),
    );
    verify(
        &format!(
            "SELECT entity,ROW_NUMBER() OVER (ORDER BY pivot_a DESC) AS position FROM ({pivot}) AS p ORDER BY position LIMIT 2"
        ),
        &json!([{"entity":5,"position":1},{"entity":4,"position":2}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{path}' WHERE value IN (SELECT entity FROM ({pivot}) AS p WHERE pivot_a > 30.0) ORDER BY value"
        ),
        &json!([{"value":4},{"value":5}]),
    );
}
