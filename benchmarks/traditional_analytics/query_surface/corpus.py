# SPDX-License-Identifier: Apache-2.0
"""Reviewed archetypes and generated neighbors, including visible capability gaps."""
from __future__ import annotations

from dataclasses import replace
import hashlib

from .coverage import covering_selection, feasible_cells
from .semantics import FloatPolicy, ResultContract, ResultType
from .specification import Case, Expression as E, Measure, Order, Relation


def measure(relation, function, column=None, alias=None):
    argument = relation.col(column) if isinstance(column, str) else column
    if function in {"count", "count_distinct"}:
        dtype = ResultType("uint64", False)
    elif function in {"sum", "avg"}:
        if argument.dtype.name == "decimal128":
            dtype = ResultType("decimal128", True, (38, argument.dtype.parameters[1]))
        else:
            dtype = ResultType("float64", True)
    else:
        dtype = replace(argument.dtype, nullable=True)
    return Measure(function, argument, alias or function, dtype)


def case(name, family, relation, *, factors=(), ordered=False, archetype=None, neighbor=None):
    floats = {name: FloatPolicy(absolute=1e-10, relative=2e-12,
                               reason="independent fsum/reference versus native sum or division order")
              for name, dtype in relation.schema if dtype.name.startswith("float")}
    # Projection and exact binary-fraction arithmetic need no tolerance.
    if family not in {"global_aggregation", "grouped_aggregation", "composite"}:
        floats = {}
    partition = "held_out" if int(hashlib.sha256(name.encode()).hexdigest()[:8], 16) % 5 == 0 else "development"
    return Case(name, family, archetype or name, relation, ResultContract(relation.schema, ordered, floats),
                tuple(factors), partition=partition, neighbor_of=neighbor)


AGGREGATE_FACTORS = {
    "argument": ("signed_value", "optional_value", "nullable_no_null", "metric", "all_null"),
    "keys": ("scalar", "group_key", "category", "group_key_category"),
    "measures": ("sum", "mixed"),
    "expression": ("direct", "arithmetic", "coalesce"),
    "filter": ("all", "one_percent"),
}


def aggregate_cells():
    return feasible_cells(AGGREGATE_FACTORS)


def aggregate_case(cell):
    source = Relation.scan("fact")
    if cell["filter"] == "one_percent":
        source = source.where(source.col("selection_key").binary("lt", 10000))
    value = source.col(cell["argument"])
    one = E.literal(1.0 if value.dtype.name.startswith("float") else 1)
    if cell["expression"] == "arithmetic":
        value = value.binary("add", one)
    elif cell["expression"] == "coalesce":
        value = E.call("coalesce", (value, one), replace(value.dtype, nullable=False))
    keys = {"scalar": (), "group_key_category": ("group_key", "category")}.get(cell["keys"], (cell["keys"],))
    functions = ("sum",) if cell["measures"] == "sum" else ("sum", "avg", "min", "max", "count")
    plan = source.aggregate(keys, [measure(source, function, value, "value_" + function) for function in functions])
    if keys:
        plan = plan.order(*(Order(key) for key in keys))
    name = "aggregate_" + "_".join(cell.values())
    neighbor = None
    if cell["measures"] == "mixed":
        neighbor = "aggregate_" + "_".join("sum" if key == "measures" else value for key, value in cell.items())
    return case(name, "grouped_aggregation" if keys else "global_aggregation", plan,
                factors=tuple(cell.items()), ordered=bool(keys), archetype="aggregate_composition", neighbor=neighbor)


def reviewed_cases():
    fact = Relation.scan("fact")
    cases = []
    def add(name, family, plan, **kwargs):
        cases.append(case(name, family, plan, **kwargs))
    # A unique row_id is an explicit tie breaker for every limited sort.
    for label, columns in (("narrow", ("row_id",)), ("reordered", ("category", "signed_value", "row_id")),
                           ("rich", ("row_id", "amount", "items", "detail", "event_date", "event_time")),
                           ("wide", tuple(dict(fact.schema)))):
        add("projection_" + label, "projection", fact.select(*columns).order(Order("row_id")).take(31), ordered=True)
    aliases = fact.project(("id", fact.col("row_id")), ("value", fact.col("signed_value")),
                           ("label", fact.col("category")))
    add("projection_alias_filter", "projection", aliases.where(aliases.col("value").binary("gt", 0))
        .order(Order("id")).take(11), ordered=True)
    add("projection_constant", "projection", fact.project(("row_id", fact.col("row_id")),
        ("constant", E.literal(7))).order(Order("row_id")).take(11), ordered=True)
    for operation, threshold in (("eq", 1), ("ne", 1), ("lt", 0), ("le", 0), ("gt", 500), ("ge", 500)):
        predicate = fact.col("signed_value").binary(operation, threshold)
        add("predicate_" + operation, "predicates", fact.where(predicate).select("row_id", "signed_value")
            .order(Order("row_id")).take(101), ordered=True)
    low, high = fact.col("signed_value").binary("ge", -17), fact.col("signed_value").binary("le", 23)
    for label, predicate in (("and", low.binary("and", high)), ("or", low.unary("not").binary("or", high.unary("not"))),
                             ("not", low.binary("and", high).unary("not"))):
        add("predicate_" + label, "predicates", fact.where(predicate).select("row_id")
            .order(Order("row_id")).take(101), ordered=True)
    for cutoff in (0, 100, 10000, 100000, 500000, 1000000):
        filtered = fact.where(fact.col("selection_key").binary("lt", cutoff))
        add(f"selectivity_{cutoff}", "predicates", filtered.aggregate((), (measure(filtered, "count", alias="rows"),)),
            archetype="selectivity_sweep", factors=(("cutoff", str(cutoff)),))
    add("predicate_one_row", "predicates", fact.where(fact.col("row_id").binary("eq", 7)).select("row_id", "signed_value"))
    for column in ("optional_value", "mostly_null", "all_null", "nullable_no_null"):
        for operation in ("is_null", "is_not_null"):
            filtered = fact.where(fact.col(column).unary(operation))
            add(f"nullable_{column}_{operation}", "nullable", filtered.aggregate((), (measure(filtered, "count", alias="rows"),)),
                archetype="nullable_predicate", factors=(("column", column), ("operator", operation)))
    for operation in ("add", "sub", "mul", "mod"):
        plan = fact.project(("row_id", fact.col("row_id")), ("result", fact.col("signed_value").binary(operation, 7)))
        add("numeric_" + operation, "numeric", plan.order(Order("row_id")).take(101), ordered=True)
    for name, column, dtype in (
        ("length", "unicode_text", ResultType("int64", False)),
        ("octet_length", "unicode_text", ResultType("int64", False)),
        ("trim", "short_text", ResultType("utf8", False)),
        ("upper", "short_text", ResultType("utf8", False)),
        ("lower", "unicode_text", ResultType("utf8", False)),
    ):
        plan = fact.project(("row_id", fact.col("row_id")), ("result", E.call(name, (fact.col(column),), dtype)))
        add("string_" + name, "strings", plan.order(Order("row_id")).take(101), ordered=True)
    for name, column in (("date_year", "event_date"), ("date_month", "event_date"), ("date_day", "event_date"),
                         ("timestamp_year", "event_time"), ("timestamp_hour", "event_time"), ("timestamp_second", "event_time")):
        plan = fact.project(("row_id", fact.col("row_id")),
                            ("result", E.call(name, (fact.col(column),), ResultType("int64", False))))
        add("temporal_" + name, "temporal", plan.order(Order("row_id")).take(101), ordered=True)
    for function in ("count", "sum", "avg", "min", "max", "count_distinct"):
        add("global_" + function, "global_aggregation", fact.aggregate((), (measure(fact, function, "optional_value", "value"),)))
    add("global_count_star", "global_aggregation", fact.aggregate((), (measure(fact, "count", alias="rows"),)))
    for keys in (("group_key",), ("category",), ("group_key", "category"), ("optional_text",)):
        plan = fact.select(*keys).distinct().order(*(Order(key) for key in keys))
        add("distinct_" + "_".join(keys), "distinct", plan, ordered=True, archetype="row_deduplication",
            factors=(("keys", ",".join(keys)),))
    for count in (0, 1, 10, 11, 100):
        for offset in (0, 7):
            plan = fact.select("row_id", "signed_value").order(Order("signed_value"), Order("row_id")).take(count, offset)
            add(f"topk_{count}_offset_{offset}", "ordering", plan, ordered=True, archetype="topk_boundary",
                factors=(("limit", str(count)), ("offset", str(offset))))
    entities = Relation.scan("entities")
    for kind in ("inner", "left", "right", "full", "semi", "anti"):
        columns = [("left", "row_id", "row_id"), ("left", "entity_id", "entity_id")]
        if kind not in {"semi", "anti"}:
            columns.append(("right", "region", "region"))
        # Preserve outer unmatched rows, but bound complete output on every scale.
        joined = fact.where(fact.col("row_id").binary("lt", 101)).join(
            entities.where(entities.col("entity_id").binary("lt", 103)), (("entity_id", "entity_id"),), columns, kind)
        add("join_" + kind, "joins", joined, archetype="equi_join", factors=(("kind", kind),))
    relationships = Relation.scan("relationships")
    fanout = fact.where(fact.col("row_id").binary("lt", 101)).join(relationships,
        (("entity_id", "entity_id"),), (("left", "row_id", "row_id"), ("right", "relationship_id", "relationship_id")))
    add("join_duplicate_fanout", "joins", fanout)
    for kind in ("union_all", "union_distinct", "intersect", "except"):
        left = fact.where(fact.col("row_id").binary("lt", 101)).select("group_key")
        right = fact.where(fact.col("row_id").binary("ge", 51).binary("and", fact.col("row_id").binary("lt", 151))).select("group_key")
        add("set_" + kind, "sets", left.set(right, kind).order(Order("group_key")), ordered=True)
    return cases


def composition_neighbors():
    """Prospective cohort prompted by the initial 1M observations.

    Keep original reproducers unchanged. Equal-size payload/limit neighbors
    isolate composition, while full-population joins avoid mistaking a selective
    101-row join for a large probing/fanout workload. All are development cases;
    later held-out seeds/layouts remain a separate acceptance cohort.
    """
    fact, entities, products = (Relation.scan(name) for name in ("fact", "entities", "products"))
    cases = []
    def add(name, family, relation, *, archetype=None, factors=(), neighbor=None, ordered=False):
        cases.append(replace(case(name, family, relation, archetype=archetype, factors=factors,
                                  neighbor=neighbor, ordered=ordered), partition="development"))
    payloads = (
        ("direct", "signed_value", lambda source: source.col("signed_value")),
        ("arithmetic", "signed_value", lambda source: source.col("signed_value").binary("add", 0)),
        ("coalesce", "optional_value", lambda source: E.call("coalesce", (source.col("optional_value"), E.literal(0)), ResultType("int64", False))),
        ("string", "unicode_text", lambda source: E.call("lower", (source.col("unicode_text"),), ResultType("utf8", False))),
        ("temporal", "event_date", lambda source: E.call("date_year", (source.col("event_date"),), ResultType("int64", False))),
    )
    for label, column, expression in payloads:
        for count in (11, 31, 100):
            name = f"projection_limit_{label}_{count}"
            plan = fact.project(("row_id", fact.col("row_id")), ("value", expression(fact)))
            add(name, "projection", plan.order(Order("row_id")).take(count), ordered=True,
                archetype="projection_limit_composition", factors=(("expression", label), ("argument", column),
                    ("sort_key", "unique_row_id"), ("limit", str(count)), ("input", "full_population")),
                neighbor=f"projection_limit_direct_{count}" if label != "direct" else None)
        source = fact.where(fact.col("row_id").binary("lt", 31))
        plan = source.project(("row_id", source.col("row_id")), ("value", expression(source)))
        add(f"projection_limit_{label}_prefiltered_31", "projection", plan.order(Order("row_id")).take(31),
            ordered=True, archetype="projection_limit_composition", neighbor=f"projection_limit_{label}_31",
            factors=(("expression", label), ("argument", column), ("sort_key", "unique_row_id"),
                     ("limit", "31"), ("input", "explicit_row_id_under_31")))
    base_columns = (("left", "row_id", "row_id"), ("left", "signed_value", "value"),
                    ("left", "product_id", "product_id"), ("right", "region", "region"))
    for kind in ("inner", "left"):
        joined = fact.join(entities, (("entity_id", "entity_id"),), base_columns, kind)
        add("join_population_" + kind + "_count", "joins", joined.aggregate((), (measure(joined, "count", alias="rows"),)),
            factors=(("kind", kind), ("probe", "full_fact"), ("build", "full_entities")))
        grouped = joined.aggregate(("region",), tuple(measure(joined, function, "value", "value_" + function)
                                                        for function in ("sum", "avg", "min", "max", "count")))
        add("join_population_" + kind + "_mixed", "joins", grouped.order(Order("region")), ordered=True,
            factors=(("kind", kind), ("probe", "full_fact"), ("build", "full_entities"), ("measures", "mixed")))
    for kind in ("semi", "anti"):
        joined = fact.join(entities, (("entity_id", "entity_id"),),
                           (("left", "group_key", "group_key"), ("left", "signed_value", "value")), kind)
        grouped = joined.aggregate(("group_key",), (measure(joined, "count", alias="rows"), measure(joined, "sum", "value", "total")))
        add("join_population_" + kind, "joins", grouped.order(Order("group_key")), ordered=True,
            factors=(("kind", kind), ("probe", "full_fact"), ("build", "full_entities")))
    relationships = Relation.scan("relationships")
    fanout = fact.join(relationships, (("entity_id", "entity_id"),),
                      (("left", "group_key", "group_key"), ("left", "signed_value", "value")))
    grouped = fanout.aggregate(("group_key",), (measure(fanout, "count", alias="rows"), measure(fanout, "sum", "value", "total")))
    add("join_population_duplicate_fanout", "joins", grouped.order(Order("group_key")), ordered=True,
        factors=(("probe", "full_fact"), ("build", "full_relationships"), ("fanout", "observed_manifest")))
    joined = fact.join(entities, (("entity_id", "entity_id"),), base_columns)
    star = joined.join(products, (("product_id", "product_id"),),
        (("left", "region", "region"), ("left", "value", "value"), ("right", "product_category", "product_category")))
    grouped = star.aggregate(("region", "product_category"), (measure(star, "sum", "value", "total"),))
    add("star_join", "multiway_joins", grouped.order(Order("region"), Order("product_category")), ordered=True,
        factors=(("join_order", "entities_then_products"), ("probe", "full_fact")))
    first_product = fact.join(products, (("product_id", "product_id"),),
        (("left", "entity_id", "entity_id"), ("left", "signed_value", "value"), ("right", "product_category", "product_category")))
    second_entity = first_product.join(entities, (("entity_id", "entity_id"),),
        (("right", "region", "region"), ("left", "value", "value"), ("left", "product_category", "product_category")))
    grouped = second_entity.aggregate(("region", "product_category"), (measure(second_entity, "sum", "value", "total"),))
    add("star_join_products_first", "multiway_joins", grouped.order(Order("region"), Order("product_category")),
        ordered=True, neighbor="star_join", factors=(("join_order", "products_then_entities"), ("probe", "full_fact")))
    active = entities.where(entities.col("active"))
    joined = fact.join(active, (("entity_id", "entity_id"),), base_columns)
    grouped = joined.aggregate(("region",), (measure(joined, "sum", "value", "total"),))
    report = grouped.where(grouped.col("total").binary("gt", E.literal(0.0)))
    add("composite_active_region_report", "composite", report.order(Order("total", True), Order("region", True)).take(10),
        ordered=True, factors=(("dimension_filter", "active"), ("probe", "full_fact"), ("having", "positive_sum"),
                               ("sort", "two_descending_keys"), ("limit", "10")))
    return cases


# Desired requests remain visible even when the current adapter/reference or
# native capability has not been implemented. These are NOT expected-error passes.
PENDING = (
    ("quoted_identifiers", "projection", 'SELECT row_id AS "event id" FROM fact', "quoted identifier parser/reachability; plain validated identifiers remain the baseline"),
    ("stats_variance", "grouped_aggregation", "SELECT group_key, VAR_SAMP(optional_value) FROM fact GROUP BY group_key", "statistical aggregate semantic and merge contract"),
    ("stats_covariance", "grouped_aggregation", "SELECT COVAR_POP(signed_value, metric), CORR(signed_value, metric) FROM fact", "paired-null statistical state and typed multi-argument declaration"),
    ("stats_quantile", "grouped_aggregation", "SELECT MEDIAN(metric), QUANTILE_CONT(metric, 0.9) FROM fact", "exact quantile interpolation and admitted memory/spill state"),
    ("sum_distinct", "distinct", "SELECT SUM(DISTINCT signed_value), AVG(DISTINCT signed_value) FROM fact", "expanded distinct aggregate state"),
    ("window_running", "windows", "SELECT row_id, SUM(signed_value) OVER (PARTITION BY group_key ORDER BY row_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM fact", "window adapter and independent frame expectation"),
    ("window_neighbors", "windows", "SELECT row_id, LAG(signed_value) OVER (ORDER BY row_id), LEAD(signed_value) OVER (ORDER BY row_id) FROM fact", "window adapter and independent offset expectation"),
    ("subquery_membership", "subqueries", "SELECT row_id FROM fact WHERE entity_id IN (SELECT entity_id FROM entities WHERE active)", "subquery adapter and three-valued expectation"),
    ("nested_cte_parameters", "cte_composition", "WITH a AS (SELECT * FROM fact WHERE row_id < :limit), b(x) AS (SELECT SUM(signed_value) FROM a) SELECT x FROM b", "parameterized composed-request adapter; native binder already exists"),
    ("recursive_cte", "cte_composition", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 100) SELECT SUM(n) FROM r", "recursive termination, cycle and resource contract"),
    ("nested_explode", "nested", "SELECT * FROM EXPLODE((SELECT row_id,items FROM fact), 'items')", "shared reshape adapter and independent nested expectation"),
    ("nested_pivot", "nested", "SELECT * FROM PIVOT((SELECT group_key,category,signed_value FROM fact), '{\"index\":\"group_key\",\"columns\":\"category\",\"values\":\"signed_value\",\"aggregate\":\"sum\"}')", "shared reshape adapter and independent schema expectation"),
    ("composite_report", "composite", "SELECT region,total FROM (SELECT e.region,SUM(f.signed_value) AS total FROM fact f JOIN entities e ON f.entity_id=e.entity_id WHERE e.active GROUP BY e.region) q WHERE total > 0 ORDER BY total DESC,region LIMIT 10", "composed analytic adapter and frozen expectation"),
    ("native_write_readback", "input_output", "SELECT row_id,amount,event_time,items,detail FROM fact", "explicit write_vortex plus independent complete readback request"),
    ("prepared_repeat", "lifecycle", "SELECT group_key,SUM(signed_value) FROM fact GROUP BY group_key", "persistent/prepared modes; fresh-session timing cannot substitute"),
    ("memory_pressure", "resources", "SELECT high_key,COUNT(*) FROM fact GROUP BY high_key", "admitted memory/spill resource cohort with cleanup proof"),
    ("deadline_cancel", "resources", "SELECT * FROM fact ORDER BY unique_text,row_id", "explicit cancellation/deadline observation and subsequent-session health"),
    ("timezone_daily", "temporal", "SELECT DATE_TRUNC('day', event_time AT TIME ZONE 'America/New_York'),COUNT(*) FROM fact GROUP BY 1", "timezone-preserving input and daylight-saving semantics"),
    ("asof_valuation", "temporal", "SELECT * FROM fact f ASOF LEFT JOIN calendar c ON f.event_date >= c.day", "as-of groups/tolerance/tie contract"),
)


def corpus(*, interactions="pairwise", include_pending=True):
    if interactions not in {"pairwise", "all"}:
        raise ValueError("interaction selection must be pairwise or all")
    cells = aggregate_cells()
    cases = reviewed_cases() + composition_neighbors() + [aggregate_case(cell) for cell in
                               (covering_selection(cells) if interactions == "pairwise" else cells)]
    if include_pending:
        cases += [Case(name, family, name, None, None, (), desired_sql=sql, pending_reason=reason)
                  for name, family, sql, reason in PENDING]
    if len({item.id for item in cases}) != len(cases):
        raise ValueError("corpus case identities collided")
    return cases
