# SPDX-License-Identifier: Apache-2.0
"""Version-two analytical catalog. Surface, scale, physical layout and trials do not create IDs."""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import sha256
from itertools import combinations, product
import json

from . import expanded_asof, expanded_nested, expanded_reuse, expanded_windows
from .corpus import case, measure
from .semantics import FloatPolicy, ResultContract, ResultType
from .specification import Expression as E, Measure, Order, Relation

VERSION = "analytical-10000-v2"
ORIGINAL = 347
TOTAL = 10_000
SCOPES = ("population", "first_4096", "middle_band", "rare", "broad", "flagged")
BOUNDED = ("prefix_17", "first_4096", "middle_band", "rare")
KEYS = (
    "group_key",
    "category",
    "nullable_flag",
    "medium_key",
    "skew_key",
    "high_key",
    "group_category",
    "group_flag",
)
NUMBERS = ("signed_value", "optional_value", "mostly_null", "nullable_no_null", "run_value")
SORTS = ("row_id", "signed_value", "optional_text", "event_time", "skew_key")
REDUCTIONS = ("sum", "avg", "min", "max", "count", "count_distinct")
DELIVERY = ("count", "key_totals", "top_values", "distinct_values")
SIZES = (7, 31, 127)
FACT = Relation.scan("fact")

# Named 5M requests are fixed before interaction selection. This prevents a
# nearby empty or trivially contradictory factor cell from becoming a proxy
# for a selective, broad, or genuinely grouped workload.
SURROGATES = {
    "scan_sum": (
        "global_reducer",
        dict(
            scope="population",
            key="scalar",
            function="sum",
            arg="signed_value",
            expression="direct",
            rank=False,
        ),
    ),
    "selective_filter": (
        "conjunction_reduction",
        dict(scope="population", other="independent_key", cutoff=10_000, key="scalar", function="sum"),
    ),
    "broad_filter": (
        "disjunction_reduction",
        dict(scope="population", other="independent_key", cutoff=350_000, key="scalar", function="sum"),
    ),
    "low_cardinality_group": (
        "single_key_reducer",
        dict(
            scope="population",
            key="group_key",
            function="sum",
            arg="signed_value",
            expression="direct",
            rank=False,
        ),
    ),
    "high_cardinality_group": (
        "single_key_reducer",
        dict(
            scope="population",
            key="high_key",
            function="count",
            arg="optional_value",
            expression="direct",
            rank=True,
        ),
    ),
    "multikey_group": (
        "multiple_key_reducer",
        dict(
            scope="population",
            key="group_category",
            function="sum",
            arg="signed_value",
            expression="direct",
            rank=False,
        ),
    ),
    "distinct": (
        "distinct_ordered_keys",
        dict(scope="population", key="group_key", arg="short_text", reverse=False, size=127, offset=0),
    ),
    "top_k": (
        "rank_hidden_payload",
        dict(scope="population", sort="signed_value", payload="wide", reverse=True, size=31, offset=0),
    ),
    "string_filter": (
        "text_filtered_reduction",
        dict(
            scope="population",
            arg="unicode_text",
            encoding_length="length",
            comparison="gt",
            cutoff=3,
            key="scalar",
            function="count",
        ),
    ),
    "nullable_aggregate": (
        "global_reducer",
        dict(
            scope="population",
            key="scalar",
            function="avg",
            arg="optional_value",
            expression="direct",
            rank=False,
        ),
    ),
    "join": (
        "entity_inner_group",
        dict(scope="population", build="all", group="group_key", arg="signed_value", function="sum"),
    ),
    "window": (
        "window_rolling_sum",
        dict(
            scope="population",
            partition="group_key",
            reverse=False,
            size=31,
            arg="optional_value",
            distance=3,
        ),
    ),
}


def digest(obj):
    return sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def population(scope):
    item = FACT
    if scope == "population":
        return item
    if scope == "middle_band":
        pred = item.col("row_id").binary("ge", 117).binary("and", item.col("row_id").binary("lt", 3217))
    elif scope == "flagged":
        pred = item.col("nullable_flag")
    elif scope in {"rare", "broad"}:
        pred = item.col("selection_key").binary("lt", 1_000 if scope == "rare" else 350_000)
    else:
        pred = item.col("row_id").binary("lt", {"first_4096": 4096, "prefix_17": 17}[scope])
    return item.where(pred)


def group_keys(label):
    return {
        "scalar": (),
        "group_category": ("group_key", "category"),
        "group_flag": ("group_key", "nullable_flag"),
    }.get(label, (label,))


def aggregate(source, keys, functions, arg):
    measures = []
    for f in functions:
        if f in REDUCTIONS:
            measures.append(measure(source, f, arg, "m_" + f))
        else:
            measures.append(Measure(f, source.col(arg), "m_" + f, ResultType("float64", True)))
    return source.aggregate(keys, measures)


def cut(plan, names, size, reverse=False, offset=0):
    return plan.order(*(Order(k, reverse) for k in dict.fromkeys(names))).take(size, offset)


def expression_case(name, c):
    src = population(c["scope"])
    arg = src.col(c["arg"])
    op = c["function"]
    if name == "integer_expression_order":
        rhs = src.col("group_key") if c["operand"] == "column" else E.literal(11)
        value = arg.binary(op, rhs)
    elif name == "nullable_expression_order":
        other = src.col("signed_value") if c["operand"] == "column" else E.literal(0)
        value = E.call(op, (arg, other), replace(arg.dtype, nullable=op == "nullif"))
    elif name == "text_expression_order":
        dtype = ResultType("int64" if op in ("length", "octet_length") else "utf8", arg.dtype.nullable)
        value = E.call(op, (arg,), dtype)
    else:
        value = E.call(
            ("date_" if c["arg"] == "event_date" else "timestamp_") + op, (arg,), ResultType("int64", False)
        )
    # Keep both a hidden ordering column and a unique tie-breaker in the plan.
    p = src.project(("id", src.col("row_id")), ("ordering", src.col(c["sort"])), ("value", value))
    return cut(p, ("ordering", "id"), c["size"], c["reverse"]).select("id", "value")


def predicate_case(name, c):
    src = population(c["scope"])
    if name == "text_filtered_reduction":
        text = src.col(c["arg"])
        size = E.call(c["encoding_length"], (text,), ResultType("int64", text.dtype.nullable))
        src = src.where(size.binary(c["comparison"], c["cutoff"]))
        keys = group_keys(c["key"])
        p = aggregate(src, keys, (c["function"],), "signed_value")
        return cut(p, keys, 31) if keys else p
    first = src.col("selection_key").binary("lt", c["cutoff"])
    other = (
        src.col(c["other"]).unary("is_null")
        if c["other"] in ("optional_value", "mostly_null")
        else src.col(c["other"]).binary("gt", 500_000)
    )
    src = src.where(first.binary("or" if name == "disjunction_reduction" else "and", other))
    keys = group_keys(c["key"])
    p = aggregate(src, keys, (c["function"],), "optional_value")
    return cut(p, keys, 31) if keys else p


def reduction_case(name, c):
    src = population(c["scope"])
    keys = group_keys(c["key"])
    arg = src.col(c["arg"])
    expr = c.get("expression", "direct")
    if expr == "add_column":
        arg = arg.binary("add", src.col("group_key"))
    elif expr == "coalesce":
        arg = E.call("coalesce", (arg, E.literal(0)), ResultType("int64", False))
    values = src.project(*[(k, src.col(k)) for k in keys], ("value", arg))
    function = c["function"]
    functions = (function, "count") if name == "mixed_state_aggregate" else (function,)
    p = aggregate(values, keys, functions, "value")
    if name == "group_having":
        p = p.where(p.col("m_" + function).binary("gt", 0))
    if not keys:
        return p
    return cut(p, (("m_" + function, *keys) if c.get("rank", False) else keys), 31, c.get("rank", False))


def joined_case(name, c):
    src = population(c["scope"])
    right_name = (
        "relationships"
        if name == "fanout_group"
        else ("products" if "product" in name and "star" not in name else "entities")
    )
    right = Relation.scan(right_name)
    key = "product_id" if right_name == "products" else "entity_id"
    if c["build"] == "selective":
        right = right.where(right.col(key).binary("lt", 517))
    elif c["build"] == "odd_keys":
        right = right.where(right.col(key).binary("mod", 2).binary("eq", 1))
    kind = {
        "entity_left_group": "left",
        "product_left_group": "left",
        "semi_join_group": "semi",
        "anti_join_group": "anti",
        "right_outer_group": "right",
        "full_outer_group": "full",
    }.get(name, "inner")
    lkey = c["group"]
    cols = [("left", lkey, "bucket"), ("left", c["arg"], "value")]
    group = ["bucket"]
    if name.startswith("star_"):
        # Declare both join orders with the same output contract.
        products = Relation.scan("products")
        if name == "star_products_first":
            first = src.join(
                products,
                (("product_id", "product_id"),),
                (
                    *cols,
                    ("left", "entity_id", "entity_id"),
                    ("right", "product_category", "product_category"),
                ),
            )
            j = first.join(
                right,
                (("entity_id", "entity_id"),),
                (
                    ("left", "bucket", "bucket"),
                    ("left", "value", "value"),
                    ("left", "product_category", "product_category"),
                    ("right", "region", "region"),
                ),
            )
        else:
            first = src.join(
                right,
                (("entity_id", "entity_id"),),
                (*cols, ("left", "product_id", "product_id"), ("right", "region", "region")),
            )
            j = first.join(
                products,
                (("product_id", "product_id"),),
                (
                    ("left", "bucket", "bucket"),
                    ("left", "value", "value"),
                    ("right", "product_category", "product_category"),
                    ("left", "region", "region"),
                ),
            )
        group += ["product_category", "region"]
    else:
        if kind not in {"semi", "anti"}:
            attribute = {"entities": "region", "products": "product_category", "relationships": "weight"}[
                right_name
            ]
            cols.append(("right", attribute, "build_value"))
            group.append("build_value")
        j = src.join(right, ((key, key),), cols, kind)
    p = aggregate(j, tuple(group), (c["function"],), "value")
    return cut(p, ("m_" + c["function"], *group), 31, True)


def set_case(name, c):
    a = population(c["scope"]).select(c["key"], c["arg"])
    b = population(c["right_scope"]).select(c["key"], c["arg"])
    op = name.removeprefix("set_").removesuffix("_reduction")
    union = a.set(b, op)
    p = aggregate(union, (c["key"],), (c["function"],), c["arg"])
    return cut(p, ("m_" + c["function"], c["key"]), 31, True)


def ordering_case(name, c):
    src = population(c["scope"])
    if name == "distinct_ordered_keys":
        names = tuple(dict.fromkeys((c["key"], c["arg"])))
        p = src.select(*names).distinct()
        return cut(p, names, c["size"], c["reverse"], c["offset"])
    payload = {
        "numeric": ("signed_value", "metric"),
        "nested": ("items", "detail"),
        "text": ("unicode_text", "short_text"),
        "temporal": ("event_date", "event_time"),
        "wide": ("amount", "category", "flag", "items", "event_time"),
    }[c["payload"]]
    p = cut(src, (c["sort"], "row_id"), c["size"], c["reverse"], c["offset"])
    return p.select("row_id", *payload)


def window_case(name, c):
    f = name.removeprefix("window_")
    src = population(c["scope"])
    p = expanded_windows.window(
        src,
        function=f,
        argument=c.get("arg", "signed_value"),
        partition=c["partition"],
        descending=c["reverse"],
        distance=c.get("distance", 0),
    )
    return cut(p, ("value", "row_id"), c["size"], True)


def asof_case(name, c):
    src, cal = population(c["scope"]), Relation.scan("calendar")
    if c["calendar"] == "weekly":
        cal = cal.where(cal.col("day_index").binary("mod", 7).binary("eq", 0))
    elif c["calendar"] == "later":
        cal = cal.where(cal.col("day_index").binary("ge", 117))
    p = expanded_asof.asof(
        src,
        cal,
        left_key=c["key"],
        right_key="day_index",
        direction=name.removeprefix("asof_"),
        exact=c["exact"],
        tolerance=c["tolerance"],
    )
    if c["result"] == "detail":
        return cut(p, ("row_id",), 31)
    grouped = aggregate(p, ("week_index",), (c["result"],), "row_id")
    return cut(grouped, ("week_index",), 127)


def extra_case(name, c):
    src = population(c["scope"])
    if name == "list_explode_reduction":
        p = expanded_nested.explode(src.select("row_id", c["key"], "items"))
        g = aggregate(p, (c["key"],), (c["function"],), "items")
        return cut(g, ("m_" + c["function"], c["key"]), c["size"], True)
    if name in ("subquery_membership", "subquery_null_aware_anti"):
        other = Relation.scan(c["dimension"])
        if c["build"] == "small":
            other = other.where(other.col("entity_id").binary("lt", 327))
        elif c["build"] == "empty":
            other = other.where(other.col("entity_id").binary("lt", -1))
        p = expanded_nested.membership(
            src, other, left_key=c["probe"], right_key="entity_id", negated=name.endswith("anti")
        )
        g = aggregate(p, (c["key"],), (c["function"],), "signed_value")
        return cut(g, (c["key"],), 31)
    p = src.project(("bucket", src.col(c["key"])), ("value", src.col(c["arg"])))
    p = (
        expanded_reuse.reused(p, c["threshold"])
        if name == "shared_materialized_consumers"
        else expanded_reuse.recursive(p, c["step"], c["depth"])
    )
    g = aggregate(p, ("bucket",), (c["function"],), "value")
    return cut(g, ("bucket",), 31)


@dataclass(frozen=True)
class Space:
    name: str
    family: str
    build: object
    axes: dict
    higher: tuple


def spaces():
    entries = []

    def add(name, family, builder, higher, **axes):
        entries.append(Space(name, family, builder, axes, tuple(higher.split())))

    order = dict(scope=SCOPES, sort=SORTS, reverse=(False, True), size=SIZES)
    add(
        "integer_expression_order",
        "numeric",
        expression_case,
        "arg function operand scope",
        arg=NUMBERS,
        function=("add", "sub", "mul"),
        operand=("column", "literal"),
        **order,
    )
    add(
        "nullable_expression_order",
        "nullable",
        expression_case,
        "arg function operand",
        arg=("optional_value", "mostly_null", "all_null"),
        function=("coalesce", "nullif"),
        operand=("column", "literal"),
        **order,
    )
    add(
        "text_expression_order",
        "strings",
        expression_case,
        "arg function sort",
        arg=("short_text", "unicode_text", "optional_text"),
        function=("length", "octet_length", "lower", "upper", "trim"),
        **order,
    )
    add(
        "calendar_expression_order",
        "temporal",
        expression_case,
        "arg function sort",
        arg=("event_date", "event_time"),
        function=("year", "month", "day"),
        **order,
    )
    for n in ("conjunction_reduction", "disjunction_reduction"):
        add(
            n,
            "predicates",
            predicate_case,
            "other cutoff function",
            scope=SCOPES,
            other=(
                "independent_key",
                "correlated_key",
                "anti_correlated_key",
                "optional_value",
                "mostly_null",
            ),
            cutoff=(100, 10_000, 350_000),
            key=("scalar", "group_key", "category", "group_flag"),
            function=("sum", "count", "count_distinct"),
        )
    add(
        "text_filtered_reduction",
        "strings",
        predicate_case,
        "arg encoding_length cutoff",
        scope=SCOPES,
        arg=("unicode_text", "short_text", "optional_text"),
        encoding_length=("length", "octet_length"),
        comparison=("gt", "eq"),
        cutoff=(0, 3, 8),
        key=("scalar", "category", "group_flag"),
        function=("count", "sum"),
    )
    for n, keys, fns in (
        ("global_reducer", ("scalar",), REDUCTIONS),
        ("single_key_reducer", KEYS[:-2], REDUCTIONS),
        ("multiple_key_reducer", KEYS[-2:], REDUCTIONS),
        ("group_having", KEYS, ("sum", "avg", "max")),
        ("mixed_state_aggregate", KEYS, ("sum", "min", "max", "avg")),
        ("variance_reduction", ("scalar", *KEYS), ("var_pop", "var_samp")),
        ("deviation_reduction", ("scalar", *KEYS), ("stddev_pop", "stddev_samp")),
        ("distinct_additive_reduction", ("scalar", *KEYS), ("sum_distinct", "avg_distinct")),
        ("median_reduction", ("scalar", *KEYS), ("median",)),
    ):
        add(
            n,
            "global_aggregation" if n == "global_reducer" else "grouped_aggregation",
            reduction_case,
            "arg function expression",
            scope=SCOPES,
            key=keys,
            function=fns,
            arg=NUMBERS,
            expression=("direct", "coalesce", "add_column"),
            rank=(False,) if keys == ("scalar",) else (False, True),
        )
    for n in (
        "entity_inner_group",
        "entity_left_group",
        "product_inner_group",
        "product_left_group",
        "right_outer_group",
        "full_outer_group",
        "semi_join_group",
        "anti_join_group",
        "fanout_group",
        "star_products_first",
        "star_entities_first",
    ):
        add(
            n,
            "multiway_joins" if n.startswith("star_") else "joins",
            joined_case,
            "scope build group function",
            scope=SCOPES,
            build=("all", "selective", "odd_keys"),
            group=("group_key", "category", "nullable_flag", "medium_key"),
            arg=("signed_value", "optional_value", "mostly_null"),
            function=("sum", "count", "max", "count_distinct"),
        )
    for n in ("union_all", "union_distinct", "intersect", "except"):
        add(
            "set_" + n + "_reduction",
            "sets",
            set_case,
            "scope right_scope function",
            scope=("population", "first_4096", "rare", "flagged"),
            right_scope=("middle_band", "broad", "prefix_17"),
            key=("group_key", "category", "skew_key", "nullable_flag"),
            arg=("signed_value", "optional_value", "mostly_null"),
            function=("count", "sum", "avg", "max"),
        )
    add(
        "rank_hidden_payload",
        "ordering",
        ordering_case,
        "sort reverse payload offset",
        scope=SCOPES,
        sort=SORTS,
        payload=("numeric", "nested", "text", "temporal", "wide"),
        reverse=(False, True),
        size=SIZES,
        offset=(0, 47, 1009),
    )
    add(
        "distinct_ordered_keys",
        "distinct",
        ordering_case,
        "key arg offset",
        scope=SCOPES,
        key=("group_key", "category", "medium_key", "skew_key"),
        arg=("signed_value", "optional_value", "short_text", "event_date", "flag"),
        reverse=(False, True),
        size=SIZES,
        offset=(0, 47),
    )
    for f in ("row_number", "lag", "lead", "rolling_sum"):
        extra = (
            {}
            if f == "row_number"
            else dict(arg=("signed_value", "optional_value", "mostly_null"), distance=(1, 3, 13))
        )
        add(
            "window_" + f,
            "windows",
            window_case,
            "scope partition reverse" if f == "row_number" else "arg distance partition reverse size",
            scope=(*BOUNDED, "population"),
            partition=("group_key", "category", "nullable_flag", "skew_key"),
            reverse=(False, True),
            size=SIZES,
            **extra,
        )
    for direction in ("backward", "forward"):
        add(
            "asof_" + direction,
            "temporal",
            asof_case,
            "key calendar exact tolerance",
            scope=BOUNDED,
            key=("signed_value", "optional_value", "medium_key", "run_value"),
            calendar=("all", "weekly", "later"),
            exact=(False, True),
            tolerance=(None, 0, 3, 14),
            result=("detail", "sum", "count"),
        )
    add(
        "list_explode_reduction",
        "nested",
        extra_case,
        "key scope function",
        scope=SCOPES,
        key=("group_key", "category", "nullable_flag", "medium_key"),
        function=("sum", "count", "min", "max", "count_distinct"),
        size=SIZES,
    )
    for n in ("subquery_membership", "subquery_null_aware_anti"):
        add(
            n,
            "subqueries",
            extra_case,
            "dimension build probe",
            scope=SCOPES,
            dimension=("entities", "relationships"),
            build=("all", "small", "empty"),
            probe=("entity_id", "product_id", "optional_value"),
            key=("category", "group_key", "nullable_flag"),
            function=("sum", "count", "min"),
        )
    add(
        "shared_materialized_consumers",
        "cte_composition",
        extra_case,
        "scope arg threshold",
        scope=SCOPES,
        key=("group_key", "category", "nullable_flag", "medium_key"),
        arg=NUMBERS,
        threshold=(-500, 0, 731),
        function=("sum", "count", "min"),
    )
    add(
        "recursive_frontier_reduction",
        "cte_composition",
        extra_case,
        "depth step function",
        scope=BOUNDED,
        key=("group_key", "category", "nullable_flag"),
        arg=("signed_value", "optional_value"),
        step=(-3, 1, 7),
        depth=(1, 3, 9),
        function=("sum", "count", "max"),
    )
    return entries


def valid(space, cell):
    # Ranking a single scalar output has no semantics. Directionless row-number
    # arguments and source-only settings are absent from the domain entirely.
    return not (cell.get("key") == "scalar" and cell.get("rank", False))


def signatures(cell, high):
    return {tuple((k, cell[k]) for k in ks) for ks in (*combinations(cell, 2), high)}


def candidate_cells(space):
    names = tuple(space.axes)
    cells = [dict(zip(names, x)) for x in product(*space.axes.values())]
    cells = [c for c in cells if valid(space, c)]
    anchors = [c for s, c in SURROGATES.values() if s == space.name]
    if any(c not in cells for c in anchors):
        raise ValueError("scale anchor outside declared factor space")
    cells.sort(key=lambda c: (c not in anchors, digest((VERSION, space.name, c))))
    universe = set()
    chosen = []
    rest = []
    # The first traversal captures every pair plus the predeclared higher-order
    # interactions. A second traversal fills the frozen family-balanced budget.
    for cell in cells:
        footprint = signatures(cell, space.higher)
        if cell in anchors or footprint - universe:
            chosen.append(cell)
            universe.update(footprint)
        else:
            rest.append(cell)
    return (
        chosen,
        rest,
        dict(
            archetype=space.name,
            feasible_cells=len(cells),
            mandatory=len(chosen),
            pairwise_and_targeted_interactions=len(universe),
            higher_order=list(space.higher),
        ),
    )


def catalog():
    """Compact descriptors. Rebuild plans lazily in the execution worker."""
    definitions = spaces()
    domains = [candidate_cells(s) for s in definitions]
    mandatory = [len(chosen) for chosen, _, _ in domains]
    counts = [len(chosen) + len(rest) for chosen, rest, _ in domains]
    if not sum(mandatory) <= TOTAL - ORIGINAL <= sum(counts):
        raise ValueError("variant budget cannot satisfy the declared factor coverage")
    budget = mandatory.copy()
    for _ in range(TOTAL - ORIGINAL - sum(mandatory)):
        i = min((i for i, n in enumerate(budget) if n < counts[i]), key=budget.__getitem__)
        budget[i] += 1
    rows, reports = [], []
    for s, n, (chosen, rest, report) in zip(definitions, budget, domains):
        cells = chosen + rest[: n - len(chosen)]
        reports.append(dict(report, selected=len(cells)))
        for c in cells:
            ident = "v2_" + s.name + "_" + digest(c)[:16]
            rows.append(dict(id=ident, archetype=s.name, family=s.family, cell=c))
    if len({r["id"] for r in rows}) != TOTAL - ORIGINAL:
        raise ValueError("expanded identity collision")
    return rows, reports


def build(row, definitions=None):
    definitions = definitions or {s.name: s for s in spaces()}
    s = definitions[row["archetype"]]
    c = row["cell"]
    if (
        set(c) != set(s.axes)
        or any(c[k] not in s.axes[k] for k in c)
        or not valid(s, c)
        or row["id"] != "v2_" + s.name + "_" + digest(c)[:16]
    ):
        raise ValueError("unrecognized variant descriptor")
    plan = s.build(s.name, c)
    item = case(
        row["id"],
        s.family,
        plan,
        ordered=True,
        archetype=s.name,
        factors=tuple((k, str(v)) for k, v in sorted(c.items())),
    )
    floats = {
        n: FloatPolicy(absolute=1e-9, relative=2e-12, reason="independent reduction over bounded inputs")
        for n, t in plan.schema
        if t.name == "float64"
        and s.family
        in {
            "global_aggregation",
            "grouped_aggregation",
            "joins",
            "multiway_joins",
            "sets",
            "cte_composition",
            "nested",
            "subqueries",
            "predicates",
            "temporal",
        }
    }
    return replace(item, contract=ResultContract(plan.schema, True, floats))
