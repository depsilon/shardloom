"""Diagnostic alternate declarations; no execution, worker imports or optimizer rule.

Caller supplies the admitted query_surface package on sys.path. Original relations
are retained by callers; every variant is rebuilt with its typed declaration DSL.
Nullable deferral is valid only for well-formed admitted integral inputs. It does
not prove preservation of malformed-input decoding/errors on discarded rows.
"""
from query_surface import expanded_catalog, expanded_nested
from query_surface.specification import Relation


SUPPORTED = frozenset({"rank_hidden_payload", "nullable_expression_order",
                       "subquery_membership", "subquery_null_aware_anti"})
PROOF_GAPS = {
    "nullable_expression_order": "Malformed-input/error equivalence on discarded rows is unproved; diagnostic alternate only, not optimizer authorization."
}
INTEGRAL = frozenset({"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"})


def _expect(node, op, arity=1):
    if not isinstance(node, Relation) or node.op != op or len(node.inputs) != arity:
        raise ValueError("unsupported declaration shape: expected " + op)
    return node


def _columns(project):
    _expect(project, "project")
    if any(e.op != "column" or alias != e.value for alias, e in project.args):
        raise ValueError("expected plain column-only selection")
    return {e.value for _, e in project.args}


def _scan_narrow(source, required):
    """Insert only a column selection directly atop Scan, before all filters."""
    if source.op == "scan":
        _expect(source, "scan", 0)
        if required - set(dict(source.schema)):
            raise ValueError("unresolved scan demand")
        return source.select(*(n for n, _ in source.schema if n in required))
    _expect(source, "filter")
    if len(source.args) != 1:
        raise ValueError("unsupported filter arity")
    predicate = source.args[0]
    child = _scan_narrow(source.inputs[0], required | predicate.columns())
    return child.where(predicate)


def _rank(original):
    names = _columns(original)
    bounded = _expect(original.inputs[0], "limit")
    ordered = _expect(bounded.inputs[0], "sort")
    source = ordered.inputs[0]
    demand = names | {key.column for key in ordered.args}
    narrowed = source.select(*(n for n, _ in source.schema if n in demand))
    return narrowed.order(*ordered.args).take(*bounded.args).select(*(n for n, _ in original.schema))


def _nullable(original):
    _columns(original)
    bounded = _expect(original.inputs[0], "limit")
    ordered = _expect(bounded.inputs[0], "sort")
    projected = _expect(ordered.inputs[0], "project")
    items = dict(projected.args)
    if tuple(items) != ("id", "ordering", "value"):
        raise ValueError("unexpected nullable projection aliases")
    value = items["value"]
    if (value.op != "call" or value.value not in {"nullif", "coalesce"}
            or len(value.args) != 2 or value.dtype.name not in INTEGRAL
            or any(e.op not in {"column", "literal"} or e.dtype.name not in INTEGRAL for e in value.args)
            or any(e.op == "literal" and type(e.value) is not int for e in value.args)):
        raise ValueError("nullable alternate requires integral column/literal NULLIF or COALESCE")
    if any(items[n].op != "column" for n in ("id", "ordering")):
        raise ValueError("nullable ordering and identity must be source columns")
    if any(k.column not in {"ordering", "id"} for k in ordered.args):
        raise ValueError("nullable sort depends on computed value")
    source = projected.inputs[0]
    demand = set().union(*(e.columns() for e in items.values()))
    raw = source.select(*(n for n, _ in source.schema if n in demand))
    # Preserve the exact original order tuple, including ties and NULL placement,
    # by changing only its column names back to their source names.
    from query_surface.specification import Order
    order = tuple(Order(items[k.column].value, k.descending, k.nulls) for k in ordered.args)
    late = raw.order(*order).take(*bounded.args)
    return late.project(*((n, items[n]) for n, _ in original.schema))


def _membership(original):
    bounded = _expect(original, "limit")
    ordered = _expect(bounded.inputs[0], "sort")
    grouped = _expect(ordered.inputs[0], "aggregate")
    member = _expect(grouped.inputs[0], "membership_case", 2)
    if len(member.args) != 3:
        raise ValueError("unexpected membership arity")
    left_key, right_key, negated = member.args
    keys, measures = grouped.args
    demand = {"row_id", left_key, *keys}
    for measure in measures:
        if measure.argument is not None:
            demand |= measure.argument.columns()
    left = _scan_narrow(member.inputs[0], demand)
    right = _scan_narrow(member.inputs[1], {right_key})
    result = expanded_nested.membership(left, right, left_key=left_key,
                                        right_key=right_key, negated=negated)
    return result.aggregate(keys, measures).order(*ordered.args).take(*bounded.args)


def alternate(row, original_relation):
    """Return an admitted diagnostic variant; reject noncanonical requests.

    No source path is modified: scans retain their table identity and renderers
    receive the caller's same source mapping. Output names/types/order and range
    arguments are preserved. This performs declaration validation, not execution.
    """
    if not isinstance(row, dict) or row.get("archetype") not in SUPPORTED:
        raise ValueError("unsupported alternate archetype")
    if not isinstance(original_relation, Relation):
        raise TypeError("original_relation must be a typed Relation")
    canonical = expanded_catalog.build(row).relation
    if original_relation != canonical:
        raise ValueError("original relation differs from admitted catalog request")
    name = row["archetype"]
    result = (_rank(original_relation) if name == "rank_hidden_payload" else
              _nullable(original_relation) if name == "nullable_expression_order" else
              _membership(original_relation))
    if result.schema != original_relation.schema or result.sources() != original_relation.sources():
        raise ValueError("variant changed output schema or source identity")
    return result
