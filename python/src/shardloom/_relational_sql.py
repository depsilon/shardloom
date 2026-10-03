"""Ordered lazy stages rendered as derived relations in the native SQL frontend."""

from __future__ import annotations

from typing import TYPE_CHECKING
from dataclasses import dataclass
import json
import re

if TYPE_CHECKING:
    from .query import LazyFrame, WorkflowOperation


def flat_order_is_safe(operations: tuple[WorkflowOperation, ...]) -> bool:
    """A flat SELECT can express only one stage at each SQL evaluation position."""
    positions = {
        "join": 0, "filter": 1, "group_by": 2, "aggregate": 3, "having": 4,
        "window": 5, "select": 6, "with_column": 6, "distinct": 7,
        "sort": 8, "limit": 9,
    }
    previous = -1
    for operation in operations:
        if operation.kind == "with_column":
            return False  # Replacement and expression dependencies bind per stage.
        position = positions.get(operation.kind, -1)
        if position <= previous or operation.right_statement is not None:
            return False
        previous = position
    return True


def computed_projection(
    columns: tuple[str, ...], name: str, expression: str,
) -> tuple[str, ...]:
    """Replace in place, or append, using the preceding stage's visible names."""
    projection = tuple(
        f"{expression} AS {name}" if column == name else column for column in columns
    )
    return projection if name in columns else (*projection, f"{expression} AS {name}")


@dataclass(frozen=True)
class RenderedStages:
    statement: str
    columns: tuple[str, ...] | None


def frame_stages(frame: LazyFrame) -> RenderedStages | None:
    # Deferred import keeps the public dataclasses in query.py as the sole owners.
    from . import query as q

    if not (q._is_declared_local_source(frame.source) or frame.source.source_format == "vortex"):
        return None
    return _render_stages(
        f"SELECT * FROM {q._quote_sql_local_source_path(frame.source.uri)}",
        frame.operations,
        tuple(name for name, _dtype in frame.source.schema) or None,
    )


def render_frame(frame: LazyFrame) -> str | None:
    rendered = frame_stages(frame)
    return rendered.statement if rendered is not None else None


def _output_names(
    projections: tuple[str, ...], previous: tuple[str, ...] | None,
) -> tuple[str, ...] | None:
    from . import query as q

    names: list[str] = []
    for expression in projections:
        if expression == "*":
            if previous is None:
                return None
            names.extend(previous)
            continue
        index = q._find_top_level_sql_keyword_outside_quotes(expression, "as")
        name = expression[index + 2:].strip() if index is not None else expression
        if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)?", name):
            return None
        names.append(name)
    return tuple(names)


def render_stages(
    statement: str, operations: tuple[WorkflowOperation, ...],
    columns: tuple[str, ...] | None = None,
) -> str | None:
    rendered = _render_stages(statement, operations, columns)
    return rendered.statement if rendered is not None else None


def _render_stages(
    statement: str, operations: tuple[WorkflowOperation, ...],
    columns: tuple[str, ...] | None = None,
) -> RenderedStages | None:
    from . import query as q

    statement = statement.strip().removesuffix(";").strip()
    group_by: tuple[str, ...] | None = None
    aggregate_stage = False
    for index, operation in enumerate(operations):
        kind, values = operation.kind, operation.values
        if kind == "set_index":
            continue  # Existing metadata-only index declarations do not alter rows.
        fragments = values
        if kind == "with_column":
            fragments = values[1:]
        if kind in {"select", "aggregate", "group_by", "window", "with_column"} and any(
            not q._sql_fragment_admitted_for_local_source_statement(
                "relational expression", value,
                allowed_keywords=("order by",) if kind == "window" else (),
            ) for value in fragments
        ):
            return None
        if kind == "group_by":
            if group_by is not None:
                return None
            group_by = values
            continue
        if group_by is not None and kind != "aggregate":
            return None
        source = f"({statement}) AS _sl_stage_{index}"
        if kind == "filter":
            statement = f"SELECT * FROM {source} WHERE {values[0]}"
        elif kind == "having":
            if not aggregate_stage:
                return None
            statement += f" HAVING {values[0]}"
        elif kind == "select" or (kind == "expression_project" and operation.projection_sql is not None):
            projection = values if kind == "select" else operation.projection_sql
            if projection is None:
                return None
            statement = f"SELECT {','.join(projection)} FROM {source}"
            columns = _output_names(projection, columns)
        elif kind == "with_column":
            name, expression = values
            if columns is None:
                statement = f"SELECT * REPLACE OR ADD ({expression} AS {name}) FROM {source}"
            else:
                projection = computed_projection(columns, name, expression)
                statement = f"SELECT {','.join(projection)} FROM {source}"
                columns = _output_names(projection, columns)
        elif kind == "window":
            projection = ("*", *values)
            statement = f"SELECT {','.join(projection)} FROM {source}"
            columns = _output_names(projection, columns)
        elif kind == "aggregate":
            projection = (*(group_by or ()), *values)
            statement = f"SELECT {','.join(projection)} FROM {source}"
            if group_by:
                statement += f" GROUP BY {','.join(group_by)}"
            columns = _output_names(projection, columns)
            group_by = None
        elif kind == "sort":
            direction, keys, nulls = q._parse_sort_operation_values(values)
            statement = f"SELECT * FROM {source}" + q._format_order_by_clause(keys, direction, nulls)
        elif kind == "limit":
            statement = f"SELECT * FROM {source} LIMIT {values[0]}"
        elif kind == "distinct":
            statement = f"SELECT * FROM DISTINCT_ROWS(({statement}), '*') AS _sl_stage_{index}"
        elif kind in {"tail", "sample", "drop_duplicates", "duplicate_mask", "expression_project", "melt", "rolling_window", "explode", "pivot"}:
            if kind == "tail":
                function, argument = "TAIL", values[0]
            elif kind == "sample":
                parts = q._sample_operation_parts(values)
                if parts is None:
                    return None
                count, fraction, seed, replace, weights = parts
                payload = {"seed": seed, "replace": replace}
                payload["n" if count is not None else "fraction"] = count if count is not None else fraction
                if weights is not None:
                    payload["weights"] = weights
                function, argument = "SAMPLE", q._sql_string_literal(json.dumps(payload, separators=(",", ":")))
            elif kind in {"drop_duplicates", "duplicate_mask"}:
                keys, keep = q._duplicate_mask_operation_parts(values)
                if keys is None:
                    return None
                function = "DROP_DUPLICATES" if kind == "drop_duplicates" else "DUPLICATED"
                argument = f"{q._sql_string_literal(','.join(keys))}, {q._sql_string_literal(keep)}"
                if kind == "duplicate_mask":
                    columns = ("duplicated",)
            else:
                payload = json.loads(values[0])
                function = {"expression_project": "REWRITE", "melt": "MELT", "rolling_window": "ROLLING", "explode": "EXPLODE", "pivot": "PIVOT"}[kind]
                argument = q._sql_string_literal(values[0])
                if kind == "expression_project":
                    selected = payload["columns"]
                    if selected != "*":
                        columns = tuple(selected.split(",") if isinstance(selected, str) else selected)
                    if columns is not None:
                        for rewrite in payload["rewrites"]:
                            if rewrite["kind"] == "row_number" and rewrite["target_column"] not in columns:
                                columns = (*columns, rewrite["target_column"])
                elif kind == "pivot":
                    columns = None  # Observed domains belong to the native execution.
                elif kind == "melt":
                    columns = (*payload["id_columns"], payload["variable_column"], payload["value_column"])
                elif kind == "explode":
                    if columns is not None and "element_field" in payload:
                        columns = tuple(payload["output_column"] if name == payload["column"] else name
                                        for name in columns)
                else:
                    columns = (payload["output_column"],)
            statement = f"SELECT * FROM {function}(({statement}), {argument}) AS _sl_stage_{index}"
        elif kind == "join":
            if len(values) not in (6, 7):
                return None
            right, left_keys, right_keys, how, left_alias, right_alias = values[:6]
            condition = values[6] if len(values) == 7 else ""
            if how == "cross":
                if left_keys or right_keys or condition:
                    return None
                on = ""
            elif condition:
                if left_keys or right_keys:
                    return None
                on = f" ON {condition}"
            else:
                left, right_columns = left_keys.split(","), right_keys.split(",")
                if len(left) != len(right_columns) or not all((*left, *right_columns)):
                    return None
                on = " ON " + " AND ".join(
                    f"{left_alias}.{l} = {right_alias}.{r}"
                    for l, r in zip(left, right_columns)
                )
            right_source = (
                f"({operation.right_statement})" if operation.right_statement is not None
                else q._quote_sql_local_source_path(right)
            )
            statement = (
                f"SELECT * FROM ({statement}) AS {left_alias} "
                f"{q._sql_join_keyword(how)} {right_source} AS {right_alias}{on}"
            )
            columns = None  # The native binder resolves the exact joined schema.
        else:
            return None
        aggregate_stage = kind == "aggregate"
    return RenderedStages(statement, columns) if group_by is None else None
