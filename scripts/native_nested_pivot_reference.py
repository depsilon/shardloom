# SPDX-License-Identifier: Apache-2.0
"""Independent nested-pivot schemas and literal values; no engine execution."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path


ORACLE_ROOT = Path(__file__).resolve().parents[1] / "docs/architecture/fixtures/native-nested-pivot-state"
ORACLES = tuple(ORACLE_ROOT / name for name in ("core-oracles.json", "typed-oracles.json"))


def fixtures():
    from shardloom._result_schema import ResultType as Type

    core, typed = (json.loads(path.read_text()) for path in ORACLES)
    integer = Type("int64", False)
    text = Type("utf8", False)
    numbers = Type("list", True, (Type("int64", True),))
    schemas = {
        "list_index_and_cells": (numbers, text, numbers),
        "list_domains": (text, numbers, integer),
        "nested_extrema_margins": (text, text, numbers),
        "struct_index_fixed_domains": (
            Type("struct", True, (("a", Type("int16", False)), ("b", text))),
            Type("fixed_size_list", False, (integer, 2)),
            Type("struct", True, (("tag", text), ("numbers", replace(numbers, nullable=False)))),
        ),
        "fixed_index_struct_domains": (
            Type("fixed_size_list", True, (Type("int16", False), 2)),
            Type("struct", True, (("a", integer),)),
            Type("list", True, (Type("utf8", True),)),
        ),
    }
    result = {}
    for name, fields in schemas.items():
        section = (core if name in core else typed)[name]
        schema = (("position", integer), *zip(("entity", "category", "amount"), fields))
        rows = [dict(zip(("position", "entity", "category", "amount"), (position, *row)))
                for position, row in enumerate(section["source"])]
        result[name] = {"oracle": section, "schema": schema, "rows": rows}
    return result


def output_schema(fixture, aggregate, columns=None):
    from shardloom._result_schema import ResultType as Type

    source = dict(fixture["schema"])
    columns = columns or fixture["oracle"]["output_columns"]
    value = (Type("uint64", True) if aggregate == "count" else
             Type("float64", True) if aggregate in {"sum", "mean"} or
             (aggregate in {"min", "max"} and source["amount"].name == "int64") else
             replace(source["amount"], nullable=True))
    return tuple((name, source["entity"] if index == 0 else value)
                 for index, name in enumerate(columns))


def records(columns, rows):
    return [dict(zip(columns, row, strict=True)) for row in rows]


def arrow_schema(fields, arrow):
    """Construct the admitted test schema independently of result conversion."""
    def dtype(value):
        if value.name == "list":
            child = value.parameters[0]
            return arrow.list_(arrow.field("item", dtype(child), nullable=child.nullable))
        if value.name == "fixed_size_list":
            child, size = value.parameters
            return arrow.list_(arrow.field("item", dtype(child), nullable=child.nullable), size)
        if value.name == "struct":
            return arrow.struct([arrow.field(name, dtype(child), nullable=child.nullable)
                                 for name, child in value.parameters])
        return arrow.type_for_alias(value.name)

    return arrow.schema([arrow.field(name, dtype(value), nullable=value.nullable) for name, value in fields])
