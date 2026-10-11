"""Direct in-process plans and results for the optional native extension.

Declarations are inert; execution requires explicit resources. For example::

    from shardloom.native import ExecutionResources, Relation, Session, col, lit

    session = Session(ExecutionResources(256 << 20, 2))
    source = session.from_range("value", 0, 1, 100_000)
    relation = Relation.scan("memory://values").filter(col("value") > lit(10))
    plan = session.plan(relation, sources=[("memory://values", source)])
    result = plan.collect()

Results implement Arrow's stream protocol and retain their native owner. Use
``result.row_export()`` only when Python row objects are wanted. SQL remains available
through ``session.sql`` and uses the same native binder, executor and writers.
"""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "ExecutionResources", "Expression", "Relation", "Session", "Source", "Plan",
    "Result", "ColumnarStream", "ExecutionStream", "ColumnarBatch", "col", "lit",
    "FrameOffset", "WindowFrame", "WindowExpression",
]


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    try:
        module = import_module("shardloom._native")
    except ModuleNotFoundError as error:
        if error.name != "shardloom._native":
            raise
        raise ImportError(
            "shardloom.native requires a package built with the native Python extension"
        ) from error
    if name == "col":
        return module.Expression.column
    if name == "lit":
        return module.Expression.literal
    return getattr(module, name)


def __dir__():
    return sorted(set(globals()) | set(__all__))
