# SPDX-License-Identifier: Apache-2.0
"""Public conversion checks over real native executions and independent values."""
from __future__ import annotations

import importlib
from unittest import mock


MATERIALIZATIONS = ("python", "pandas", "numpy", "arrow", "arrow_ipc")
_PACKAGES = {"pandas": "pandas", "numpy": "numpy", "arrow": "pyarrow", "arrow_ipc": "pyarrow"}
_TERMINALS = {"python": "to_python_objects", "pandas": "to_pandas", "numpy": "to_numpy",
              "arrow": "to_arrow_table", "arrow_ipc": "to_arrow_ipc"}


def dependencies(materializations):
    """Requested conversions must have their real optional dependencies."""
    return {name: importlib.import_module(name).__version__
            for name in sorted({_PACKAGES[item] for item in materializations if item in _PACKAGES})}


def verify_materializations(context, workflow, expected, columns, *, name, materializations,
                            guard, accepted, complete):
    """Record each real engine report and prove the conversion executes once."""
    results = {}
    for conversion in materializations:
        guard()
        label = f"{name}-convert-{conversion}"
        reports = []
        execute = context.client.public_workflow_run

        def capture(*args, **kwargs):
            report = execute(*args, **kwargs)
            reports.append(report)
            return report

        with mock.patch.object(context.client, "public_workflow_run", side_effect=capture):
            result = getattr(workflow, _TERMINALS[conversion])(check=False)
        if len(reports) != 1:
            raise ValueError(f"{label}: requested conversion executed {len(reports)} times")
        accepted(label, reports[0])
        if conversion == "pandas":
            if list(result.columns) != list(columns):
                raise ValueError(f"{label}: pandas column order differs")
            actual = result.to_dict(orient="records")
        elif conversion == "numpy":
            if result.shape != (len(expected), len(columns)):
                raise ValueError(f"{label}: NumPy shape differs: {result.shape!r}")
            actual = [dict(zip(columns, values)) for values in result.tolist()]
        elif conversion in {"arrow", "arrow_ipc"}:
            if conversion == "arrow_ipc":
                pyarrow = importlib.import_module("pyarrow")
                table = pyarrow.ipc.open_stream(pyarrow.BufferReader(result)).read_all()
            else:
                table = result
            if table.column_names != list(columns):
                raise ValueError(f"{label}: Arrow column order differs")
            actual = table.to_pylist()
        else:
            actual = list(result)
        complete(label, actual, expected)
        results[conversion] = result
    return results
