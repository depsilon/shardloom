# SPDX-License-Identifier: Apache-2.0
"""Explicit-context notebook SQL that returns the same lazy DataFrame."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import shlex

__all__ = ("install",)


def install(context, shell=None, *, name="shardloom"):
    """Register SQL magic on an existing context and return a removable handle.

    ``%shardloom SELECT ...`` returns a lazy DataFrame. Cell form accepts
    ``%%shardloom --params bindings --into frame`` where bindings is a mapping
    in the notebook namespace. Registration and magic invocation do not execute
    the plan; call collect(), iter_batches() or a writer explicitly.
    """
    from .context import ShardLoomContext
    if not isinstance(context, ShardLoomContext):
        raise TypeError("notebook SQL requires an explicitly selected ShardLoom context")
    context.client._ensure_open()
    if shell is None:
        from IPython import get_ipython
        shell = get_ipython()
    if shell is None:
        raise ValueError("notebook SQL requires an active IPython shell")
    if not isinstance(name, str) or not name.isidentifier():
        raise ValueError("SQL magic name must be an identifier")
    for kind in ("line", "cell"):
        if name in shell.magics_manager.magics[kind]:
            raise ValueError(f"{kind} magic {name!r} is already registered")

    def sql_magic(line, cell=None):
        context.client._ensure_open()
        parameters, target = None, None
        if cell is None:
            statement = line
        else:
            parser = argparse.ArgumentParser(prog=f"%%{name}", add_help=False, exit_on_error=False)
            parser.add_argument("--params")
            parser.add_argument("--into")
            options, unknown = parser.parse_known_args(shlex.split(line))
            if unknown:
                raise ValueError(f"unknown SQL magic arguments: {unknown!r}")
            if options.params:
                parameters = shell.user_ns[options.params]
                if not isinstance(parameters, Mapping):
                    raise TypeError("SQL magic parameters must name a mapping")
            target = options.into
            if target is not None and not target.isidentifier():
                raise ValueError("SQL magic destination must be a variable name")
            statement = cell
        frame = context.sql(statement, parameters=parameters)
        if target:
            shell.user_ns[target] = frame
        return frame

    shell.register_magic_function(sql_magic, magic_kind="line_cell", magic_name=name)
    return _Installation(shell, name, sql_magic)


class _Installation:
    def __init__(self, shell, name, function):
        self.shell, self.name, self.function = shell, name, function

    def close(self):
        for kind in ("line", "cell"):
            registry = self.shell.magics_manager.magics[kind]
            if registry.get(self.name) is self.function:
                del registry[self.name]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
