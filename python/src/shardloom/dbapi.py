# SPDX-License-Identifier: Apache-2.0
"""PEP 249 connection backed by the independent Apache ADBC driver manager.

Install ``shardloom[adbc]`` and call ``connect(existing_context)``. Only named
scalar bindings (a mapping or a one-row named Arrow batch) are supported. The
context's native plans perform all execution; fetches convert only at the
explicit client boundary. This local adapter has no transaction/SQL write API.
"""

from importlib import import_module

apilevel = "2.0"
threadsafety = 1
paramstyle = "named"
Binary = bytes

__all__ = ("connect", "apilevel", "threadsafety", "paramstyle", "Warning", "Error", "InterfaceError",
    "DatabaseError", "DataError", "OperationalError", "IntegrityError", "InternalError", "ProgrammingError",
    "NotSupportedError", "Date", "Time", "Timestamp", "DateFromTicks", "TimeFromTicks", "TimestampFromTicks", "Binary")

_MANAGER_EXPORTS = frozenset(__all__) - {"connect", "apilevel", "threadsafety", "paramstyle", "Binary"}


def __getattr__(name):
    if name not in _MANAGER_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module("adbc_driver_manager.dbapi"), name)
    globals()[name] = value
    return value


def connect(context, *, autocommit=True):
    """Borrow an existing context; close releases this connection's operations."""
    from adbc_driver_manager.dbapi import NotSupportedError
    if autocommit is not True:
        raise NotSupportedError("ShardLoom local DB-API supports autocommit queries only")
    from adbc_driver_manager import dbapi
    from .adbc import driver_options
    with driver_options(context) as options:
        return dbapi.connect(db_kwargs=options, autocommit=True)
