# SPDX-License-Identifier: Apache-2.0
"""Lazy application API and named compatibility exports for ShardLoom.

The curated application surface is discoverable through __all__ and dir.
Historical top-level names remain resolvable on explicit access; maintainer and
diagnostic surfaces are discoverable through shardloom.testing and
shardloom.diagnostics. Native bindings load only when explicitly requested.
"""

from __future__ import annotations

from importlib import import_module as _import_module
from sys import modules as _sys_modules
from types import ModuleType as _ModuleType

from ._exports import EXPORTS as _EXPORTS
from ._exports import NAMESPACE_MODULES as _NAMESPACE_MODULES
from ._exports import names_for_audience as _names_for_audience
from ._exports import resolve_export as _resolve_export

_APPLICATION_NAMES = tuple(
    name for name in _names_for_audience("application") if name != "__version__"
)
__all__ = tuple(
    sorted({"__version__", "native", "diagnostics", "testing", *_APPLICATION_NAMES})
)
_MISSING = object()


def _load_namespace(name: str) -> object:
    if name not in _NAMESPACE_MODULES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = _import_module(f".{name}", __name__)
    globals()[name] = module
    return module


def __getattr__(name: str) -> object:
    """Resolve a public, diagnostic, or legacy export only when requested."""

    if name in _NAMESPACE_MODULES:
        return _load_namespace(name)
    if name in _EXPORTS:
        value = _resolve_export(name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Expose only the curated application surface and public namespaces."""

    return list(__all__)


class _ShardLoomPackageModule(_ModuleType):
    """Prefer named API exports when Python has cached same-named submodules."""

    def __getattribute__(self, name: str) -> object:
        namespace = _ModuleType.__getattribute__(self, "__dict__")
        exports = namespace.get("_EXPORTS", {})
        if name in exports:
            current = namespace.get(name, _MISSING)
            if current is _MISSING:
                value = _resolve_export(name)
                namespace[name] = value
                return value
            if isinstance(current, _ModuleType):
                package_name = _ModuleType.__getattribute__(self, "__name__")
                if current.__name__ == f"{package_name}.{name}":
                    value = _resolve_export(name)
                    namespace[name] = value
                    return value
            return current
        return _ModuleType.__getattribute__(self, name)


_sys_modules[__name__].__class__ = _ShardLoomPackageModule
