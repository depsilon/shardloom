"""Load an explicitly supplied local extension binary without copying/installing it."""

import hashlib
import importlib.util
from importlib.machinery import ExtensionFileLoader
from pathlib import Path
import sys


def load(path: Path):
    path = path.resolve(strict=True)
    if not path.is_file() or path.stat().st_size > 256 * 1024 * 1024:
        raise ValueError("explicit extension must be a regular file no larger than 256 MiB")
    name = "_shardloom_native_experiment"
    if name in sys.modules:
        if Path(sys.modules[name].__file__).resolve() != path:
            raise ValueError("another native extension generation is already loaded")
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path, loader=ExtensionFileLoader(name, str(path)))
    if spec is None or spec.loader is None:
        raise ValueError("explicit native extension could not be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[name] = module
    if module.API_STATUS != "local_unpublished_experiment" or module.FALLBACK_EXECUTION_ALLOWED is not False:
        raise ValueError("native extension experiment/no-fallback contract differs")
    return module


def sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()
