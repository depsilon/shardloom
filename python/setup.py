"""Setuptools command hooks for ShardLoom Python wheels."""

from __future__ import annotations

import os
import platform
import re

from setuptools import setup

try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # pragma: no cover - setuptools sdist path without wheel
    setup()
else:
    _ARCH_ALIASES = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "arm64": "aarch64",
    }

    def _normalized_arch() -> str:
        arch = platform.machine().strip().lower() or "unknown"
        return _ARCH_ALIASES.get(arch, arch).replace("-", "_")

    def _manylinux_platform_tag() -> str | None:
        if platform.system().strip().lower() != "linux":
            return None
        libc_name, libc_version = platform.libc_ver()
        if libc_name != "glibc" or not libc_version:
            return None
        match = re.match(r"^(\d+)\.(\d+)", libc_version)
        if match is None:
            return None
        major, minor = match.groups()
        return f"manylinux_{major}_{minor}_{_normalized_arch()}"

    class bdist_wheel(_bdist_wheel):
        """Mark wheels as platform-specific when a bundled CLI binary is present."""

        def finalize_options(self) -> None:
            super().finalize_options()
            self.root_is_pure = False
            explicit_plat_name = os.environ.get("SHARDLOOM_WHEEL_PLAT_NAME")
            if explicit_plat_name:
                self.plat_name = explicit_plat_name
                return
            manylinux_tag = _manylinux_platform_tag()
            if manylinux_tag is not None:
                self.plat_name = manylinux_tag

    setup(cmdclass={"bdist_wheel": bdist_wheel})
