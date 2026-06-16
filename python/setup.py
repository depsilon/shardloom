"""Setuptools command hooks for ShardLoom Python wheels."""

from __future__ import annotations

from setuptools import setup

try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # pragma: no cover - setuptools sdist path without wheel
    setup()
else:

    class bdist_wheel(_bdist_wheel):
        """Mark wheels as platform-specific when a bundled CLI binary is present."""

        def finalize_options(self) -> None:
            super().finalize_options()
            self.root_is_pure = False

    setup(cmdclass={"bdist_wheel": bdist_wheel})
