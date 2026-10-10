"""Caller allocation is explicit, exact, immutable and independent of ambient state."""

import os
import unittest
from unittest.mock import patch

from shardloom.execution_resources import (
    BYTES_PER_GIB, MAX_MEMORY_BYTES, ExecutionResourceLimits, ExecutionResources,
    ShardLoomResourceConfigurationError, resolve_resources,
)


class ExecutionResourcesTests(unittest.TestCase):
    def test_missing_fields_have_a_stable_configuration_diagnostic(self):
        for kwargs, missing in [({}, "memory_gb (or memory_bytes) and max_parallelism"),
                                ({"memory_gb": 16}, "max_parallelism"),
                                ({"max_parallelism": 8}, "memory_gb (or memory_bytes)")]:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ShardLoomResourceConfigurationError) as caught:
                    resolve_resources(**kwargs)
                self.assertIn(missing, str(caught.exception))
                self.assertEqual(caught.exception.diagnostics[0].code, "SL_CONFIGURATION_ERROR")
                self.assertFalse(caught.exception.fallback.attempted)

    def test_exact_bytes_and_binary_gib_do_not_round_platform_allocations(self):
        resources = resolve_resources(memory_bytes=1_500_000_001, max_parallelism=3, origin="platform")
        self.assertEqual(resources.memory_bytes, 1_500_000_001)
        self.assertIsNone(resources.whole_gib)
        whole = ExecutionResources.from_gib(16, 8, origin="context")
        self.assertEqual(whole.memory_bytes, 17_179_869_184)
        self.assertEqual(whole.whole_gib, 16)
        self.assertEqual(whole.memory_origin, "context")
        self.assertEqual(whole.parallelism_origin, "context")

    def test_invalid_supplied_values_never_use_valid_inheritance(self):
        inherited = ExecutionResources.from_gib(16, 8, origin="context")
        for field in ("memory_gb", "memory_bytes", "max_parallelism"):
            for value in (0, -1, True, False, 1.0, "8", [], MAX_MEMORY_BYTES + 1):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ShardLoomResourceConfigurationError):
                        resolve_resources(inherited=inherited, **{field: value})
        with self.assertRaises(ShardLoomResourceConfigurationError):
            resolve_resources(inherited=inherited, memory_gb=MAX_MEMORY_BYTES // BYTES_PER_GIB + 1)
        with self.assertRaises(ShardLoomResourceConfigurationError):
            resolve_resources(inherited=inherited, memory_gb=1, memory_bytes=BYTES_PER_GIB)

    def test_overrides_preserve_each_origin_and_never_mutate_context_allocation(self):
        inherited = ExecutionResources.from_gib(16, 8, origin="context")
        changed = resolve_resources(inherited=inherited, max_parallelism=4)
        self.assertEqual(changed.memory_bytes, inherited.memory_bytes)
        self.assertEqual(changed.memory_origin, "context")
        self.assertEqual(changed.parallelism_origin, "execution_call")
        self.assertEqual(changed.max_parallelism, 4)
        self.assertEqual(inherited.max_parallelism, 8)
        self.assertEqual(resolve_resources(inherited=changed, origin="session"), changed)

    def test_inherited_ceilings_cannot_be_removed_or_raised(self):
        inherited = ExecutionResources.from_gib(1, 2, origin="platform",
            limits=ExecutionResourceLimits(2 * BYTES_PER_GIB, 4))
        for limits in (None, ExecutionResourceLimits(), ExecutionResourceLimits(9 * BYTES_PER_GIB, 12)):
            for supplied in ({"memory_gb": 3}, {"max_parallelism": 5}):
                with self.subTest(limits=limits, supplied=supplied):
                    with self.assertRaises(ShardLoomResourceConfigurationError):
                        resolve_resources(inherited=inherited, limits=limits, **supplied)
        exact = resolve_resources(inherited=inherited, memory_gb=2, max_parallelism=4)
        self.assertEqual(exact.limits, inherited.limits)
        with self.assertRaises(ShardLoomResourceConfigurationError):
            resolve_resources(inherited=exact, limits=ExecutionResourceLimits(BYTES_PER_GIB, None))

    def test_environment_is_only_read_by_explicit_loading_and_never_repaired(self):
        with patch.dict(os.environ, {"SHARDLOOM_MEMORY_GB": "invalid", "SHARDLOOM_MAX_PARALLELISM": "eight"}):
            explicit = resolve_resources(memory_gb=1, max_parallelism=1)
            self.assertEqual(explicit.memory_bytes, BYTES_PER_GIB)
            with self.assertRaises(ShardLoomResourceConfigurationError):
                ExecutionResources.from_env()
        loaded = ExecutionResources.from_env({"SHARDLOOM_MEMORY_BYTES": "1500000001", "SHARDLOOM_MAX_PARALLELISM": "3"})
        self.assertEqual(loaded.memory_bytes, 1_500_000_001)
        self.assertEqual((loaded.memory_origin, loaded.parallelism_origin), ("environment", "environment"))
        for env in ({}, {"SHARDLOOM_MEMORY_GB": "1"}, {"SHARDLOOM_MAX_PARALLELISM": "2"},
                    {"SHARDLOOM_MEMORY_GB": "1", "SHARDLOOM_MEMORY_BYTES": str(BYTES_PER_GIB), "SHARDLOOM_MAX_PARALLELISM": "2"}):
            with self.assertRaises(ShardLoomResourceConfigurationError):
                ExecutionResources.from_env(env)
        for invalid in ("0", "-1", "1.5", "eight", "", " 2", "+2", "２", "9" * 5000):
            with self.subTest(invalid=invalid[:12]):
                with self.assertRaises(ShardLoomResourceConfigurationError):
                    ExecutionResources.from_env({"SHARDLOOM_MEMORY_GB": "1", "SHARDLOOM_MAX_PARALLELISM": invalid})


if __name__ == "__main__":
    unittest.main()
