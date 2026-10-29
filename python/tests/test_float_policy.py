# SPDX-License-Identifier: Apache-2.0
"""The public semantic policy is inert, immutable and independent of resources."""

import unittest
from unittest.mock import patch

import shardloom as sl
from shardloom import functions as f


class FloatPolicyDeclarations(unittest.TestCase):
    def test_policy_selection_validation_inheritance_and_command_transport(self):
        with patch("shardloom._native_backend.load_native", side_effect=AssertionError("eager native load")):
            for policy in ("strict", "extended"):
                client = sl.ShardLoomClient(transport="native", float_policy=policy)
                self.addCleanup(client.close)
                self.assertEqual(client.float_policy, policy)
                for context in (sl.ShardLoomContext(client), sl.context(client=client),
                                sl.context(client=client, float_policy=policy)):
                    self.assertEqual(context.float_policy, policy)
                    with self.assertRaises(AttributeError):
                        context.float_policy = "strict"
                with self.assertRaises(AttributeError):
                    client.float_policy = "strict"
                opposite = "strict" if policy == "extended" else "extended"
                with self.assertRaises(ValueError):
                    sl.context(client=client, float_policy=opposite)
                with self.assertRaises(ValueError):
                    client.run(["status", "--float-policy", opposite])
                args = client._command_args(["status"])
                self.assertEqual("--float-policy" in args, policy == "extended")
                if policy == "extended":
                    self.assertEqual(args[args.index("--float-policy") + 1], policy)
            for invalid in ("", "Extended", " extended", "ieee", None, True, 1):
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    sl.ShardLoomClient(float_policy=invalid)
            for make in (sl.context, sl.ShardLoomContext.from_env, sl.ShardLoomContext.from_repo):
                context = make(float_policy="extended")
                self.addCleanup(context.close)
                self.assertEqual(context.float_policy, "extended")
            context = sl.context(float_policy="extended")
            self.addCleanup(context.close)
            self.assertEqual(context.session(float_policy="extended").client.float_policy, "extended")
            with self.assertRaises(ValueError):
                context.session(float_policy="strict")

    def test_classifiers_are_declarations_with_preserved_source_bindings(self):
        client = sl.ShardLoomClient(binary="unused")
        self.addCleanup(client.close)
        column = sl.scalar_subquery(sl.read_vortex("/unused/data.vortex", client=client).select("x"))
        for name in ("is_nan", "is_infinite", "is_finite", "signbit"):
            left = getattr(f, name)(column)
            right = getattr(column, name)()
            self.assertEqual(str(left), str(right))
            self.assertTrue(left.source_bindings)
            self.assertEqual(str(getattr(f, name)("x")), f"{name.upper()}(x)")
