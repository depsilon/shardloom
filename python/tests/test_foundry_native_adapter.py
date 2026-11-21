# SPDX-License-Identifier: Apache-2.0
"""Contract and real native integration tests for shardloom.foundry."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "python" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from shardloom.errors import ShardLoomError
from shardloom.execution_resources import (
    ExecutionResourceLimits,
    ShardLoomResourceConfigurationError,
)
from shardloom.foundry import (
    FoundryBoundaryError,
    FoundryInputSpec,
    FoundryOutputSpec,
    FoundryRoute,
    IncrementalAggregateSpec,
    IncrementalCheckpointStore,
    execute_foundry_transform,
    execute_incremental_aggregate_transform,
    map_foundry_resources,
    redact_sensitive_text,
)

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


def _optional_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        if os.environ.get("SHARDLOOM_REQUIRE_NATIVE_PYTHON") == "1":
            raise AssertionError(f"pyarrow is required for integration suite: {error}") from error
        raise unittest.SkipTest("optional pyarrow is not installed") from error
    return pa, pq


class _MockArrowInput:
    def __init__(self, table, *, rid="ri.foundry.main.dataset.arrow", branch="master", tx="tx-1"):
        self._table = table
        self.rid = rid
        self.branch = branch
        self.end_transaction_rid = tx

    def arrow(self):
        return self._table


class _MockPathInput:
    def __init__(self, path: Path, *, rid="ri.foundry.main.dataset.path", branch="master", tx="tx-1"):
        self._path = path
        self.rid = rid
        self.branch = branch
        self.end_transaction_rid = tx

    def path(self):
        return self._path


class _MockFilesystem:
    def __init__(self, root: Path):
        self._root = root

    def ls(self):
        out = []
        for entry in sorted(self._root.rglob("*")):
            if entry.is_file():
                out.append(entry.relative_to(self._root).as_posix())
        return out

    def open(self, relpath: str, mode: str = "rb"):
        target = self._root / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        return target.open(mode)


class _MockFilesystemInput:
    def __init__(self, root: Path, *, rid="ri.foundry.main.dataset.fs", branch="master", tx="tx-1"):
        self._root = root
        self.rid = rid
        self.branch = branch
        self.end_transaction_rid = tx

    def filesystem(self):
        return _MockFilesystem(self._root)


class _MockArrowOutput:
    def __init__(
        self,
        dest_file: Path,
        *,
        rid="ri.foundry.main.dataset.out",
        branch="master",
        fail_on_write: Exception | None = None,
    ):
        self.dest_file = dest_file
        self.rid = rid
        self.branch = branch
        self.mode_history: list[str] = []
        self.write_calls = 0
        self.fail_on_write = fail_on_write
        self.last_table = None

    def set_mode(self, mode: str) -> None:
        self.mode_history.append(mode)

    def write_table(self, table) -> None:
        self.write_calls += 1
        if self.fail_on_write is not None:
            raise self.fail_on_write
        import pyarrow.parquet as pq

        self.dest_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.dest_file.with_suffix(".tmp.parquet")
        pq.write_table(table, tmp)
        tmp.replace(self.dest_file)
        self.last_table = table


class _MockPathContextOutput:
    def __init__(self, dest_dir: Path, *, rid="ri.foundry.main.table.iceberg", branch="master"):
        self.dest_dir = dest_dir
        self.rid = rid
        self.branch = branch
        self.modes: list[str] = []
        self.committed = 0
        self._pending_dir: Path | None = None
        self._seq = 0

    def set_mode(self, mode: str) -> None:
        self.modes.append(mode)

    @contextmanager
    def path_for_write_table(self):
        self._seq += 1
        staging = self.dest_dir.parent / f".tx-staging-{self.dest_dir.name}-{self._seq}"
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir(parents=True, exist_ok=True)
        self._pending_dir = staging
        try:
            yield staging
        except BaseException:
            self._pending_dir = None
            shutil.rmtree(staging, ignore_errors=True)
            raise

    def commit(self) -> None:
        if self._pending_dir is None:
            raise RuntimeError("no staged transaction directory to commit")
        staging = self._pending_dir
        self._pending_dir = None
        try:
            self.dest_dir.mkdir(parents=True, exist_ok=True)
            for staged in sorted(staging.glob("*.parquet")):
                tmp = self.dest_dir / f".tmp-{staged.name}"
                shutil.copyfile(staged, tmp)
                tmp.replace(self.dest_dir / staged.name)
            self.committed += 1
        finally:
            shutil.rmtree(staging, ignore_errors=True)


class _MockVortexFsOutput:
    def __init__(self, dest_dir: Path, *, rid="ri.foundry.main.dataset.vortex"):
        self.dest_dir = dest_dir
        self.rid = rid
        self.branch = "master"

    def filesystem(self):
        return _MockFilesystem(self.dest_dir)


class _MockModeAwareIcebergInput:
    """Mock TableInput/IcebergInput supporting mode=('current'|'added'|'previous')."""

    def __init__(self, *, current_dir: Path, added_dir: Path | None = None, previous_dir: Path | None = None, rid="ri.iceberg.tbl.1"):
        self._paths = {"current": current_dir, "added": added_dir, "previous": previous_dir}
        self.rid = rid
        self.branch = "master"

    def path(self, mode: str = "current") -> Path:
        selected = self._paths.get(mode)
        if selected is None:
            raise FileNotFoundError(f"no path staged for mode={mode!r}")
        return selected


class FoundryAdapterContractTests(unittest.TestCase):
    def test_foundry_import_stays_lazy_and_clean(self) -> None:
        script = r"""
import sys
import shardloom as sl
root_dir = dir(sl)
import shardloom.foundry as foundry
assert dir(sl) == root_dir, "importing shardloom.foundry altered top-level dir(shardloom)"
for forbidden in (
    "shardloom._native",
    "shardloom.native",
    "shardloom._native_backend",
    "shardloom.context",
    "shardloom.query",
    "pyarrow",
    "numpy",
    "pandas",
):
    assert forbidden not in sys.modules, f"unexpected eager import: {forbidden}"
assert hasattr(foundry, "execute_foundry_transform")
"""
        env = os.environ.copy()
        env["PYTHONPATH"] = str(SRC)
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_resource_mapping_reserves_headroom_and_rejects_invalid_allocations(self) -> None:
        profile = map_foundry_resources(
            container_memory_gb=16,
            max_parallelism=4,
            python_headroom_fraction=0.25,
            limits=ExecutionResourceLimits(memory_bytes=16 << 30, max_parallelism=8),
        )
        self.assertEqual(profile.container_memory_bytes, 16 << 30)
        self.assertEqual(profile.reserved_headroom_bytes, 4 << 30)
        self.assertEqual(profile.engine_memory_bytes, 12 << 30)
        self.assertFalse(profile.engine_credit_is_container_ceiling)
        self.assertEqual(profile.execution_resources.memory_origin, "platform")
        self.assertEqual(profile.execution_resources.parallelism_origin, "platform")

        with self.assertRaises(ShardLoomResourceConfigurationError):
            map_foundry_resources(container_memory_gb=True, max_parallelism=2)  # type: ignore[arg-type]
        with self.assertRaises(ShardLoomResourceConfigurationError):
            map_foundry_resources(container_memory_bytes=128 << 20, max_parallelism=True)  # type: ignore[arg-type]
        with self.assertRaises(ShardLoomResourceConfigurationError):
            map_foundry_resources(
                container_memory_bytes=256 << 20,
                max_parallelism=2,
                python_headroom_bytes=256 << 20,
            )
        with self.assertRaises(ShardLoomResourceConfigurationError):
            map_foundry_resources(
                container_memory_gb=8,
                max_parallelism=2,
                limits=ExecutionResourceLimits(memory_bytes=4 << 30),
            )

    def test_boundary_and_security_rejections_and_credential_redaction(self) -> None:
        dummy = object()
        for external_kind in ("virtual_table", "external_pushdown", "snowflake", "databricks", "bigquery"):
            with self.assertRaises(FoundryBoundaryError) as ctx_err:
                FoundryInputSpec(alias="src", handle=dummy, resource_kind=external_kind)
            self.assertEqual(ctx_err.exception.code, "SL_FOUNDRY_UNSUPPORTED_RESOURCE")
            self.assertFalse(ctx_err.exception.fallback_attempted)
            self.assertFalse(ctx_err.exception.external_engine_invoked)

        with self.assertRaises(FoundryBoundaryError) as rm_err:
            FoundryInputSpec(alias="src", handle=dummy, read_mode="changelog")
        self.assertEqual(rm_err.exception.code, "SL_FOUNDRY_UNSUPPORTED_READ_MODE")

        with self.assertRaises(FoundryBoundaryError) as wm_err:
            FoundryOutputSpec(handle=dummy, write_mode="changelog")
        self.assertEqual(wm_err.exception.code, "SL_FOUNDRY_UNSUPPORTED_WRITE_MODE")

        with self.assertRaises(FoundryBoundaryError) as op_err:
            IncrementalAggregateSpec(
                source_rid="ri.tbl.1",
                group_by=("k",),
                retracted_operators=("min",),
            )
        self.assertEqual(op_err.exception.code, "SL_INCREMENTAL_UNSUPPORTED_OPERATOR")

        # Naive added + replace aggregate without checkpoint fails explicitly.
        res = map_foundry_resources(container_memory_bytes=128 << 20, max_parallelism=1)
        with self.assertRaises(FoundryBoundaryError) as mode_err:
            execute_incremental_aggregate_transform(
                current_input=FoundryInputSpec(alias="cur", handle=dummy),
                added_input=FoundryInputSpec(alias="add", handle=dummy, read_mode="added"),
                output=FoundryOutputSpec(handle=dummy, write_mode="replace"),
                spec=IncrementalAggregateSpec(source_rid="ri.tbl.1", group_by=("k",)),
                checkpoint_store=None,
                resources=res,
                current_transaction_rid="tx-2",
            )
        self.assertEqual(mode_err.exception.code, "SL_INCREMENTAL_INVALID_MODE")

        # Permission error + secret redaction check.
        raw_secret = "Bearer eyJ.super.secret token=abc123xyz https://svc:topsecret@foundry.internal/api"
        redacted = redact_sensitive_text(raw_secret)
        self.assertNotIn("eyJ.super.secret", redacted)
        self.assertNotIn("abc123xyz", redacted)
        self.assertNotIn("topsecret", redacted)
        self.assertIn("[REDACTED]", redacted)

        class _DeniedInput:
            def path(self):
                raise PermissionError("access denied with Bearer secret-token-999 and api_key=key42")

        with tempfile.TemporaryDirectory(prefix="shardloom-foundry-deny-") as tmp:
            err = FoundryBoundaryError("SL_TEST", "dummy")
            _ = err
            with self.assertRaises(FoundryBoundaryError) as denied_err:
                execute_foundry_transform(
                    inputs=[FoundryInputSpec(alias="src", handle=_DeniedInput())],
                    output=FoundryOutputSpec(handle=_MockArrowOutput(Path(tmp) / "out.parquet")),
                    resources=map_foundry_resources(
                        container_memory_bytes=128 << 20,
                        max_parallelism=1,
                        spill_dir=Path(tmp) / "spill",
                    ),
                    sql="SELECT * FROM src",
                )
            msg = str(denied_err.exception)
            self.assertNotIn("secret-token-999", msg)
            self.assertNotIn("key42", msg)
            self.assertIn("access denied", msg)


class FoundryNativeAdapterIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.native = _load_native()
        cls.pa, cls.pq = _optional_pyarrow()

    def setUp(self) -> None:
        popen_patch = patch("subprocess.Popen", side_effect=AssertionError("external subprocess prohibited"))
        popen_patch.start()
        self.addCleanup(popen_patch.stop)
        tmp = tempfile.TemporaryDirectory(prefix="shardloom-foundry-native-")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.resources = map_foundry_resources(
            container_memory_bytes=128 << 20,
            max_parallelism=2,
            spill_dir=self.root / "job_spill",
        )

    def test_arrow_fixture_transform_and_truthful_report(self) -> None:
        table = self.pa.table(
            {
                "id": self.pa.array([1, 2, 3], type=self.pa.int64()),
                "region": self.pa.array(["us", "eu", "us"]),
                "amount": self.pa.array([10, 20, 35], type=self.pa.int64()),
            }
        )
        inp = _MockArrowInput(table)
        out_file = self.root / "arrow_out.parquet"
        out = _MockArrowOutput(out_file)

        report = execute_foundry_transform(
            inputs=[FoundryInputSpec(alias="sales", handle=inp)],
            output=FoundryOutputSpec(handle=out, output_format="parquet", write_mode="replace"),
            resources=self.resources,
            sql="SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region",
        )
        payload = report.as_dict()
        self.assertEqual(payload["schema_version"], "shardloom.foundry_native_transform_report.v1")
        self.assertEqual(payload["route"], FoundryRoute.SINGLE_NODE_NATIVE_TRANSFORM)
        self.assertEqual(payload["query_engine"], "shardloom_native")
        self.assertFalse(payload["fallback_attempted"])
        self.assertFalse(payload["external_engine_invoked"])
        self.assertFalse(payload["foundry_spark_invoked"])
        self.assertFalse(payload["foundry_runtime_invoked"])
        self.assertFalse(payload["foundry_compute_invoked"])
        self.assertFalse(payload["live_foundry_enrollment_proved"])
        self.assertEqual(payload["claim_gate_status"], "not_claim_grade")
        self.assertTrue(payload["scratch_cleaned"])
        self.assertEqual(out.write_calls, 1)
        self.assertEqual(out.mode_history, ["replace"])

        readback = self.pq.read_table(out_file).to_pylist()
        self.assertEqual(
            readback,
            [
                {"region": "eu", "n": 1, "total": 20.0},
                {"region": "us", "n": 2, "total": 45.0},
            ],
        )

    def test_multi_file_logical_relation_join_and_global_aggregation(self) -> None:
        orders_dir = self.root / "orders"
        customers_dir = self.root / "customers"
        orders_dir.mkdir()
        customers_dir.mkdir()
        (orders_dir / "_SUCCESS").write_text("", encoding="utf-8")

        # Customer 10 spans three separate Parquet files; per-file agg + concat would emit duplicates.
        self.pq.write_table(
            self.pa.table({"oid": [1, 2], "cid": [10, 20], "amt": [100, 200]}),
            orders_dir / "part-000.parquet",
        )
        self.pq.write_table(
            self.pa.table({"oid": [3, 4], "cid": [10, 30], "amt": [50, 75]}),
            orders_dir / "part-001.parquet",
        )
        self.pq.write_table(
            self.pa.table({"oid": [5], "cid": [10], "amt": [25]}),
            orders_dir / "part-002.parquet",
        )
        self.pq.write_table(
            self.pa.table({"cid": [10, 20], "segment": ["enterprise", "smb"]}),
            customers_dir / "part-000.parquet",
        )
        self.pq.write_table(
            self.pa.table({"cid": [30], "segment": ["enterprise"]}),
            customers_dir / "part-001.parquet",
        )

        out_dir = self.root / "joined_out"
        out_handle = _MockPathContextOutput(out_dir)
        report = execute_foundry_transform(
            inputs=[
                FoundryInputSpec(alias="orders", handle=_MockPathInput(orders_dir)),
                FoundryInputSpec(alias="customers", handle=_MockPathInput(customers_dir)),
            ],
            output=FoundryOutputSpec(handle=out_handle, output_format="parquet", write_mode="snapshot"),
            resources=self.resources,
            sql=(
                "SELECT c.segment AS segment, COUNT(*) AS order_count, SUM(o.amt) AS revenue "
                "FROM orders AS o JOIN customers AS c ON o.cid = c.cid "
                "GROUP BY c.segment ORDER BY c.segment"
            ),
        )
        self.assertEqual(report.inputs[0]["physical_file_count"], 3)
        self.assertEqual(report.inputs[1]["physical_file_count"], 2)
        self.assertTrue(all(inp["logical_relation_bound"] for inp in report.inputs))
        self.assertEqual(out_handle.modes, ["replace"])
        self.assertEqual(out_handle.committed, 1)

        readback = self.pq.read_table(out_dir / "part-00000.parquet").to_pylist()
        self.assertEqual(
            readback,
            [
                {"segment": "enterprise", "order_count": 4, "revenue": 250.0},
                {"segment": "smb", "order_count": 1, "revenue": 200.0},
            ],
        )

    def test_filesystem_staged_rich_types_and_empty_dataset_fidelity(self) -> None:
        schema = self.pa.schema(
            [
                self.pa.field("id", self.pa.int64()),
                self.pa.field("amount", self.pa.decimal128(12, 2)),
                self.pa.field("ts", self.pa.timestamp("us")),
                self.pa.field("payload", self.pa.binary()),
                self.pa.field("items", self.pa.list_(self.pa.int32())),
                self.pa.field("meta", self.pa.struct([self.pa.field("k", self.pa.string())])),
            ]
        )
        rich_dir = self.root / "fs_rich"
        (rich_dir / "sub").mkdir(parents=True)
        rows = [
            {
                "id": -(1 << 62),
                "amount": Decimal("1234.56"),
                "ts": datetime(2026, 2, 3, 4, 5, 6),
                "payload": b"\x00\xab\xff",
                "items": [1, None, 9],
                "meta": {"k": "alpha"},
            },
            {
                "id": 1 << 62,
                "amount": None,
                "ts": None,
                "payload": None,
                "items": None,
                "meta": None,
            },
        ]
        self.pq.write_table(self.pa.Table.from_pylist(rows[:1], schema=schema), rich_dir / "a.parquet")
        self.pq.write_table(self.pa.Table.from_pylist(rows[1:], schema=schema), rich_dir / "sub" / "b.parquet")

        out_rich = self.root / "out_rich.parquet"
        out_handle = _MockArrowOutput(out_rich)
        report = execute_foundry_transform(
            inputs=[FoundryInputSpec(alias="events", handle=_MockFilesystemInput(rich_dir))],
            output=FoundryOutputSpec(handle=out_handle),
            resources=self.resources,
            transform=lambda _ctx, rels: rels["events"],
        )
        self.assertEqual(report.inputs[0]["physical_file_count"], 2)
        actual = self.pq.read_table(out_rich)
        self.assertEqual([f.name for f in actual.schema], [f.name for f in schema])
        self.assertEqual(sorted(actual.to_pylist(), key=lambda r: r["id"]), sorted(rows, key=lambda r: r["id"]))

        # Empty dataset preserves exact schema and zero rows.
        empty_dir = self.root / "fs_empty"
        empty_dir.mkdir()
        self.pq.write_table(self.pa.Table.from_pylist([], schema=schema), empty_dir / "part-0.parquet")
        out_empty = self.root / "out_empty.parquet"
        empty_report = execute_foundry_transform(
            inputs=[FoundryInputSpec(alias="events", handle=_MockFilesystemInput(empty_dir))],
            output=FoundryOutputSpec(handle=_MockArrowOutput(out_empty)),
            resources=self.resources,
            transform=lambda _ctx, rels: rels["events"],
        )
        self.assertEqual(empty_report.output["row_count"], 0)
        read_empty = self.pq.read_table(out_empty)
        self.assertEqual(read_empty.num_rows, 0)
        self.assertTrue(read_empty.schema.equals(schema, check_metadata=False))

    def test_failed_publication_preserves_prior_snapshot_and_cleans_scratch(self) -> None:
        in_table = self.pa.table({"k": ["ok"], "v": [42]})
        out_file = self.root / "published.parquet"
        prior_table = self.pa.table({"k": ["prior"], "v": [99]})
        self.pq.write_table(prior_table, out_file)
        prior_bytes = out_file.read_bytes()

        failing_out = _MockArrowOutput(
            out_file, fail_on_write=RuntimeError("simulated commit failure https://user:secretpw@host")
        )
        with self.assertRaises(FoundryBoundaryError) as ctx_err:
            execute_foundry_transform(
                inputs=[FoundryInputSpec(alias="src", handle=_MockArrowInput(in_table))],
                output=FoundryOutputSpec(handle=failing_out),
                resources=self.resources,
                transform=lambda _ctx, rels: rels["src"],
            )
        self.assertNotIn("secretpw", str(ctx_err.exception))
        self.assertEqual(out_file.read_bytes(), prior_bytes)
        self.assertEqual(self.pq.read_table(out_file).to_pylist(), [{"k": "prior", "v": 99}])
        leftover_jobs = list((self.root / "job_spill").glob("shardloom-foundry-job-*"))
        self.assertEqual(leftover_jobs, [])

    def test_managed_iceberg_incremental_lifecycle_vs_full_recomputation(self) -> None:
        import shardloom as sl

        cur_dir = self.root / "iceberg_current"
        add_dir = self.root / "iceberg_added"
        out_dir = self.root / "iceberg_out"
        ckpt_path = self.root / "state" / "ckpt.json"
        cur_dir.mkdir()
        add_dir.mkdir()

        def write_dir(target: Path, rows: list[dict]) -> None:
            for old in target.glob("*.parquet"):
                old.unlink()
            self.pq.write_table(
                self.pa.Table.from_pylist(
                    rows,
                    schema=self.pa.schema(
                        [
                            self.pa.field("region", self.pa.string()),
                            self.pa.field("amount", self.pa.int64()),
                            self.pa.field("_op", self.pa.string()),
                        ]
                    ),
                ),
                target / "part-000.parquet",
            )

        def full_recompute_truth(rows: list[dict]) -> list[dict]:
            if not rows:
                return []
            with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1) as ctx:
                tbl = self.pa.Table.from_pylist(
                    [{"region": r["region"], "amount": r["amount"]} for r in rows],
                    schema=self.pa.schema(
                        [self.pa.field("region", self.pa.string()), self.pa.field("amount", self.pa.int64())]
                    ),
                )
                agg = ctx.sql(
                    "SELECT region, COUNT(*) AS row_count, SUM(amount) AS sum_amount "
                    "FROM s GROUP BY region ORDER BY region",
                    relations={"s": ctx.from_arrow_table(tbl)},
                ).collect(check=True).to_arrow().to_pylist()
                return [
                    {
                        "region": r["region"],
                        "row_count": r["row_count"],
                        "sum_amount": None if r["sum_amount"] is None else int(r["sum_amount"]),
                    }
                    for r in agg
                ]

        spec = IncrementalAggregateSpec(
            source_rid="ri.iceberg.main.table.sales",
            group_by=("region",),
            sum_columns=("amount",),
            delta_op_column="_op",
        )
        ckpt = IncrementalCheckpointStore(ckpt_path)
        out_handle = _MockPathContextOutput(out_dir)

        # tx-001 initial snapshot
        tx1_rows = [
            {"region": "us", "amount": 10, "_op": "insert"},
            {"region": "us", "amount": 5, "_op": "insert"},
            {"region": "eu", "amount": None, "_op": "insert"},
            {"region": "apac", "amount": 100, "_op": "insert"},
        ]
        write_dir(cur_dir, tx1_rows)
        ice_in = _MockModeAwareIcebergInput(current_dir=cur_dir, added_dir=add_dir)
        r1 = execute_incremental_aggregate_transform(
            current_input=FoundryInputSpec(alias="cur", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="current"),
            output=FoundryOutputSpec(handle=out_handle, resource_kind="foundry_managed_iceberg", write_mode="replace"),
            spec=spec,
            checkpoint_store=ckpt,
            resources=self.resources,
            current_transaction_rid="tx-001",
        )
        self.assertEqual(r1.incremental["action"], "initial_snapshot")
        self.assertEqual(self.pq.read_table(out_dir / "part-00000.parquet").to_pylist(), full_recompute_truth(tx1_rows))

        # tx-002 incremental append
        add2_rows = [
            {"region": "us", "amount": 20, "_op": "insert"},
            {"region": "eu", "amount": 7, "_op": "insert"},
        ]
        tx2_rows = tx1_rows + add2_rows
        write_dir(add_dir, add2_rows)
        r2 = execute_incremental_aggregate_transform(
            current_input=FoundryInputSpec(alias="cur", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="current"),
            added_input=FoundryInputSpec(alias="add", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="added"),
            output=FoundryOutputSpec(handle=out_handle, resource_kind="foundry_managed_iceberg", write_mode="replace"),
            spec=spec,
            checkpoint_store=ckpt,
            resources=self.resources,
            current_transaction_rid="tx-002",
            previous_transaction_rid="tx-001",
        )
        self.assertEqual(r2.incremental["action"], "incremental_retraction_and_append")
        self.assertEqual(self.pq.read_table(out_dir / "part-00000.parquet").to_pylist(), full_recompute_truth(tx2_rows))

        # Duplicate tx-002 skipped idempotently
        r_dup = execute_incremental_aggregate_transform(
            current_input=FoundryInputSpec(alias="cur", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="current"),
            added_input=FoundryInputSpec(alias="add", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="added"),
            output=FoundryOutputSpec(handle=out_handle, resource_kind="foundry_managed_iceberg", write_mode="replace"),
            spec=spec,
            checkpoint_store=ckpt,
            resources=self.resources,
            current_transaction_rid="tx-002",
            previous_transaction_rid="tx-001",
        )
        self.assertEqual(r_dup.incremental["action"], "duplicate_skipped_idempotent")
        self.assertEqual(self.pq.read_table(out_dir / "part-00000.parquet").to_pylist(), full_recompute_truth(tx2_rows))

        # Gap fails explicitly without mutating state
        with self.assertRaises(FoundryBoundaryError) as gap_err:
            execute_incremental_aggregate_transform(
                current_input=FoundryInputSpec(alias="cur", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="current"),
                added_input=FoundryInputSpec(alias="add", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="added"),
                output=FoundryOutputSpec(handle=out_handle, resource_kind="foundry_managed_iceberg", write_mode="replace"),
                spec=spec,
                checkpoint_store=ckpt,
                resources=self.resources,
                current_transaction_rid="tx-009",
                previous_transaction_rid="tx-008",
            )
        self.assertEqual(gap_err.exception.code, "SL_INCREMENTAL_SEQUENCE_GAP")
        self.assertEqual(ckpt.load()["last_transaction_rid"], "tx-002")

        # tx-003 deletions/retractions (removes 'apac' completely and deletes eu's non-null 7 leaving NULL sum)
        add3_rows = [
            {"region": "apac", "amount": 100, "_op": "delete"},
            {"region": "eu", "amount": 7, "_op": "delete"},
            {"region": "us", "amount": 5, "_op": "delete"},
        ]
        tx3_rows = [
            {"region": "us", "amount": 10, "_op": "insert"},
            {"region": "us", "amount": 20, "_op": "insert"},
            {"region": "eu", "amount": None, "_op": "insert"},
        ]
        write_dir(add_dir, add3_rows)
        r3 = execute_incremental_aggregate_transform(
            current_input=FoundryInputSpec(alias="cur", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="current"),
            added_input=FoundryInputSpec(alias="add", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="added"),
            output=FoundryOutputSpec(handle=out_handle, resource_kind="foundry_managed_iceberg", write_mode="replace"),
            spec=spec,
            checkpoint_store=ckpt,
            resources=self.resources,
            current_transaction_rid="tx-003",
            previous_transaction_rid="tx-002",
        )
        self.assertEqual(r3.incremental["maintained_group_count"], 2)
        self.assertEqual(self.pq.read_table(out_dir / "part-00000.parquet").to_pylist(), full_recompute_truth(tx3_rows))

        # Deliberate publication failure on tx-004 keeps checkpoint at tx-003 & prior output intact
        add4_rows = [{"region": "latam", "amount": 55, "_op": "insert"}]
        write_dir(add_dir, add4_rows)
        prior_ckpt_bytes = ckpt_path.read_bytes()
        prior_out_bytes = (out_dir / "part-00000.parquet").read_bytes()

        class _FailingOutputHandle(_MockPathContextOutput):
            def commit(self) -> None:
                if self._pending_dir is not None:
                    shutil.rmtree(self._pending_dir, ignore_errors=True)
                    self._pending_dir = None
                raise IOError("injected transaction commit abort")

        with self.assertRaises(FoundryBoundaryError):
            execute_incremental_aggregate_transform(
                current_input=FoundryInputSpec(alias="cur", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="current"),
                added_input=FoundryInputSpec(alias="add", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="added"),
                output=FoundryOutputSpec(handle=_FailingOutputHandle(out_dir), resource_kind="foundry_managed_iceberg"),
                spec=spec,
                checkpoint_store=ckpt,
                resources=self.resources,
                current_transaction_rid="tx-004",
                previous_transaction_rid="tx-003",
            )
        self.assertEqual(ckpt_path.read_bytes(), prior_ckpt_bytes)
        self.assertEqual((out_dir / "part-00000.parquet").read_bytes(), prior_out_bytes)

        # Clean retry of tx-004 succeeds and matches full recomputation
        tx4_rows = tx3_rows + add4_rows
        execute_incremental_aggregate_transform(
            current_input=FoundryInputSpec(alias="cur", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="current"),
            added_input=FoundryInputSpec(alias="add", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="added"),
            output=FoundryOutputSpec(handle=out_handle, resource_kind="foundry_managed_iceberg"),
            spec=spec,
            checkpoint_store=ckpt,
            resources=self.resources,
            current_transaction_rid="tx-004",
            previous_transaction_rid="tx-003",
        )
        self.assertEqual(self.pq.read_table(out_dir / "part-00000.parquet").to_pylist(), full_recompute_truth(tx4_rows))

        # Upstream snapshot replacement (tx-005) recomputes from current
        tx5_rows = [{"region": "ca", "amount": 88, "_op": "insert"}]
        write_dir(cur_dir, tx5_rows)
        r5 = execute_incremental_aggregate_transform(
            current_input=FoundryInputSpec(alias="cur", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="current"),
            added_input=FoundryInputSpec(alias="add", handle=ice_in, resource_kind="foundry_managed_iceberg", read_mode="added"),
            output=FoundryOutputSpec(handle=out_handle, resource_kind="foundry_managed_iceberg"),
            spec=spec,
            checkpoint_store=ckpt,
            resources=self.resources,
            current_transaction_rid="tx-005",
            is_snapshot_replacement=True,
        )
        self.assertEqual(r5.incremental["action"], "snapshot_replacement_recomputed")
        self.assertEqual(self.pq.read_table(out_dir / "part-00000.parquet").to_pylist(), full_recompute_truth(tx5_rows))

        # Floating sum column rejected for direct incremental maintenance
        float_table = self.pa.table({"region": ["us"], "fval": self.pa.array([1.25], type=self.pa.float64())})
        with self.assertRaises(FoundryBoundaryError) as float_err:
            execute_incremental_aggregate_transform(
                current_input=FoundryInputSpec(alias="cur", handle=_MockArrowInput(float_table)),
                output=FoundryOutputSpec(handle=out_handle),
                spec=IncrementalAggregateSpec(source_rid="ri.float.1", group_by=("region",), sum_columns=("fval",)),
                checkpoint_store=IncrementalCheckpointStore(self.root / "state" / "float_ckpt.json"),
                resources=self.resources,
                current_transaction_rid="tx-f1",
            )
        self.assertEqual(float_err.exception.code, "SL_INCREMENTAL_UNSUPPORTED_TYPE")

    def test_governed_vortex_output_roundtrip_and_non_tabular_boundary(self) -> None:
        import shardloom as sl

        table = self.pa.table({"k": ["x", "y"], "v": [11, 22]})
        vtx_dir = self.root / "vortex_ds"
        out_handle = _MockVortexFsOutput(vtx_dir)
        report = execute_foundry_transform(
            inputs=[FoundryInputSpec(alias="src", handle=_MockArrowInput(table))],
            output=FoundryOutputSpec(handle=out_handle, output_format="vortex", target_filename="data.vortex"),
            resources=self.resources,
            sql="SELECT k, v * 2 AS v2 FROM src ORDER BY k",
        )
        self.assertFalse(report.output["general_foundry_tabular_support"])
        self.assertEqual(report.output["output_format"], "vortex")
        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1) as reader:
            actual = reader.read_vortex(vtx_dir / "data.vortex").collect(check=True).to_arrow().to_pylist()
        self.assertEqual(actual, [{"k": "x", "v2": 22}, {"k": "y", "v2": 44}])


if __name__ == "__main__":
    unittest.main()
