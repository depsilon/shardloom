# SPDX-License-Identifier: Apache-2.0
"""One ShardLoom benchmark adapter over the public native workflow protocol."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import sys
import time
import uuid

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from fixtures import fixture_schema_for_role
from native_workflow_protocol import extract_result, public_workflow_command, read_json_output, report_fields, strict_json
from run_clickbench_query_uat import file_sha256, run_profiled_command
from workloads import WORKLOADS


class NativeDeclarationUnsupported(RuntimeError):
    """The native engine rejected the recorded complete workload declaration."""


@dataclass(frozen=True)
class NativeConfiguration:
    binary: Path
    workspace: Path
    input_state: str = "raw"
    output_format: str = "collect"
    memory_gb: int = 1
    max_parallelism: int = 2
    timeout: float = 120.0

    def __post_init__(self):
        if self.input_state not in ("raw", "prepared"):
            raise ValueError("input_state must be raw or prepared")
        if self.output_format not in ("collect", "vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"):
            raise ValueError("unknown output format")
        if (type(self.memory_gb) is not int or self.memory_gb <= 0
                or type(self.max_parallelism) is not int or self.max_parallelism <= 0
                or type(self.timeout) not in (int, float)
                or not math.isfinite(self.timeout) or self.timeout <= 0):
            raise ValueError("native resources and deadline must be positive")


def _generation(path: Path) -> tuple[int, ...]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


class NativePublicRunner:
    def __init__(self, configuration: NativeConfiguration, *, fact_path, dim_path,
                 fact_part_paths, round_float, guard=None):
        self.configuration = configuration
        self.fact_path, self.dim_path = fact_path, dim_path
        self.fact_part_paths, self.round_float = fact_part_paths, round_float
        self.binary = configuration.binary.resolve(strict=True)
        self.binary_generation = _generation(self.binary)
        self.binary_sha256 = file_sha256(self.binary)
        self.run_id = uuid.uuid4().hex
        self.call_count = 0
        self.prepared = {}
        self.preparation_receipts = []
        self.fixture_preparation_receipts = []
        self.reused_native_inputs = []
        self.guard = guard

    def _workspace(self, paths):
        root = require_local_path(self.configuration.workspace, Path.home(), sys.platform)
        workspace = root / "native-public" / self.run_id
        self._guard(root, workspace)
        workspace.mkdir(parents=True, exist_ok=True)
        return root, workspace

    def _guard(self, root, workspace):
        if self.guard is not None:
            self.guard()
        check_budgets(root, workspace / "reserved-output", root / "logs",
                      min_free_bytes=12 * GIB, reserve_bytes=64 * MIB,
                      max_workspace_bytes=100 * GIB, max_log_bytes=192 * MIB)

    def _run(self, paths, command, *, phase):
        root, workspace = self._workspace(paths)
        if _generation(self.binary) != self.binary_generation:
            raise RuntimeError("native benchmark executable changed")
        self.call_count += 1
        logs = root / "logs" / "native-public" / self.run_id
        logs.mkdir(parents=True, exist_ok=True)
        prefix = logs / f"call-{self.call_count:06d}"
        stdout, stderr = prefix.with_suffix(".stdout.json"), prefix.with_suffix(".stderr.txt")
        observation = run_profiled_command(command, prefix, self.configuration.timeout,
                                          lambda: self._guard(root, workspace))
        receipt = {**observation, "command": command, "phase": phase,
                   "binary_sha256": self.binary_sha256, "stdout": str(stdout),
                   "stdout_sha256": file_sha256(stdout), "stderr": str(stderr)}
        prefix.with_suffix(".receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        if any(failure != "profiled native command failed" for failure in observation["guard_failures"]):
            raise RuntimeError(f"native command guard failed: {receipt}")
        envelope = strict_json(stdout.read_text())
        if envelope.get("status") != "success" or observation["returncode"] != 0:
            detail = f"{envelope.get('summary')}: {envelope.get('diagnostics')}; evidence={stdout}"
            if envelope.get("status") == "unsupported":
                raise NativeDeclarationUnsupported(detail)
            raise RuntimeError(detail)
        if (envelope.get("fallback", {}).get("attempted") is not False
                or envelope.get("fallback", {}).get("engine") is not None):
            raise RuntimeError(f"unsafe native execution report: {stdout}")
        if phase != "input_preparation":
            fields = report_fields(envelope)
            if (fields.get("public_workflow_fallback_attempted") != "false"
                    or fields.get("public_workflow_external_engine_invoked") != "false"):
                raise RuntimeError(f"public native request lacks no-fallback evidence: {stdout}")
        if _generation(self.binary) != self.binary_generation:
            raise RuntimeError("native benchmark executable changed during execution")
        return envelope, receipt

    def _source_paths(self, paths, data_format, roles):
        sources = {}
        for role in roles:
            if role == "fact":
                sources[role] = [(self.fact_path(paths, data_format), data_format)]
            elif role == "dim":
                sources[role] = [(self.dim_path(paths, data_format), data_format)]
            elif role == "parts":
                parts = self.fact_part_paths(paths, data_format)
                if not parts:
                    raise ValueError("multipart workload requires declared input parts")
                sources[role] = [(part, data_format) for part in parts]
            elif role == "delta":
                if paths.cdc_delta_csv is None:
                    raise ValueError("CDC workload requires the declared delta fixture")
                sources[role] = [(paths.cdc_delta_csv, "csv")]
            else:
                raise ValueError(f"unknown workload source role: {role}")
        for members in sources.values():
            for path, _ in members:
                require_local_path(path, Path.home(), sys.platform)
                if not path.is_file():
                    raise ValueError(f"declared input is not a file: {path}")
        return sources

    def _prepare_source(self, paths, source, source_format, target, role):
        generation = _generation(source)
        if target.exists():
            raise ValueError(f"native preparation refuses an existing destination: {target}")
        command = [str(self.binary), "vortex-prepare", str(source), str(target),
                   "--input-format", source_format, "--memory-gb", str(self.configuration.memory_gb),
                   "--max-parallelism", str(self.configuration.max_parallelism), "--format", "json"]
        if source_format in ("csv", "json", "jsonl"):
            command.extend(["--schema", fixture_schema_for_role(paths, role)])
        envelope, receipt = self._run(paths, command, phase="input_preparation")
        fields = {entry["key"]: entry["value"] for entry in envelope["fields"]}
        if (fields.get("vortex_ingest_performed") != "true" or not target.is_file()
                or fields.get("external_engine_invoked") != "false"):
            raise RuntimeError(f"native input preparation lacks evidence: {receipt}")
        if _generation(source) != generation:
            raise RuntimeError("input changed during native preparation")
        return receipt, generation, _generation(target)

    def prepare_fixture_inputs(self, paths, formats):
        """Create native-format fixtures before freezing inputs or timing queries.

        Comparison engines use the independently generated original CSV values;
        none of these preparation reports supply expected query results.
        """
        if "vortex" not in formats:
            return
        sources = [(paths.fact_csv, paths.fact_vortex, "fact"),
                   (paths.dim_csv, paths.dim_vortex, "dim")]
        parts = self.fact_part_paths(paths, "csv")
        if parts:
            if paths.fact_vortex_parts_dir is None:
                raise ValueError("native fixture parts require a declared destination")
            paths.fact_vortex_parts_dir.mkdir(exist_ok=False)
            sources.extend((source, paths.fact_vortex_parts_dir / (source.stem + ".vortex"), "parts")
                           for source in parts)
        for source, target, role in sources:
            if target is None:
                raise ValueError("native fixture requires a declared destination")
            receipt, _, _ = self._prepare_source(paths, source, "csv", target, role)
            self.fixture_preparation_receipts.append(receipt)

    def prepare(self, paths, formats, scenarios):
        """Optional durable input preparation, recorded outside query timing."""
        if self.configuration.input_state != "prepared":
            return
        roles = {role for name in scenarios for role in WORKLOADS[name].source_roles}
        _, workspace = self._workspace(paths)
        for data_format in formats:
            for role, members in self._source_paths(paths, data_format, sorted(roles)).items():
                for path, source_format in members:
                    key = (str(path.resolve()), source_format)
                    if key in self.prepared:
                        continue
                    generation = _generation(path)
                    if source_format == "vortex":
                        self.prepared[key] = (path, generation, generation)
                        self.reused_native_inputs.append({"path": str(path), "input_format": "vortex",
                                                          "sha256": file_sha256(path), "bytes": path.stat().st_size})
                        continue
                    token = hashlib.sha256(json.dumps(key).encode()).hexdigest()[:20]
                    target = workspace / f"input-{token}.vortex"
                    receipt, generation, prepared_generation = self._prepare_source(paths, path, source_format, target, role)
                    self.prepared[key] = (target, generation, prepared_generation)
                    self.preparation_receipts.append(receipt)

    def _bindings(self, sources, paths):
        expressions, bindings, generations = {}, {}, {}
        for role, members in sources.items():
            references = []
            for source, source_format in members:
                generations[source] = _generation(source)
                path, data_format = source, source_format
                if self.configuration.input_state == "prepared":
                    key = (str(source.resolve()), source_format)
                    if key not in self.prepared:
                        raise RuntimeError("prepare must complete before timed execution")
                    path, original, prepared_generation = self.prepared[key]
                    if generations[source] != original or _generation(path) != prepared_generation:
                        raise RuntimeError("declared input or prepared artifact changed")
                    data_format = "vortex"
                    generations[path] = prepared_generation
                binding = {"input_format": data_format}
                if data_format in ("csv", "json", "jsonl"):
                    binding["source_schema"] = fixture_schema_for_role(paths, role)
                bindings[str(path)] = binding
                references.append(_literal(path))
            expressions[role] = ("(" + " UNION ALL ".join(f"SELECT * FROM {ref}" for ref in references)
                                 + ") AS parts") if role == "parts" else references[0]
        return expressions, bindings, generations

    def _statement(self, paths, sql, bindings, *, output_format="collect"):
        _, workspace = self._workspace(paths)
        output = None if output_format == "collect" else workspace / f"output-{self.call_count + 1:06d}.{output_format}"
        command = public_workflow_command(
            self.binary, sql, source_bindings=bindings, memory_gb=self.configuration.memory_gb,
            max_parallelism=self.configuration.max_parallelism,
            requested_output="collect" if output is None else f"write_{output_format}", output=output,
        )
        envelope, receipt = self._run(paths, command, phase="query_and_requested_output")
        if output is None:
            return extract_result(envelope), envelope, receipt, None
        fields = report_fields(envelope)
        if (fields.get("native_vortex_result_export_all_targets_committed") != "true"
                or fields.get("native_vortex_result_export_path") != str(output)
                or not output.is_file()):
            raise RuntimeError(f"native sink lacks committed output evidence: {receipt}")
        receipt["output"] = {"path": str(output), "sha256": file_sha256(output),
                             "bytes": output.stat().st_size}
        if output_format in ("json", "jsonl"):
            started = time.perf_counter()
            rows = read_json_output(output, output_format)
            validation = {
                "kind": "complete_json_file_readback", "format": output_format,
                **receipt["output"], "seconds": time.perf_counter() - started,
            }
            return rows, envelope, receipt, validation
        reopen = public_workflow_command(
            self.binary, f"SELECT * FROM {_literal(output)}",
            source_bindings={str(output): {"input_format": output_format}},
            memory_gb=self.configuration.memory_gb, max_parallelism=self.configuration.max_parallelism,
        )
        readback, readback_receipt = self._run(paths, reopen, phase="output_readback_validation")
        return extract_result(readback), envelope, receipt, readback_receipt

    def run(self, scenario, paths, data_format):
        workload = WORKLOADS[scenario]
        sources = self._source_paths(paths, data_format, workload.source_roles)
        expressions, bindings, generations = self._bindings(sources, paths)
        statements, writer = workload.bind(expressions)
        receipts, validations, rows = [], [], []
        started = time.perf_counter()
        if writer:
            _, _, receipt, validation = self._statement(paths, writer, bindings, output_format="csv")
            receipts.append(receipt)
            validations.append(validation)
        for sql in statements:
            result, envelope, receipt, validation = self._statement(
                paths, sql, bindings, output_format=self.configuration.output_format)
            if not isinstance(result, list):
                raise RuntimeError("declared benchmark query did not return complete rows")
            rows.append(result)
            receipts.append(receipt)
            if validation is not None:
                validations.append(validation)
        if any(_generation(path) != generation for path, generation in generations.items()):
            raise RuntimeError("benchmark source changed during execution")
        result = workload.result(rows, self.round_float)
        fields = report_fields(envelope)
        fields.update({
            "benchmark_request_protocol": "public_native_workflow",
            "benchmark_input_state": self.configuration.input_state,
            "benchmark_output_format": self.configuration.output_format,
            "benchmark_transport": "new_cli_process_per_request",
            "benchmark_binary_sha256": self.binary_sha256,
            "benchmark_sql_declarations": json.dumps(statements),
            "benchmark_native_calls": json.dumps(receipts),
            "benchmark_output_validation_calls": json.dumps(validations),
            "benchmark_input_preparation_calls": json.dumps(self.preparation_receipts),
            "benchmark_native_input_reuse": json.dumps(self.reused_native_inputs),
            "cli_process_wall_millis": str(1000 * sum(row["seconds"] for row in receipts)),
            "benchmark_readback_wall_millis": str(1000 * sum(row["seconds"] for row in validations)),
            "benchmark_case_wall_millis": str(1000 * (time.perf_counter() - started)),
            "benchmark_timing_boundary": "query and requested sink process exit; input preparation and output readback excluded",
            "benchmark_query_answer_cached": "false",
            "build_time_excluded": "true",
        })
        return {"__benchmark_result": result, "__shardloom_evidence": fields}
