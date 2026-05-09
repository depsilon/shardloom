"""Subprocess client for ShardLoom's CLI JSON protocol."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Mapping, Sequence

from .errors import ShardLoomCommandError, ShardLoomProtocolError
from .models import OutputEnvelope

CommandPart = str | os.PathLike[str]
Binary = CommandPart | Sequence[CommandPart]
DEFAULT_PROFILE_ORDER = ("release", "debug")
ETL_INPUT_FORMATS = frozenset({"csv", "vortex"})


class ShardLoomClient:
    """Thin client that invokes the ShardLoom CLI with `--format json`.

    The client does not inspect datasets, probe catalogs, load external engines,
    or provide fallback execution. It only runs explicit CLI commands requested
    by the caller and parses the resulting JSON envelope.
    """

    def __init__(
        self,
        binary: Binary | None = None,
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | os.PathLike[str] | None = None,
        repo_root: str | os.PathLike[str] | None = None,
        profile_order: Sequence[str] = DEFAULT_PROFILE_ORDER,
        timeout: float | None = None,
    ) -> None:
        self._binary = binary
        self._env = dict(env) if env is not None else None
        self._cwd = Path(cwd) if cwd is not None else None
        self._repo_root = Path(repo_root) if repo_root is not None else None
        self._profile_order = tuple(profile_order)
        self._timeout = timeout

    @classmethod
    def from_repo(
        cls,
        repo_root: str | os.PathLike[str] | None = None,
        *,
        profile_order: Sequence[str] = DEFAULT_PROFILE_ORDER,
        **kwargs: object,
    ) -> "ShardLoomClient":
        """Create a client that resolves `target/<profile>/shardloom` lazily.

        This is intended for source-tree development and local ETL testing. It
        does not run commands or probe anything at import time.
        """

        root = Path.cwd() if repo_root is None else Path(repo_root)
        return cls(repo_root=root, profile_order=profile_order, **kwargs)

    def status(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CLI status envelope."""

        return self.run(["status"], check=check)

    def api_compat_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CLI/API JSON compatibility plan envelope."""

        return self.run(["api-compat-plan"], check=check)

    def python_wrapper_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the Python wrapper foundation plan envelope."""

        return self.run(["python-wrapper-plan"], check=check)

    def capabilities(self, scope: str | None = None, *, check: bool = True) -> OutputEnvelope:
        """Return a capability-discovery envelope for the optional scope."""

        args = ["capabilities"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def vortex_run(
        self,
        dataset_uri: str | os.PathLike[str],
        primitive: str,
        *,
        memory_gb: int = 4,
        max_parallelism: int = 1,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit `vortex-run` CLI command and parse its envelope."""

        return self.run(
            [
                "vortex-run",
                str(dataset_uri),
                primitive,
                str(memory_gb),
                str(max_parallelism),
            ],
            check=check,
        )

    def traditional_analytics_run(
        self,
        scenario: str,
        fact_csv: str | os.PathLike[str],
        dim_csv: str | os.PathLike[str],
        *,
        workspace: str | os.PathLike[str] | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit traditional analytics universal-I/O smoke command."""

        args = [
            "traditional-analytics-run",
            scenario,
            str(fact_csv),
            str(dim_csv),
        ]
        if workspace is not None:
            args.extend(["--workspace", str(workspace)])
        return self.run(args, check=check)

    def traditional_analytics_vortex_run(
        self,
        scenario: str,
        fact_vortex: str | os.PathLike[str],
        dim_vortex: str | os.PathLike[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit native Vortex traditional analytics smoke command."""

        return self.run(
            [
                "traditional-analytics-vortex-run",
                scenario,
                str(fact_vortex),
                str(dim_vortex),
            ],
            check=check,
        )

    def live_etl_smoke(
        self,
        scenario: str,
        fact_input: str | os.PathLike[str],
        dim_input: str | os.PathLike[str],
        *,
        input_format: str = "csv",
        workspace: str | os.PathLike[str] | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the current live ETL smoke surface for CSV or native Vortex inputs.

        CSV mode imports deterministic local CSV files into temporary Vortex
        files, reopens them, and runs the temporary benchmark operator. Vortex
        mode starts from existing `.vortex` inputs. Both modes are explicit CLI
        invocations and preserve the returned materialization/certificate fields.
        """

        normalized_format = input_format.lower().replace("_", "-")
        if normalized_format not in ETL_INPUT_FORMATS:
            raise ValueError(
                f"input_format must be one of {sorted(ETL_INPUT_FORMATS)}; "
                f"got {input_format!r}"
            )
        if normalized_format == "csv":
            return self.traditional_analytics_run(
                scenario,
                fact_input,
                dim_input,
                workspace=workspace,
                check=check,
            )
        if workspace is not None:
            raise ValueError("workspace is only supported for CSV live ETL smoke runs")
        return self.traditional_analytics_vortex_run(
            scenario,
            fact_input,
            dim_input,
            check=check,
        )

    def dynamic_work_shaping_plan(
        self, profile: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return the advisory dynamic work-shaping plan for an optional profile."""

        args = ["dynamic-work-shaping-plan"]
        if profile is not None:
            args.append(profile)
        return self.run(args, check=check)

    def sizing_feedback_plan(
        self,
        memory_gb: int,
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return the advisory dynamic sizing feedback plan."""

        if isinstance(signals, str):
            signals_text = signals
        else:
            signals_text = ",".join(signals)
        return self.run(
            ["sizing-feedback-plan", str(memory_gb), signals_text],
            check=check,
        )

    def benchmark_plan(
        self, scope: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return the benchmark plan for the optional scope."""

        args = ["benchmark-plan"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def benchmark_claim_evidence_plan(
        self, scope: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return benchmark claim-evidence planning for the optional scope."""

        args = ["benchmark-claim-evidence-plan"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def world_class_sufficiency_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the current CG-20 world-class sufficiency evidence envelope."""

        return self.run(["world-class-sufficiency-plan"], check=check)

    def run(self, args: Sequence[CommandPart], *, check: bool = True) -> OutputEnvelope:
        """Invoke a ShardLoom CLI command with JSON output enabled."""

        command = self._command(args)
        completed = subprocess.run(
            command,
            cwd=self._cwd,
            env=self._env,
            text=True,
            capture_output=True,
            timeout=self._timeout,
            check=False,
        )
        envelope = self._parse_stdout(completed.stdout, command)
        if check and (completed.returncode != 0 or envelope.is_error):
            raise ShardLoomCommandError(
                command=command,
                returncode=completed.returncode,
                envelope=envelope,
                stderr=completed.stderr,
            )
        return envelope

    def _command(self, args: Sequence[CommandPart]) -> list[str]:
        command = self._binary_parts()
        command.extend(str(arg) for arg in args)
        self._append_json_format(command)
        return command

    def _binary_parts(self) -> list[str]:
        binary = self._resolved_binary()
        if isinstance(binary, (str, os.PathLike)):
            return [str(binary)]
        if not binary:
            raise ValueError("ShardLoom binary command cannot be empty")
        return [str(part) for part in binary]

    def _resolved_binary(self) -> Binary:
        if self._binary is not None:
            return self._binary

        env_binary = self._effective_env().get("SHARDLOOM_BIN")
        if env_binary:
            return env_binary

        if self._repo_root is not None:
            candidate = self._repo_binary_candidate()
            if candidate is not None:
                return candidate

        path_binary = shutil.which("shardloom", path=self._effective_env().get("PATH"))
        if path_binary is not None:
            return path_binary

        return "shardloom"

    def _effective_env(self) -> Mapping[str, str]:
        return self._env if self._env is not None else os.environ

    def _repo_binary_candidate(self) -> Path | None:
        suffixes = (".exe", "") if os.name == "nt" else ("",)
        for profile in self._profile_order:
            for suffix in suffixes:
                candidate = self._repo_root / "target" / profile / f"shardloom{suffix}"
                if candidate.is_file():
                    return candidate
        return None

    @staticmethod
    def _append_json_format(command: list[str]) -> None:
        if "--format" not in command:
            command.extend(["--format", "json"])
            return
        index = command.index("--format")
        try:
            value = command[index + 1]
        except IndexError as exc:
            raise ValueError("--format requires a value") from exc
        if value != "json":
            raise ValueError("ShardLoom Python client requires --format json")

    @staticmethod
    def _parse_stdout(stdout: str, command: Sequence[str]) -> OutputEnvelope:
        first_line = stdout.splitlines()[0] if stdout else ""
        if not first_line:
            raise ShardLoomProtocolError(
                f"ShardLoom command emitted no JSON output: {' '.join(command)}"
            )
        try:
            payload = json.loads(first_line)
        except json.JSONDecodeError as exc:
            raise ShardLoomProtocolError(
                f"ShardLoom command emitted invalid JSON: {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise ShardLoomProtocolError("ShardLoom JSON output envelope must be an object")
        try:
            return OutputEnvelope.from_json(payload)
        except ValueError as exc:
            raise ShardLoomProtocolError(str(exc)) from exc
