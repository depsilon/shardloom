"""Lazy workflow planning helpers for the ShardLoom Python surface."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Mapping, Sequence, cast
from urllib.parse import quote

from .client import (
    Binary,
    DEFAULT_PROFILE_ORDER,
    EngineSelectionPlan,
    GeneratedSourceWriteReport,
    ShardLoomClient,
)
from .models import Diagnostic, OutputEnvelope

SUPPORTED_SOURCE_FORMATS = ("vortex", "csv", "json", "parquet")


@dataclass(frozen=True, slots=True)
class WorkflowSource:
    """A declared workflow source that is not read during construction."""

    source_format: str
    uri: str
    schema: tuple[tuple[str, str], ...] = ()

    @property
    def schema_map(self) -> dict[str, str]:
        """Return the optional declared schema as a dict."""

        return dict(self.schema)

    def to_summary(self) -> str:
        """Return a deterministic source summary for CLI explain/estimate calls."""

        return f"read_{self.source_format}({self.uri})"


@dataclass(frozen=True, slots=True)
class WorkflowOperation:
    """A lazy query-builder operation."""

    kind: str
    values: tuple[str, ...]

    def to_summary(self) -> str:
        """Return a deterministic operation summary."""

        if self.kind == "filter":
            return f"filter({self.values[0]})"
        if self.kind == "select":
            return f"select({','.join(self.values)})"
        if self.kind == "limit":
            return f"limit({self.values[0]})"
        return f"{self.kind}({','.join(self.values)})"


@dataclass(frozen=True, slots=True)
class WorkflowCertificationReport:
    """Report-only certificate surfaces for a lazy workflow."""

    workflow: "LazyFrame"
    execution_certificate_plan: OutputEnvelope
    native_io_envelope_plan: OutputEnvelope
    certification_capabilities: OutputEnvelope

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return all certificate-related envelopes."""

        return (
            self.execution_certificate_plan,
            self.native_io_envelope_plan,
            self.certification_capabilities,
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether any certificate surface attempted fallback execution."""

        return any(envelope.fallback.attempted for envelope in self.envelopes)

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        """Return certificate and capability diagnostics."""

        return tuple(
            diagnostic
            for envelope in self.envelopes
            for diagnostic in envelope.diagnostics
        )


@dataclass(frozen=True, slots=True)
class UnsupportedWorkflowReport:
    """Aggregated diagnostics for report-only lazy workflow inspection."""

    workflow: "LazyFrame"
    input_plan: OutputEnvelope
    explain: OutputEnvelope
    estimate: OutputEnvelope
    certification: WorkflowCertificationReport

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return all envelopes collected for the report."""

        return (
            self.input_plan,
            self.explain,
            self.estimate,
            *self.certification.envelopes,
        )

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        """Return all diagnostics across the collected envelopes."""

        return tuple(
            diagnostic
            for envelope in self.envelopes
            for diagnostic in envelope.diagnostics
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether any inspected surface attempted fallback execution."""

        return any(envelope.fallback.attempted for envelope in self.envelopes)

    @property
    def unsupported_reasons(self) -> tuple[str, ...]:
        """Return stable unsupported diagnostic reasons/messages."""

        reasons: list[str] = []
        for diagnostic in self.diagnostics:
            if diagnostic.reason:
                reasons.append(diagnostic.reason)
            elif diagnostic.message:
                reasons.append(diagnostic.message)
        return tuple(dict.fromkeys(reasons))

    @property
    def materialization_boundaries(self) -> tuple[str, ...]:
        """Return materialization-related fields from collected envelopes."""

        boundaries: list[str] = []
        for envelope in self.envelopes:
            for key, value in envelope.field_map.items():
                if "materialization" in key and value not in {"", "false", "none"}:
                    boundaries.append(f"{envelope.command}:{key}={value}")
        return tuple(dict.fromkeys(boundaries))


@dataclass(frozen=True, slots=True)
class GeneratedRowsSource:
    """Scoped source-free user rows that can write a local JSONL smoke output."""

    schema_arg: str
    rows_arg: str
    client: ShardLoomClient

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write generated user rows to a scoped local output sink with evidence."""

        return self.client.generated_source_user_rows_smoke(
            target_uri,
            self.schema_arg,
            self.rows_arg,
            output_format=output_format,
            allow_overwrite=allow_overwrite,
            check=check,
        )


@dataclass(frozen=True, slots=True)
class UnsupportedWorkflowOperationReport:
    """Report-only unsupported diagnostic for a single workflow affordance."""

    workflow: "LazyFrame"
    operation: str
    envelope: OutputEnvelope

    @property
    def blocker_id(self) -> str | None:
        """Return the stable blocker ID for this unsupported workflow method."""

        return self.envelope.field("blocker_id")

    @property
    def required_evidence(self) -> tuple[str, ...]:
        """Return evidence required before the operation can be certified."""

        value = self.envelope.field("required_evidence", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def suggested_next_action(self) -> str | None:
        """Return the deterministic next action surfaced by the CLI."""

        return self.envelope.field("suggested_next_action")

    @property
    def fallback_attempted(self) -> bool:
        """Whether the unsupported-report path attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def runtime_execution(self) -> bool:
        """Whether runtime execution occurred while building this report."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def data_read(self) -> bool:
        """Whether data was read while building this report."""

        return self.envelope.field_bool("data_read", False) is True

    @property
    def write_io(self) -> bool:
        """Whether write I/O occurred while building this report."""

        return self.envelope.field_bool("write_io", False) is True


@dataclass(frozen=True, slots=True)
class LazyFrame:
    """A lazy ShardLoom workflow plan.

    The object records the requested source and transformations only. It does
    not read data, infer schema, probe object stores, materialize output, or
    invoke external engines. Explicit inspection methods lower the declaration
    to existing ShardLoom CLI JSON report surfaces.
    """

    source: WorkflowSource
    client: ShardLoomClient
    operations: tuple[WorkflowOperation, ...] = ()
    engine_mode: str = "auto"

    @property
    def source_format(self) -> str:
        """Return the declared input source format."""

        return self.source.source_format

    @property
    def uri(self) -> str:
        """Return the declared input URI/path."""

        return self.source.uri

    @property
    def operation_summary(self) -> str:
        """Return a deterministic logical-plan summary for report surfaces."""

        parts = [self.source.to_summary()]
        parts.extend(operation.to_summary() for operation in self.operations)
        return " -> ".join(parts)

    def with_engine(self, engine_mode: str) -> "LazyFrame":
        """Return this lazy workflow with a different requested engine mode."""

        return LazyFrame(
            source=self.source,
            client=self.client,
            operations=self.operations,
            engine_mode=_normalize_engine_mode(engine_mode),
        )

    def filter(self, predicate: object) -> "LazyFrame":
        """Return a lazy plan with an added filter predicate."""

        value = str(predicate).strip()
        if not value:
            raise ValueError("filter predicate must not be empty")
        return self._append(WorkflowOperation("filter", (value,)))

    def select(self, *columns: object) -> "LazyFrame":
        """Return a lazy plan with an added projection."""

        return self._append(WorkflowOperation("select", _normalize_columns(columns)))

    def with_column(
        self,
        name: str,
        expression: object,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for expression-backed column creation."""

        column_name = _require_non_empty("column name", name)
        expression_text = _require_non_empty("column expression", expression)
        return self._unsupported_operation(
            "with-column",
            f"{column_name}={expression_text}",
            check=check,
        )

    def limit(self, count: int) -> "LazyFrame":
        """Return a lazy plan with an added limit."""

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("limit count must be an integer")
        if count < 0:
            raise ValueError("limit count must be non-negative")
        return self._append(WorkflowOperation("limit", (str(count),)))

    def plan(self, *, check: bool = False) -> OutputEnvelope:
        """Return a side-effect-free input/read planning envelope."""

        if self.source.source_format == "vortex":
            return self.client.vortex_read_plan(self.source.uri, check=check)
        return self.client.input_plan(
            self.source.uri,
            source_format=self.source.source_format,
            check=check,
        )

    def explain(self, *, check: bool = False) -> OutputEnvelope:
        """Return the CLI explain envelope for this logical workflow summary."""

        return self.client.explain(self.operation_summary, check=check)

    def estimate(self, *, check: bool = False) -> OutputEnvelope:
        """Return the CLI estimate envelope for this logical workflow summary."""

        return self.client.estimate(self.operation_summary, check=check)

    def profile(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for runtime profile collection."""

        return self._unsupported_operation("profile", check=check)

    def collect(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for materializing workflow rows."""

        return self._unsupported_operation("collect", check=check)

    def to_pandas(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for pandas materialization."""

        return self._unsupported_operation("to-pandas", check=check)

    def to_arrow(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Arrow materialization."""

        return self._unsupported_operation("to-arrow", check=check)

    def to_arrow_table(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Arrow table materialization."""

        return self._unsupported_operation("to-arrow-table", check=check)

    def to_arrow_ipc(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Arrow IPC materialization."""

        return self._unsupported_operation("to-arrow-ipc", check=check)

    def to_numpy(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for NumPy materialization."""

        return self._unsupported_operation("to-numpy", check=check)

    def to_python_objects(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Python-object materialization."""

        return self._unsupported_operation("to-python-objects", check=check)

    def write_vortex(
        self,
        target_uri: str | os.PathLike[str],
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for native Vortex workflow writes."""

        return self._unsupported_operation("write-vortex", str(target_uri), check=check)

    def write_parquet(
        self,
        target_uri: str | os.PathLike[str],
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Parquet compatibility exports."""

        return self._unsupported_operation("write-parquet", str(target_uri), check=check)

    def sql(
        self,
        statement: str,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for SQL workflow execution."""

        target = _require_non_empty("sql statement", statement)
        return self._unsupported_operation("sql", target, check=check)

    def join(
        self,
        other: "LazyFrame | str",
        *,
        on: str | Sequence[str],
        how: str = "inner",
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for DataFrame joins."""

        columns = ",".join(_normalize_columns((on,)))
        right = other.operation_summary if isinstance(other, LazyFrame) else str(other)
        target = f"{how.strip().lower()}:{columns}:{right}"
        return self._unsupported_operation("join", target, check=check)

    def group_by(self, *columns: object) -> "GroupedLazyFrame":
        """Return a grouped lazy workflow handle for unsupported aggregation diagnostics."""

        return GroupedLazyFrame(
            workflow=self,
            columns=_normalize_columns(columns),
        )

    def aggregate(
        self,
        *expressions: object,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for DataFrame aggregations."""

        return self._unsupported_operation(
            "aggregate",
            ",".join(_normalize_columns(expressions)),
            check=check,
        )

    def agg(
        self,
        *expressions: object,
        check: bool = False,
        **named_expressions: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for DataFrame-style `agg`."""

        values = list(_normalize_columns(expressions)) if expressions else []
        values.extend(
            f"{_require_non_empty('aggregate name', name)}={_require_non_empty('aggregate expression', expression)}"
            for name, expression in named_expressions.items()
        )
        if not values:
            raise ValueError("aggregate expressions must not be empty")
        return self._unsupported_operation("agg", ",".join(values), check=check)

    def sort(
        self,
        *columns: object,
        descending: bool = False,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for DataFrame sorting."""

        direction = "desc" if descending else "asc"
        target = f"{direction}:{','.join(_normalize_columns(columns))}"
        return self._unsupported_operation("sort", target, check=check)

    def window(
        self,
        *expressions: object,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for DataFrame window functions."""

        return self._unsupported_operation(
            "window",
            ",".join(_normalize_columns(expressions)),
            check=check,
        )

    def schema_contract(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for schema contract enforcement."""

        normalized = _normalize_schema(schema)
        if not normalized:
            raise ValueError("schema contract must not be empty")
        target = ",".join(f"{name}:{dtype}" for name, dtype in normalized)
        return self._unsupported_operation("schema-contract", target, check=check)

    def schema(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for workflow schema discovery."""

        return self._unsupported_operation("schema", check=check)

    def describe_schema(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for rich schema description."""

        return self._unsupported_operation("describe-schema", check=check)

    def validate_schema(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for workflow schema validation."""

        normalized = _normalize_schema(schema)
        if not normalized:
            raise ValueError("schema validation contract must not be empty")
        target = ",".join(f"{name}:{dtype}" for name, dtype in normalized)
        return self._unsupported_operation("validate-schema", target, check=check)

    def data_quality_check(
        self,
        *checks: object,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for data-quality checks."""

        return self._unsupported_operation(
            "data-quality",
            ",".join(_normalize_columns(checks)),
            check=check,
        )

    def data_quality(
        self,
        *checks: object,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Alias for data-quality check unsupported reporting."""

        return self.data_quality_check(*checks, check=check)

    def data_quality_summary(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for data-quality summary output."""

        return self._unsupported_operation("data-quality-summary", check=check)

    def quarantine(
        self,
        target_uri: str | os.PathLike[str] | None = None,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for data-quality quarantine output."""

        target = "none" if target_uri is None else str(target_uri)
        return self._unsupported_operation("quarantine", target, check=check)

    def preview(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for bounded notebook previews."""

        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("preview limit must be an integer")
        if limit <= 0:
            raise ValueError("preview limit must be positive")
        return self._unsupported_operation("preview", str(limit), check=check)

    def display(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for rich notebook display."""

        return self._unsupported_operation("display", check=check)

    def certify(self, *, check: bool = False) -> WorkflowCertificationReport:
        """Return report-only certificate surfaces for this workflow."""

        return WorkflowCertificationReport(
            workflow=self,
            execution_certificate_plan=self.client.execution_certificate_plan(check=check),
            native_io_envelope_plan=self.client.native_io_envelope_plan(check=check),
            certification_capabilities=self.client.capabilities("certification", check=check),
        )

    def engine_selection(
        self,
        *,
        boundedness: str = "snapshot",
        update_mode: str = "snapshot",
        output_mode: str = "snapshot",
        check: bool = False,
    ) -> EngineSelectionPlan:
        """Return engine selection/rejection for this lazy workflow."""

        return self.client.engine_selection_plan(
            self.engine_mode,
            boundedness=boundedness,
            update_mode=update_mode,
            output_mode=output_mode,
            check=check,
        )

    def unsupported_report(self, *, check: bool = False) -> UnsupportedWorkflowReport:
        """Collect unsupported diagnostics and no-fallback evidence for the workflow."""

        return UnsupportedWorkflowReport(
            workflow=self,
            input_plan=self.plan(check=check),
            explain=self.explain(check=check),
            estimate=self.estimate(check=check),
            certification=self.certify(check=check),
        )

    def _append(self, operation: WorkflowOperation) -> "LazyFrame":
        return LazyFrame(
            source=self.source,
            client=self.client,
            operations=(*self.operations, operation),
            engine_mode=self.engine_mode,
        )

    def _unsupported_operation(
        self,
        operation: str,
        target_ref: str | None = None,
        *,
        check: bool,
    ) -> UnsupportedWorkflowOperationReport:
        envelope = self.client.workflow_unsupported_plan(
            operation,
            self.operation_summary,
            target_ref,
            check=check,
        )
        return UnsupportedWorkflowOperationReport(
            workflow=self,
            operation=operation,
            envelope=envelope,
        )


@dataclass(frozen=True, slots=True)
class GroupedLazyFrame:
    """Grouped lazy workflow handle used for unsupported aggregation diagnostics."""

    workflow: LazyFrame
    columns: tuple[str, ...]

    @property
    def operation_summary(self) -> str:
        """Return the grouped workflow summary."""

        return f"{self.workflow.operation_summary} -> group_by({','.join(self.columns)})"

    def agg(
        self,
        *expressions: object,
        check: bool = False,
        **named_expressions: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for grouped aggregation."""

        values = list(_normalize_columns(expressions)) if expressions else []
        values.extend(
            f"{_require_non_empty('aggregate name', name)}={_require_non_empty('aggregate expression', expression)}"
            for name, expression in named_expressions.items()
        )
        if not values:
            raise ValueError("aggregate expressions must not be empty")
        target = f"group_by:{','.join(self.columns)};agg:{','.join(values)}"
        envelope = self.workflow.client.workflow_unsupported_plan(
            "agg",
            self.operation_summary,
            target,
            check=check,
        )
        return UnsupportedWorkflowOperationReport(
            workflow=self.workflow,
            operation="agg",
            envelope=envelope,
        )

    def aggregate(
        self,
        *expressions: object,
        check: bool = False,
        **named_expressions: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Alias for grouped `agg` unsupported reporting."""

        return self.agg(*expressions, check=check, **named_expressions)


def read_vortex(
    uri: str | os.PathLike[str],
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy native Vortex source."""

    return _read_source(
        "vortex",
        uri,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def read_csv(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy CSV compatibility source."""

    return _read_source(
        "csv",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def read_json(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy JSON/NDJSON compatibility source."""

    return _read_source(
        "json",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def read_parquet(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy Parquet compatibility source."""

    return _read_source(
        "parquet",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> GeneratedRowsSource:
    """Create a scoped source-free generated row set for local output smoke writes."""

    schema_arg, rows_arg = _generated_rows_args(rows)
    return GeneratedRowsSource(
        schema_arg=schema_arg,
        rows_arg=rows_arg,
        client=_client_from_config(client, client_config),
    )


def from_pandas(
    dataframe: object,
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> UnsupportedWorkflowOperationReport:
    """Return the unsupported report for a pandas in-memory input boundary."""

    workflow = _materialized_boundary_workflow(
        "pandas",
        _python_object_boundary_ref("pandas", dataframe),
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )
    return workflow._unsupported_operation("from-pandas", workflow.uri, check=check)


def from_arrow_table(
    table: object,
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> UnsupportedWorkflowOperationReport:
    """Return the unsupported report for an Arrow table input boundary."""

    workflow = _materialized_boundary_workflow(
        "arrow_table",
        _python_object_boundary_ref("arrow_table", table),
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )
    return workflow._unsupported_operation("from-arrow-table", workflow.uri, check=check)


def from_arrow_ipc(
    source: object,
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> UnsupportedWorkflowOperationReport:
    """Return the unsupported report for an Arrow IPC input boundary."""

    target = (
        str(source)
        if isinstance(source, (str, os.PathLike))
        else _python_object_boundary_ref("arrow_ipc", source)
    )
    workflow = _materialized_boundary_workflow(
        "arrow_ipc",
        target,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )
    return workflow._unsupported_operation("from-arrow-ipc", workflow.uri, check=check)


def _generated_rows_args(
    rows: Sequence[Mapping[str, object]],
) -> tuple[str, str]:
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise TypeError("rows must be a non-empty sequence of mappings")
    if not rows:
        raise ValueError("rows must not be empty")
    first = rows[0]
    if not isinstance(first, Mapping):
        raise TypeError("rows must contain mappings")
    if any(not isinstance(key, str) for key in first.keys()):
        raise TypeError("generated row column names must be strings")
    columns = tuple(first.keys())
    if not columns or any(column.strip() == "" for column in columns):
        raise ValueError("row column names must not be empty")
    if len(set(columns)) != len(columns):
        raise ValueError("row column names must be unique")
    value_types = tuple(_generated_value_type(first[column]) for column in columns)
    row_tokens: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise TypeError(f"row {index} is not a mapping")
        if any(not isinstance(key, str) for key in row.keys()):
            raise TypeError("generated row column names must be strings")
        row_keys = tuple(row.keys())
        if row_keys != columns:
            raise ValueError(
                "all generated rows must have the same columns in the same order"
            )
        parts = []
        for column, value_type in zip(columns, value_types):
            parts.append(
                f"{_generated_token(column)}={_generated_token(_generated_value(value_type, row[column]))}"
            )
        row_tokens.append(",".join(parts))
    schema_arg = ",".join(
        f"{_generated_token(column)}:{value_type}"
        for column, value_type in zip(columns, value_types)
    )
    return schema_arg, ";".join(row_tokens)


def _generated_value_type(value: object) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("float generated row values must be finite")
        return "float64"
    if isinstance(value, str):
        return "utf8"
    raise TypeError(
        "generated row values must be bool, int, float, or str for the scoped local smoke"
    )


def _generated_value(value_type: str, value: object) -> str:
    if value_type == "bool":
        if not isinstance(value, bool):
            raise TypeError("generated bool columns must contain only bool values")
        return "true" if value else "false"
    if value_type == "int64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("generated int64 columns must contain only int values")
        return str(value)
    if value_type == "float64":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("generated float64 columns must contain only numeric values")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError("float generated row values must be finite")
        return str(numeric)
    if value_type == "utf8":
        if not isinstance(value, str):
            raise TypeError("generated utf8 columns must contain only str values")
        return value
    raise ValueError(f"unsupported generated value type {value_type!r}")


def _generated_token(value: str) -> str:
    return quote(value, safe="")


def _read_source(
    source_format: str,
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    normalized = source_format.strip().lower().replace("_", "-")
    if normalized not in SUPPORTED_SOURCE_FORMATS:
        raise ValueError(
            f"source_format must be one of {SUPPORTED_SOURCE_FORMATS}; got {source_format!r}"
        )
    return LazyFrame(
        source=WorkflowSource(
            source_format=normalized,
            uri=str(uri),
            schema=_normalize_schema(schema),
        ),
        client=_client_from_config(client, client_config),
        engine_mode=_normalize_engine_mode(engine_mode),
    )


def _materialized_boundary_workflow(
    source_format: str,
    uri: str,
    *,
    client: ShardLoomClient | None,
    engine_mode: str,
    **client_config: object,
) -> LazyFrame:
    return LazyFrame(
        source=WorkflowSource(
            source_format=source_format,
            uri=uri,
        ),
        client=_client_from_config(client, client_config),
        engine_mode=_normalize_engine_mode(engine_mode),
    )


def _python_object_boundary_ref(kind: str, value: object) -> str:
    value_type = type(value)
    return f"{kind}:{value_type.__module__}.{value_type.__qualname__}"


def _client_from_config(
    client: ShardLoomClient | None,
    client_config: Mapping[str, object],
) -> ShardLoomClient:
    if client is not None:
        if client_config:
            raise ValueError("client cannot be combined with client configuration arguments")
        return client
    config = dict(client_config)
    binary = config.pop("binary", None)
    env = config.pop("env", None)
    cwd = config.pop("cwd", None)
    repo_root = config.pop("repo_root", None)
    profile_order = config.pop("profile_order", None)
    timeout = config.pop("timeout", None)
    if config:
        unknown = ", ".join(sorted(str(key) for key in config))
        raise TypeError(f"unknown client configuration argument(s): {unknown}")
    if repo_root is not None:
        return ShardLoomClient.from_repo(
            repo_root,
            binary=_optional_binary(binary),
            env=_optional_env(env),
            cwd=_optional_path(cwd),
            profile_order=_optional_profile_order(profile_order) or DEFAULT_PROFILE_ORDER,
            timeout=_optional_timeout(timeout),
        )
    return ShardLoomClient.from_env(
        env=_optional_env(env),
        binary=_optional_binary(binary),
        cwd=_optional_path(cwd),
        profile_order=_optional_profile_order(profile_order),
        timeout=_optional_timeout(timeout),
    )


def _normalize_schema(schema: Mapping[str, object] | None) -> tuple[tuple[str, str], ...]:
    if schema is None:
        return ()
    return tuple((str(key), str(value)) for key, value in schema.items())


def _normalize_engine_mode(engine_mode: str) -> str:
    normalized = engine_mode.strip().lower().replace("_", "-")
    if normalized not in {"auto", "batch", "live", "hybrid"}:
        raise ValueError("engine_mode must be one of ('auto', 'batch', 'live', 'hybrid')")
    return normalized


def _normalize_columns(columns: Sequence[object]) -> tuple[str, ...]:
    if len(columns) == 1 and _is_non_string_sequence(columns[0]):
        values = [str(column).strip() for column in columns[0]]
    else:
        values = [str(column).strip() for column in columns]
    values = [value for value in values if value]
    if not values:
        raise ValueError("select columns must not be empty")
    return tuple(values)


def _require_non_empty(name: str, value: object) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} must not be empty")
    return text


def _is_non_string_sequence(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _optional_binary(value: object) -> Binary | None:
    return cast(Binary | None, value)


def _optional_env(value: object) -> Mapping[str, str] | None:
    return cast(Mapping[str, str] | None, value)


def _optional_path(value: object) -> str | os.PathLike[str] | None:
    return cast(str | os.PathLike[str] | None, value)


def _optional_profile_order(value: object) -> Sequence[str] | None:
    return cast(Sequence[str] | None, value)


def _optional_timeout(value: object) -> float | None:
    return cast(float | None, value)
