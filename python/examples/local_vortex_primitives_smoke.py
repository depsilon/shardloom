from __future__ import annotations

import argparse
from pathlib import Path

from shardloom import OutputEnvelope, ShardLoomClient


DEFAULT_FIXTURE = Path("shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")


ROW_FIELDS = {
    "vortex-run": (
        "local_primitive_rows_scanned",
        "local_primitive_rows_selected",
        "local_primitive_arrays_read_count",
        "work_avoided_known_metrics",
        "work_avoided_decode_avoided",
        "work_avoided_materialization_avoided",
        "work_avoided_fallback_blocked",
    ),
    "vortex-count-where": (
        "count",
        "filtered_count_local_execution_count",
        "filtered_count_local_execution_rows_scanned",
        "filtered_count_local_execution_rows_selected",
        "filtered_count_local_execution_arrays_read_count",
    ),
    "vortex-filter": (
        "rows_selected",
        "filter_local_execution_rows_scanned",
        "filter_local_execution_rows_selected",
        "filter_local_execution_arrays_read_count",
    ),
    "vortex-project": (
        "rows_projected",
        "project_local_execution_rows_scanned",
        "project_local_execution_rows_projected",
        "project_local_execution_projected_columns",
        "project_local_execution_arrays_read_count",
    ),
    "vortex-filter-project": (
        "rows_selected",
        "rows_projected",
        "filter_project_local_execution_rows_scanned",
        "filter_project_local_execution_rows_selected",
        "filter_project_local_execution_rows_projected",
        "filter_project_local_execution_projected_columns",
        "filter_project_local_execution_arrays_read_count",
    ),
}

CERTIFICATE_FIELDS = (
    "local_primitive_native_io_certificate_status",
    "local_primitive_native_io_certified",
    "local_primitive_native_io_pushdown_guarantee",
    "local_primitive_native_io_materialization_boundaries",
    "local_primitive_execution_certificate_status",
    "local_primitive_execution_certificate_correctness_passed",
    "local_primitive_execution_certificate_fixture_id",
)

MATERIALIZATION_FIELDS = (
    "data_read",
    "data_decoded",
    "data_materialized",
    "row_read",
    "arrow_converted",
    "object_store_io",
    "write_io",
    "spill_io_performed",
    "fallback_execution_allowed",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ShardLoom's certified local Vortex primitive smoke workflow."
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--shardloom-bin")
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--predicate", default="gte:value:3")
    parser.add_argument("--columns", default="metric")
    parser.add_argument("--memory-gb", type=int, default=1)
    parser.add_argument("--max-parallelism", type=int, default=2)
    parser.add_argument(
        "--profile-order",
        default="debug,release",
        help="Comma-separated target profile order for ShardLoomClient.from_repo().",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    fixture = args.fixture if args.fixture.is_absolute() else repo_root / args.fixture
    columns = tuple(part.strip() for part in args.columns.split(",") if part.strip())
    if not columns:
        raise ValueError("--columns must contain at least one column")

    if args.shardloom_bin:
        client = ShardLoomClient(binary=args.shardloom_bin)
    else:
        client = ShardLoomClient.from_repo(
            repo_root,
            profile_order=_profile_order(args.profile_order),
        )

    report = client.local_vortex_primitive_smoke(
        fixture,
        predicate=args.predicate,
        columns=columns,
        memory_gb=args.memory_gb,
        max_parallelism=args.max_parallelism,
    )

    print(f"fixture: {fixture}")
    print(f"commands: {', '.join(report.commands)}")
    print(f"all certified: {report.all_certified}")
    print(f"fallback attempted: {report.fallback_attempted}")
    if report.uncertified_commands:
        print(f"uncertified commands: {', '.join(report.uncertified_commands)}")

    for envelope in report.envelopes:
        _print_envelope(envelope)

    return 0 if report.all_certified and not report.fallback_attempted else 1


def _print_envelope(envelope: OutputEnvelope) -> None:
    print("")
    print(f"command: {envelope.command}")
    print(f"status: {envelope.status}")
    print(f"mode: {envelope.field('mode', 'unknown')}")
    print(f"execution: {envelope.field('execution', 'unknown')}")
    print(f"fallback attempted: {envelope.fallback.attempted}")
    _print_fields("work metrics", envelope, ROW_FIELDS.get(envelope.command, ()))
    _print_fields("certificates", envelope, CERTIFICATE_FIELDS)
    _print_fields("materialization", envelope, MATERIALIZATION_FIELDS)
    evidence = _artifact_kinds(envelope)
    if evidence:
        print(f"evidence artifacts: {', '.join(evidence)}")


def _print_fields(label: str, envelope: OutputEnvelope, keys: tuple[str, ...]) -> None:
    parts = [f"{key}={value}" for key in keys if (value := envelope.field(key)) is not None]
    if parts:
        print(f"{label}: {', '.join(parts)}")


def _artifact_kinds(envelope: OutputEnvelope) -> tuple[str, ...]:
    values = [
        str(artifact.get("artifact_kind", ""))
        for artifact in envelope.artifacts
        if artifact.get("artifact_kind")
    ]
    values.extend(
        str(certificate.get("kind", ""))
        for certificate in envelope.certificates
        if certificate.get("kind")
    )
    return tuple(dict.fromkeys(values))


def _profile_order(value: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    return values or ("debug", "release")


if __name__ == "__main__":
    raise SystemExit(main())
