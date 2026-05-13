from __future__ import annotations

import argparse
from pathlib import Path

from shardloom import ShardLoomClient


PLAN_FIELDS = (
    "source_kind",
    "adapter_kind",
    "dataset_format",
    "uri_scheme",
    "capability_status",
    "metadata_availability",
    "fidelity",
    "materialization_risk",
    "native_vortex",
    "compatibility_structured",
    "requires_credentials",
    "plan_only",
    "execution",
    "data_read",
    "data_materialized",
    "object_store_io",
    "write_io",
    "fallback_execution_allowed",
)

NATIVE_IO_FIELDS = (
    "certificate_path_order",
    "per_path_certificate_required",
    "source_pushdown_proof_required",
    "sink_requirement_propagation_required",
    "adapter_fidelity_report_required",
    "materialization_boundary_required_for_decoded_columnar",
    "materialization_boundary_required_for_rows",
    "runtime_execution",
    "data_read",
    "write_io",
    "fallback_attempted",
)

ADAPTER_FIELDS = (
    "critical_structured_adapter_order",
    "csv_status",
    "jsonl_status",
    "parquet_status",
    "arrow_ipc_status",
    "plan_only",
    "execution",
    "write_io",
    "fallback_execution_allowed",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan ShardLoom compatibility-source inputs without reading data."
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--shardloom-bin")
    parser.add_argument(
        "--source",
        action="append",
        metavar="NAME=URI",
        help="Override or add a planned source, for example csv=data/fact.csv.",
    )
    parser.add_argument(
        "--profile-order",
        default="debug,release",
        help="Comma-separated target profile order for ShardLoomClient.from_repo().",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.shardloom_bin:
        client = ShardLoomClient(binary=args.shardloom_bin)
    else:
        client = ShardLoomClient.from_repo(
            args.repo_root.resolve(),
            profile_order=_profile_order(args.profile_order),
        )

    report = client.compatibility_source_smoke(_sources(args.source))

    print(f"commands: {', '.join(report.commands)}")
    print(f"compatibility sources: {', '.join(report.compatibility_source_names)}")
    print(f"planned sources: {', '.join(report.planned_source_names)}")
    print(f"all plan only: {report.all_plan_only}")
    print(f"fallback attempted: {report.fallback_attempted}")
    _print_fields("adapter registry", report.input_adapters, ADAPTER_FIELDS)
    _print_fields("Native I/O requirements", report.native_io_envelope, NATIVE_IO_FIELDS)

    for source in report.sources:
        print("")
        print(f"source: {source.source_name}")
        print(f"uri: {source.dataset_uri}")
        print(f"status: {source.plan.status}")
        _print_fields("plan", source.plan, PLAN_FIELDS)
        artifacts = _artifact_kinds(source.plan)
        if artifacts:
            print(f"evidence artifacts: {', '.join(artifacts)}")

    return 0 if report.all_plan_only and not report.fallback_attempted else 1


def _print_fields(label: str, envelope, keys: tuple[str, ...]) -> None:
    parts = [f"{key}={value}" for key in keys if (value := envelope.field(key)) is not None]
    if parts:
        print(f"{label}: {', '.join(parts)}")


def _artifact_kinds(envelope) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            str(artifact.get("artifact_kind", ""))
            for artifact in envelope.artifacts
            if artifact.get("artifact_kind")
        )
    )


def _sources(values: list[str] | None) -> dict[str, str] | None:
    if values is None:
        return None
    sources: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--source values must use NAME=URI")
        name, uri = value.split("=", 1)
        name = name.strip()
        uri = uri.strip()
        if not name or not uri:
            raise ValueError("--source values must include non-empty NAME and URI")
        sources[name] = uri
    return sources


def _profile_order(value: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    return values or ("debug", "release")


if __name__ == "__main__":
    raise SystemExit(main())
