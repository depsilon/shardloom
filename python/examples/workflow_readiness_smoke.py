from __future__ import annotations

import argparse
from pathlib import Path

from shardloom import OutputEnvelope, ShardLoomClient


OUTPUT_FIELDS = (
    "target_format",
    "target_uri",
    "target_is_native_vortex",
    "staged_output_required",
    "write_execution_allowed",
    "commit_execution_allowed",
    "marker_write_allowed",
    "object_store_target",
    "output_data_written",
    "manifest_written",
    "manifest_committed",
    "object_store_io",
    "upstream_vortex_write_called",
    "execution",
)

TABLE_REMOTE_FIELDS = (
    "mode",
    "scenario",
    "source_kind",
    "dataset_format",
    "uri_scheme",
    "capability_status",
    "table_formats_are",
    "blocked_surface_count",
    "claim_blocked",
    "object_store_range_status",
    "planned_request_count",
    "planned_range_count",
    "can_plan_without_io",
    "data_read",
    "object_store_io",
    "write_io",
    "plan_only",
)

EVIDENCE_FIELDS = (
    "scope",
    "migration_report_count",
    "status",
    "fixture_count",
    "decoded_reference_output_coverage_complete",
    "claim_evidence_status",
    "claim_gate_status",
    "blocked_surface_count",
    "performance_claim_allowed",
    "superiority_claim_allowed",
    "fallback_attempted",
    "fallback_execution_allowed",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview ShardLoom workflow readiness without reads, writes, or probes."
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--shardloom-bin")
    parser.add_argument(
        "--target-uri",
        default="file://tmp/shardloom-output-readiness/out.vortex",
        help="Native Vortex output target URI to preview.",
    )
    parser.add_argument(
        "--compat-target-uri",
        default="file://tmp/shardloom-output-readiness/out.parquet",
        help="Compatibility-output target URI to preview.",
    )
    parser.add_argument(
        "--workspace",
        default="target/shardloom-output-readiness-stage",
        help="Local staged workspace path used only for planning.",
    )
    parser.add_argument(
        "--remote-source",
        action="append",
        metavar="NAME=URI",
        help="Override or add a remote source planning URI.",
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

    report = client.workflow_readiness_smoke(
        target_uri=args.target_uri,
        workspace_path=args.workspace,
        compatibility_target_uri=args.compat_target_uri,
        remote_sources=_remote_sources(args.remote_source),
    )

    print(f"commands: {', '.join(report.commands)}")
    print(f"plans: {', '.join(report.plan_names)}")
    print(f"all no write: {report.all_no_write}")
    print(f"all report only/planned: {report.all_report_only_or_planned}")
    print(f"fallback attempted: {report.fallback_attempted}")
    if report.blocked_plan_names:
        print(f"blocked/incomplete plans: {', '.join(report.blocked_plan_names)}")

    _print_group("output/commit readiness", report.output_commit, OUTPUT_FIELDS)
    _print_group("table/remote readiness", report.table_remote, TABLE_REMOTE_FIELDS)
    _print_group("evidence readiness", report.evidence, EVIDENCE_FIELDS)

    return (
        0
        if report.all_no_write
        and report.all_report_only_or_planned
        and not report.fallback_attempted
        else 1
    )


def _print_group(label: str, plans, keys: tuple[str, ...]) -> None:
    print("")
    print(label)
    for plan in plans:
        envelope = plan.envelope
        print(f"- {plan.name}: command={envelope.command}, status={envelope.status}")
        _print_fields("  fields", envelope, keys)
        if envelope.has_error_diagnostics:
            print(f"  error diagnostics: {len(envelope.diagnostics)}")


def _print_fields(label: str, envelope: OutputEnvelope, keys: tuple[str, ...]) -> None:
    parts = [f"{key}={value}" for key in keys if (value := envelope.field(key)) is not None]
    if parts:
        print(f"{label}: {', '.join(parts)}")


def _remote_sources(values: list[str] | None) -> dict[str, str] | None:
    if values is None:
        return None
    sources: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--remote-source values must use NAME=URI")
        name, uri = value.split("=", 1)
        name = name.strip()
        uri = uri.strip()
        if not name or not uri:
            raise ValueError("--remote-source values must include non-empty NAME and URI")
        sources[name] = uri
    return sources


def _profile_order(value: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    return values or ("debug", "release")


if __name__ == "__main__":
    raise SystemExit(main())
