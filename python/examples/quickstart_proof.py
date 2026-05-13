from __future__ import annotations

import argparse
from pathlib import Path

from shardloom import QuickstartProofReport, ShardLoomClient, quickstart_proof


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ShardLoom's local quickstart proof from a source checkout."
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--shardloom-bin")
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path("shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex"),
    )
    parser.add_argument("--predicate", default="gte:value:3")
    parser.add_argument("--columns", default="metric")
    parser.add_argument(
        "--run-local-vortex",
        action="store_true",
        help="Also run the certified local Vortex primitive fixture smoke.",
    )
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

    report = quickstart_proof(
        client,
        fixture=fixture,
        predicate=args.predicate,
        columns=columns,
        run_local_vortex=args.run_local_vortex,
        memory_gb=args.memory_gb,
        max_parallelism=args.max_parallelism,
    )
    _print_report(report, fixture)

    local_ok = (not args.run_local_vortex) or report.local_execution_certified
    return (
        0
        if report.all_no_write_planning
        and local_ok
        and not report.fallback_attempted
        else 1
    )


def _print_report(report: QuickstartProofReport, fixture: Path) -> None:
    print(f"fixture: {fixture}")
    print(f"commands: {', '.join(report.commands)}")
    print(f"fallback attempted: {report.fallback_attempted}")
    print(f"planning no-write: {report.all_no_write_planning}")
    print(f"local execution ran: {report.local_execution_ran}")
    print(f"local execution certified: {report.local_execution_certified}")
    print("")
    print("smoke")
    print(f"  protocol: {report.smoke.protocol_version}")
    print(f"  cli: {report.smoke.resolved_cli_path}")
    print(f"  version: {report.smoke.cli_version}")
    print("")
    print("capabilities")
    for scope, envelope in report.capabilities.items():
        state = (
            envelope.field("capability_status")
            or envelope.field("certification_status")
            or envelope.field("support_status")
            or envelope.status
        )
        print(f"  {scope}: {state}")
    print("")
    print("lazy workflow")
    print(f"  summary: {report.workflow.operation_summary}")
    print(f"  input plan: {report.workflow_report.input_plan.status}")
    print(f"  explain: {report.workflow_report.explain.status}")
    print(f"  estimate: {report.workflow_report.estimate.status}")
    print(f"  unsupported reasons: {len(report.workflow_report.unsupported_reasons)}")
    print("")
    print("compatibility sources")
    print(f"  compatibility: {', '.join(report.compatibility_sources.compatibility_source_names)}")
    print(f"  planned: {', '.join(report.compatibility_sources.planned_source_names)}")
    print("")
    print("workflow readiness")
    print(f"  plans: {len(report.readiness.plans)}")
    print(f"  blocked/incomplete: {', '.join(report.readiness.blocked_plan_names)}")
    if report.local_vortex is not None:
        print("")
        print("local Vortex execution")
        print(f"  certified: {report.local_vortex.all_certified}")
        print(f"  commands: {', '.join(report.local_vortex.commands)}")


def _profile_order(value: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    return values or ("debug", "release")


if __name__ == "__main__":
    raise SystemExit(main())
