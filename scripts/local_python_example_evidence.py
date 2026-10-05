# SPDX-License-Identifier: Apache-2.0
"""Shared release evidence contract for the runnable local Python quickstart."""

RESULT_MARKERS = (
    "quickstart_local_file_blocker_id=none",
    "quickstart_local_file_route_status=passed",
    "quickstart_local_file_runtime_execution=true",
    "quickstart_local_file_native_plan_family=native_vortex_unified_plan",
    "quickstart_local_file_source_opens=1",
    "quickstart_local_file_output_row_count=2",
    "quickstart_local_file_fallback_attempted=false",
    "quickstart_local_file_external_engine_invoked=false",
    "quickstart_local_file_result_rows=",
    "quickstart_generated_input_row_count=1",
    "quickstart_generated_native_plan_family=native_vortex_unified_plan",
    "quickstart_generated_source_opens=0",
    "quickstart_generated_result_verified=true",
    "quickstart_generated_rows_written=1",
    "quickstart_generated_output_row_count=1",
    "quickstart_generated_output_path=",
    "quickstart_generated_output_commit_status=committed",
    "quickstart_generated_fallback_attempted=false",
    "quickstart_generated_external_engine_invoked=false",
    "quickstart_generated_claim_gate_status=",
)

UNSUPPORTED_MARKERS = (
    "quickstart_unsupported_blocker_id=",
    "quickstart_unsupported_runtime_execution=false",
    "quickstart_unsupported_data_read=false",
    "quickstart_unsupported_write_io=false",
    "quickstart_unsupported_fallback_attempted=false",
    "quickstart_unsupported_external_engine_invoked=false",
)


def marker_present(stdout: str, marker: str) -> bool:
    """Require one unambiguous complete field, with a nonempty declared value."""
    key = marker.split("=", 1)[0] + "="
    lines = [line for line in stdout.splitlines() if line.startswith(key)]
    if len(lines) != 1:
        return False
    if marker.endswith("="):
        return lines[0][len(marker):].strip() not in ("", "None")
    return lines[0] == marker
