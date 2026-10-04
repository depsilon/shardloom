# SPDX-License-Identifier: Apache-2.0
"""Shared native admission evidence for public collect and direct writers."""


def require_native_resource_admission(name, envelope):
    peak_key = "resident_peak_reserved_buffer_bytes"
    if envelope.field("public_workflow_route_id") == "native_vortex_primitive_row_export":
        if (envelope.field("native_vortex_result_export_kind") != "owned_native_array_stream"
                or envelope.field("native_vortex_array_sink_source_generation_validated") != "true"):
            raise ValueError(f"{name}: direct writer lacks owned native stream evidence")
        # All native writers snapshot the same admitted producer session's pool.
        peak_key = "native_vortex_array_sink_peak_reserved_bytes"
    peak = envelope.field(peak_key)
    if (envelope.field("public_workflow_memory_gb") != "1"
            or envelope.field("public_workflow_native_vortex_provider_scenario") != "none"
            or not isinstance(peak, str) or not peak.isascii() or not peak.isdecimal()
            or int(peak) > 1 << 30):
        raise ValueError(f"{name}: shared native resource admission differs")
