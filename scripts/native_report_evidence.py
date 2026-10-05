# SPDX-License-Identifier: Apache-2.0
"""Shared native admission evidence for public collect and direct writers."""


def has_diagnostic_detail(envelope, detail):
    """Both standard diagnostic constructors retain their specific explanation."""
    return any(detail in (item.get(key) or "")
               for item in envelope.raw.get("diagnostics", [])
               for key in ("message", "reason"))


def require_native_resource_admission(name, envelope):
    required = {
        "public_workflow_memory_gb": "1",
        "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
        "resident_provider_crate": "vortex",
    }
    for key, expected in required.items():
        if envelope.field(key) != expected:
            raise ValueError(f"{name}: {key} differs from shared native admission")
    # Collect and all writers snapshot the admitted producer session's pool.
    peak = envelope.field("resident_peak_reserved_buffer_bytes")
    if (not isinstance(peak, str) or not peak.isascii() or not peak.isdecimal()
            or int(peak) > 1 << 30):
        raise ValueError(f"{name}: shared native resource admission differs")
