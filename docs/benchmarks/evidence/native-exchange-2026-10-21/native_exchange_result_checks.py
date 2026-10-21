#!/usr/bin/env python3
"""Deterministic checks for one supervised native-exchange workflow record."""

from __future__ import annotations

import ipaddress
import hashlib
import json
import math
import re
from collections import defaultdict
from numbers import Real
from typing import Any


_GRANT_BYTES = 256 * 1024 * 1024
_COMBINED_RSS_LIMIT = 2 * 1024 * 1024 * 1024
_EXECUTION_ORDER = (0, 1, 2, 1)
_NATIVE_MODES = {"native_fragments", "native_assembled"}
_ARROW_MODES = {"arrow_none", "arrow_lz4", "arrow_zstd"}
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_FIELDS = (
    ("source_id", "source_id"),
    ("mode", "mode"),
    ("server_epoch", "server_epoch"),
    ("attempt_id", "attempt_id"),
    ("certificate_manifest_sha256", "certificate_manifest_sha256"),
)
_UNKNOWN_REPORT_FIELDS = (
    "provider_allocation_bytes",
    "provider_copy_bytes",
    "tls_runtime_kernel_allocation_bytes",
    "encrypted_tcp_bytes",
)


class _Checks:
    def __init__(self) -> None:
        self.counts: dict[str, int] = defaultdict(int)

    def require(self, category: str, condition: bool, path: str, reason: str) -> None:
        if not condition:
            raise ValueError(f"{path}: {reason}")
        self.counts[category] += 1

    def equal(self, category: str, actual: Any, expected: Any, path: str) -> None:
        if not _same_json(actual, expected):
            raise ValueError(f"{path}: expected {expected!r}, got {actual!r}")
        self.counts[category] += 1


def _object(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected an object")
    return value


def _array(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{path}: expected an array")
    return value


def _field(value: dict[str, Any], key: str, path: str) -> Any:
    if key not in value:
        raise ValueError(f"{path}.{key}: required field is missing")
    return value[key]


def _integer(value: Any, path: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{path}: expected an integer")
    return value


def _finite_nonnegative(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{path}: expected a finite nonnegative number")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{path}: expected a finite nonnegative number")
    return number


def _socket(value: Any, path: str) -> tuple[str, int]:
    if not isinstance(value, str):
        raise ValueError(f"{path}: expected a socket address string")
    if value.startswith("["):
        close = value.find("]")
        if close < 0 or value[close + 1 : close + 2] != ":":
            raise ValueError(f"{path}: malformed bracketed socket address {value!r}")
        host, port_text = value[1:close], value[close + 2 :]
    else:
        host, separator, port_text = value.rpartition(":")
        if not separator:
            raise ValueError(f"{path}: malformed socket address {value!r}")
    if not re.fullmatch(r"[0-9]+", port_text):
        raise ValueError(f"{path}: malformed socket port in {value!r}")
    try:
        address = ipaddress.ip_address(host)
        port = int(port_text)
    except ValueError as error:
        raise ValueError(f"{path}: malformed socket address {value!r}") from error
    if not address.is_loopback:
        raise ValueError(f"{path}: address {host!r} is not loopback")
    if port <= 0 or port > 65535:
        raise ValueError(f"{path}: port must be in 1..65535")
    return str(address), port


def _sha256(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{path}: expected a lowercase 64-digit SHA-256")
    return value


def _same_json(actual: Any, expected: Any) -> bool:
    """Compare JSON values without Python's bool/int equality coercion."""
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _same_json(actual[key], expected[key]) for key in expected
        )
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _same_json(left, right) for left, right in zip(actual, expected)
        )
    return actual == expected


def _case_by_id(cases: Any, source_id: str, path: str) -> dict[str, Any]:
    found = [case for case in _array(cases, path) if isinstance(case, dict) and case.get("id") == source_id]
    if len(found) != 1:
        raise ValueError(f"{path}: expected exactly one case for source {source_id!r}, found {len(found)}")
    return found[0]


def _check_query_result(
    checks: _Checks,
    actual: Any,
    expected: dict[str, Any],
    path: str,
) -> None:
    result = _object(actual, path)
    for output_key, reference_key in (
        ("query", "id"),
        ("rows", "rows"),
        ("dtype", "dtype"),
        ("jsonl_bytes", "jsonl_bytes"),
        ("complete_jsonl_sha256", "jsonl_sha256"),
    ):
        checks.equal(
            "results",
            _field(result, output_key, path),
            _field(expected, reference_key, f"manifest query {expected.get('id', '<unknown>')}"),
            f"{path}.{output_key}",
        )
    checks.equal("results", _field(result, "certified", path), True, f"{path}.certified")
    checks.equal("results", _field(result, "fallback_attempted", path), False, f"{path}.fallback_attempted")
    _finite_nonnegative(_field(result, "engine_execution_seconds", path), f"{path}.engine_execution_seconds")
    checks.counts["results"] += 1
    _finite_nonnegative(
        _field(result, "result_serialization_seconds", path),
        f"{path}.result_serialization_seconds",
    )
    checks.counts["results"] += 1


def verify_workflow(record: dict, manifest: dict, freeze: dict) -> dict:
    """Validate one parent workflow record against child, manifest and freeze evidence.

    This function is deterministic and has no filesystem or process side effects. It raises
    ``ValueError`` at the first missing or inconsistent field and returns successful check counts.
    """
    checks = _Checks()
    record = _object(record, "record")
    manifest = _object(manifest, "manifest")
    freeze = _object(freeze, "freeze")
    configuration = _object(_field(record, "configuration", "record"), "record.configuration")
    required_config = {
        "version",
        "fixture_root",
        "tls_root",
        "certificate_manifest_sha256",
        "source_id",
        "mode",
        "server_epoch",
        "attempt_id",
    }
    checks.equal("configuration", set(configuration), required_config, "record.configuration keys")
    checks.equal("configuration", _integer(_field(configuration, "version", "record.configuration"), "record.configuration.version"), 1, "record.configuration.version")
    for key in ("fixture_root", "tls_root"):
        value = _field(configuration, key, "record.configuration")
        checks.require("configuration", isinstance(value, str) and value.startswith("/"), f"record.configuration.{key}", "expected an absolute path")

    source_id = _field(configuration, "source_id", "record.configuration")
    mode = _field(configuration, "mode", "record.configuration")
    if not isinstance(source_id, str) or not source_id:
        raise ValueError("record.configuration.source_id: expected a nonempty string")
    if not isinstance(mode, str):
        raise ValueError("record.configuration.mode: expected a string")
    allowed_modes = set(_array(_field(freeze, "modes", "freeze"), "freeze.modes")) | {"sql"}
    checks.require("configuration", mode in allowed_modes, "record.configuration.mode", f"unknown frozen mode {mode!r}")
    limits = _object(_field(freeze, "limits", "freeze"), "freeze.limits")
    checks.equal("configuration", _integer(_field(limits, "native_memory_bytes", "freeze.limits"), "freeze.limits.native_memory_bytes"), _GRANT_BYTES, "freeze.limits.native_memory_bytes")
    frozen_resources = _object(_field(freeze, "resources", "freeze"), "freeze.resources")
    checks.equal("resources", _integer(_field(frozen_resources, "combined_peer_rss_bytes", "freeze.resources"), "freeze.resources.combined_peer_rss_bytes"), _COMBINED_RSS_LIMIT, "freeze.resources.combined_peer_rss_bytes")

    epoch = _field(configuration, "server_epoch", "record.configuration")
    attempt_id = _field(configuration, "attempt_id", "record.configuration")
    for key, identity in (("server_epoch", epoch), ("attempt_id", attempt_id)):
        if not isinstance(identity, str) or not re.fullmatch(r"[0-9a-f]{32}", identity) or identity == "0" * 32:
            raise ValueError(f"record.configuration.{key}: expected a nonzero 16-byte lowercase hex identity")
        checks.counts["configuration"] += 1

    config_hash = _sha256(_field(record, "configuration_sha256", "record"), "record.configuration_sha256")
    cert_hash = _sha256(
        _field(configuration, "certificate_manifest_sha256", "record.configuration"),
        "record.configuration.certificate_manifest_sha256",
    )
    canonical_config = json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()
    checks.equal("identity", config_hash, hashlib.sha256(canonical_config).hexdigest(), "record.configuration_sha256")

    manifest_case = _case_by_id(_field(manifest, "cases", "manifest"), source_id, "manifest.cases")
    freeze_case = _case_by_id(_field(freeze, "cases", "freeze"), source_id, "freeze.cases")
    operation_hash = _sha256(_field(freeze_case, "operation_sha256", f"freeze.cases[{source_id}]"), f"freeze.cases[{source_id}].operation_sha256")

    sender_pid = _integer(_field(record, "sender_pid", "record"), "record.sender_pid")
    receiver_pid = _integer(_field(record, "receiver_pid", "record"), "record.receiver_pid")
    checks.require("identity", sender_pid > 0, "record.sender_pid", "must be a positive child PID")
    checks.require("identity", receiver_pid > 0, "record.receiver_pid", "must be a positive child PID")
    checks.require("identity", sender_pid != receiver_pid, "record.receiver_pid", "must differ from sender PID")
    sender = _object(_field(record, "sender", "record"), "record.sender")
    receiver = _object(_field(record, "receiver", "record"), "record.receiver")
    checks.equal("identity", _field(sender, "role", "record.sender"), "sender", "record.sender.role")
    checks.equal("identity", _field(receiver, "role", "record.receiver"), "receiver", "record.receiver.role")
    checks.equal("identity", _integer(_field(sender, "version", "record.sender"), "record.sender.version"), 1, "record.sender.version")
    checks.equal("identity", _integer(_field(receiver, "version", "record.receiver"), "record.receiver.version"), 1, "record.receiver.version")
    checks.equal("identity", _integer(_field(sender, "pid", "record.sender"), "record.sender.pid"), sender_pid, "record.sender.pid")
    checks.equal("identity", _integer(_field(receiver, "pid", "record.receiver"), "record.receiver.pid"), receiver_pid, "record.receiver.pid")
    checks.equal("identity", _integer(_field(receiver, "sender_pid", "record.receiver"), "record.receiver.sender_pid"), sender_pid, "record.receiver.sender_pid")
    for child_name, child in (("sender", sender), ("receiver", receiver)):
        child_path = f"record.{child_name}"
        for key in ("metadata_preparation_seconds", "cleanup_seconds"):
            _finite_nonnegative(_field(child, key, child_path), f"{child_path}.{key}")
            checks.counts["clocks"] += 1
        for config_key, report_key in _IDENTITY_FIELDS:
            checks.equal("identity", _field(child, report_key, child_path), _field(configuration, config_key, "record.configuration"), f"{child_path}.{report_key}")
        checks.equal("identity", _sha256(_field(child, "configuration_sha256", child_path), f"{child_path}.configuration_sha256"), config_hash, f"{child_path}.configuration_sha256")
        checks.equal("identity", _sha256(_field(child, "frozen_suite_operation_sha256", child_path), f"{child_path}.frozen_suite_operation_sha256"), operation_hash, f"{child_path}.frozen_suite_operation_sha256")
        checks.equal("identity", _field(child, "certificate_manifest_sha256", child_path), cert_hash, f"{child_path}.certificate_manifest_sha256")
        checks.equal("identity", _field(child, "fallback_attempted", child_path), False, f"{child_path}.fallback_attempted")
        checks.equal("identity", _field(child, "external_engine_invoked", child_path), False, f"{child_path}.external_engine_invoked")
        for key in _UNKNOWN_REPORT_FIELDS:
            checks.equal("identity", _field(child, key, child_path), None, f"{child_path}.{key}")

    checks.equal("barrier", _integer(_field(record, "sender_exit_code", "record"), "record.sender_exit_code"), 0, "record.sender_exit_code")
    checks.equal("barrier", _integer(_field(record, "receiver_exit_code", "record"), "record.receiver_exit_code"), 0, "record.receiver_exit_code")
    checks.equal("barrier", _field(record, "sender_reaped_before_consume", "record"), True, "record.sender_reaped_before_consume")
    checks.equal("barrier", _field(receiver, "sender_exited_before_consumer", "record.receiver"), True, "record.receiver.sender_exited_before_consumer")
    checks.equal("barrier", _field(record, "owned_processes_drained", "record"), True, "record.owned_processes_drained")
    for owner, key in (("sender", "source_preparation_seconds"), ("receiver", "connection_seconds"),
                       ("receiver", "transfer_and_admission_seconds")):
        _finite_nonnegative(_field(record[owner], key, f"record.{owner}"), f"record.{owner}.{key}")
        checks.counts["clocks"] += 1
    received = _object(_field(record, "received_before_consumer", "record"), "record.received_before_consumer")
    for key, expected in (("event", "received"), ("pid", receiver_pid), ("sender_pid", sender_pid),
                          ("configuration_sha256", config_hash), ("completed_executions", 0)):
        checks.equal("barrier", _field(received, key, "record.received_before_consumer"), expected,
                     f"record.received_before_consumer.{key}")

    for child_name, child in (("sender", sender), ("receiver", receiver)):
        child_path = f"record.{child_name}"
        checks.equal("ownership", _integer(_field(child, "native_grant_bytes", child_path), f"{child_path}.native_grant_bytes"), _GRANT_BYTES, f"{child_path}.native_grant_bytes")
        peak = _integer(_field(child, "peak_reserved_bytes", child_path), f"{child_path}.peak_reserved_bytes")
        checks.require("ownership", 0 <= peak <= _GRANT_BYTES, f"{child_path}.peak_reserved_bytes", f"must be within 0..{_GRANT_BYTES}")
        checks.equal("ownership", _integer(_field(child, "final_reserved_bytes", child_path), f"{child_path}.final_reserved_bytes"), 0, f"{child_path}.final_reserved_bytes")

    service = _object(_field(sender, "service", "record.sender"), "record.sender.service")
    checks.equal("ownership", _integer(_field(service, "active_attempts", "record.sender.service"), "record.sender.service.active_attempts"), 0, "record.sender.service.active_attempts")
    checks.equal("ownership", _integer(_field(service, "queued_bytes", "record.sender.service"), "record.sender.service.queued_bytes"), 0, "record.sender.service.queued_bytes")
    checks.equal("ownership", _integer(_field(service, "queued_frames", "record.sender.service"), "record.sender.service.queued_frames"), 0, "record.sender.service.queued_frames")
    checks.equal("ownership", _integer(_field(sender, "final_live_connections", "record.sender"), "record.sender.final_live_connections"), 0, "record.sender.final_live_connections")
    checks.equal("transport", _integer(_field(sender, "accepted_connections", "record.sender"), "record.sender.accepted_connections"), 1, "record.sender.accepted_connections")
    checks.equal("transport", _integer(_field(sender, "peak_live_connections", "record.sender"), "record.sender.peak_live_connections"), 1, "record.sender.peak_live_connections")

    transfers = _array(_field(service, "transfers", "record.sender.service"), "record.sender.service.transfers")
    if len(transfers) != 1:
        raise ValueError(f"record.sender.service.transfers: expected exactly one transfer, found {len(transfers)}")
    transfer = _object(transfers[0], "record.sender.service.transfers[0]")
    checks.equal("transport", _field(transfer, "success", "record.sender.service.transfers[0]"), True, "record.sender.service.transfers[0].success")
    checks.equal("transport", _integer(_field(transfer, "sender_pid", "record.sender.service.transfers[0]"), "record.sender.service.transfers[0].sender_pid"), sender_pid, "record.sender.service.transfers[0].sender_pid")
    checks.equal("transport", _field(transfer, "principal", "record.sender.service.transfers[0]"), "reader", "record.sender.service.transfers[0].principal")
    checks.equal("transport", _field(transfer, "source_id", "record.sender.service.transfers[0]"), source_id, "record.sender.service.transfers[0].source_id")
    ready_address = _field(record, "sender_ready_address", "record")
    checks.equal("transport", _field(transfer, "local_socket", "record.sender.service.transfers[0]"), ready_address, "record.sender.service.transfers[0].local_socket")
    local_ip, local_port = _socket(_field(transfer, "local_socket", "record.sender.service.transfers[0]"), "record.sender.service.transfers[0].local_socket")
    remote_ip, remote_port = _socket(_field(transfer, "remote_socket", "record.sender.service.transfers[0]"), "record.sender.service.transfers[0].remote_socket")
    checks.require("transport", local_port != remote_port, "record.sender.service.transfers[0]", "loopback local and remote ports must differ")
    checks.counts["transport"] += 2

    resources = _object(_field(record, "resources", "record"), "record.resources")
    child_rss_peaks = []
    for child_name in ("sender", "receiver"):
        child_resources = _object(_field(resources, child_name, "record.resources"), f"record.resources.{child_name}")
        for key in ("user_cpu_seconds", "system_cpu_seconds"):
            _finite_nonnegative(_field(child_resources, key, f"record.resources.{child_name}"), f"record.resources.{child_name}.{key}")
            checks.counts["resources"] += 1
        peak_rss = _integer(_field(child_resources, "peak_rss_bytes", f"record.resources.{child_name}"), f"record.resources.{child_name}.peak_rss_bytes")
        checks.require("resources", peak_rss > 0, f"record.resources.{child_name}.peak_rss_bytes", "must be positive")
        child_rss_peaks.append(peak_rss)
    sampled_peak = _integer(_field(resources, "peak_combined_sampled_rss_bytes", "record.resources"), "record.resources.peak_combined_sampled_rss_bytes")
    checks.require("resources", 0 < sampled_peak <= _COMBINED_RSS_LIMIT, "record.resources.peak_combined_sampled_rss_bytes", f"must be within 1..{_COMBINED_RSS_LIMIT}")
    checks.require("resources", sum(child_rss_peaks) <= _COMBINED_RSS_LIMIT, "record.resources", f"sum of child peak RSS must not exceed {_COMBINED_RSS_LIMIT}")
    samples = _array(_field(resources, "rss_samples", "record.resources"), "record.resources.rss_samples")
    checks.require("resources", len(samples) > 0, "record.resources.rss_samples", "must contain at least one sample")
    observed_peak = 0
    previous_elapsed = -1.0
    for index, sample in enumerate(samples):
        path = f"record.resources.rss_samples[{index}]"
        sample = _object(sample, path)
        elapsed = _finite_nonnegative(_field(sample, "elapsed_seconds", path), f"{path}.elapsed_seconds")
        checks.require("resources", elapsed >= previous_elapsed, path, "sample clocks moved backwards")
        previous_elapsed = elapsed
        processes = _array(_field(sample, "processes", path), f"{path}.processes")
        pids = set()
        combined = 0
        for process in processes:
            process = _object(process, path)
            pid = _integer(_field(process, "pid", path), f"{path}.pid")
            checks.require("resources", pid in {sender_pid, receiver_pid} and pid not in pids,
                           path, "RSS sample has an unknown or duplicate process")
            pids.add(pid)
            rss = _integer(_field(process, "rss_bytes", path), f"{path}.rss_bytes")
            checks.require("resources", rss >= 0, path, "RSS must be nonnegative")
            combined += rss
        checks.require("resources", bool(pids), path, "RSS sample has no processes")
        observed_peak = max(observed_peak, combined)
    checks.equal("resources", sampled_peak, observed_peak, "record.resources.peak_combined_sampled_rss_bytes")

    receive = _object(_field(receiver, "receive", "record.receiver"), "record.receiver.receive")
    intake = _object(_field(receive, "native_intake", "record.receiver.receive"), "record.receiver.receive.native_intake")
    for key in ("physical_decoder_bytes", "provider_internal_allocations_and_copies"):
        checks.equal("identity", _field(intake, key, "record.receiver.receive.native_intake"), None, f"record.receiver.receive.native_intake.{key}")
    for key in ("encoded_array_nodes_copied", "encoded_buffer_bytes_copied", "encoded_buffer_copies",
                "ipc_buffer_bytes_copied", "ipc_coordinate_arrays_executed", "ipc_coordinate_values_checked",
                "ipc_decode_workspace_peak_reserved_bytes", "ipc_transport_bytes", "materialized_batches",
                "materialized_rows", "provider_scratch_peak_reserved_bytes", "retained_metadata_bytes",
                "reused_array_references", "reused_buffer_references"):
        value = _integer(_field(intake, key, "record.receiver.receive.native_intake"), f"native_intake.{key}")
        checks.require("ownership", value >= 0, f"native_intake.{key}", "counter must be nonnegative")

    if mode == "sql":
        checks.equal("configuration", source_id, "dictionary_large", "record.configuration.source_id for SQL")
        sql_contract = _object(_field(freeze, "ordinary_sql_client", "freeze"), "freeze.ordinary_sql_client")
        checks.equal("execution", _field(receive, "sql", "record.receiver.receive"), _field(sql_contract, "sql", "freeze.ordinary_sql_client"), "record.receiver.receive.sql")
        checks.equal("execution", _field(receive, "client", "record.receiver.receive"), _field(sql_contract, "provider", "freeze.ordinary_sql_client"), "record.receiver.receive.client")
        checks.equal("execution", _field(receiver, "completed_executions", "record.receiver"), 1, "record.receiver.completed_executions")
        reference_queries = _array(_field(manifest_case, "queries", f"manifest.cases[{source_id}]"), f"manifest.cases[{source_id}].queries")
        if len(reference_queries) <= 2:
            raise ValueError(f"manifest.cases[{source_id}].queries: frozen SQL reference query index 2 is missing")
        sql_reference = _object(reference_queries[2], f"manifest.cases[{source_id}].queries[2]")
        checks.equal("execution", _field(sql_contract, "reference_query", "freeze.ordinary_sql_client"), _field(sql_reference, "id", "manifest SQL query"), "freeze.ordinary_sql_client.reference_query")
        _check_query_result(checks, _field(receiver, "consumer", "record.receiver"), sql_reference, "record.receiver.consumer")
        checks.equal("execution", _field(receive, "unsupported_sql_refused", "record.receiver.receive"), True, "record.receiver.receive.unsupported_sql_refused")
        checks.equal("execution", _field(receive, "unsupported_command_refused", "record.receiver.receive"), True, "record.receiver.receive.unsupported_command_refused")
        checks.equal("execution", _field(receive, "completed_ticket_replay_refused", "record.receiver.receive"), True, "record.receiver.receive.completed_ticket_replay_refused")
        checks.equal("execution", _field(receive, "ordinary_arrow_results", "record.receiver.receive"), True, "record.receiver.receive.ordinary_arrow_results")
        send = _object(_field(transfer, "send", "record.sender.service.transfers[0]"), "record.sender.service.transfers[0].send")
        native_execution = _object(_field(send, "native_execution", "record.sender.service.transfers[0].send"), "record.sender.service.transfers[0].send.native_execution")
        checks.equal("execution", _field(native_execution, "completed_executions", "record.sender.service.transfers[0].send.native_execution"), 1, "record.sender.service.transfers[0].send.native_execution.completed_executions")
        checks.equal("execution", _field(native_execution, "certified", "record.sender.service.transfers[0].send.native_execution"), True, "record.sender.service.transfers[0].send.native_execution.certified")
        checks.equal("execution", _field(native_execution, "fallback_attempted", "record.sender.service.transfers[0].send.native_execution"), False, "record.sender.service.transfers[0].send.native_execution.fallback_attempted")
    else:
        if mode not in _NATIVE_MODES | _ARROW_MODES:
            raise ValueError(f"record.configuration.mode: mode {mode!r} has no result-check contract")
        expected_execution_order = list(_EXECUTION_ORDER)
        checks.equal("execution", _field(freeze, "execution_order", "freeze"), expected_execution_order, "freeze.execution_order")
        reference_queries = _array(_field(manifest_case, "queries", f"manifest.cases[{source_id}]"), f"manifest.cases[{source_id}].queries")
        expected_queries = []
        for index in _EXECUTION_ORDER:
            if index >= len(reference_queries):
                raise ValueError(f"manifest.cases[{source_id}].queries: required query index {index} is missing")
            expected_queries.append(_object(reference_queries[index], f"manifest.cases[{source_id}].queries[{index}]"))
        checks.equal("execution", _integer(_field(receiver, "completed_executions", "record.receiver"), "record.receiver.completed_executions"), 4, "record.receiver.completed_executions")
        consumer = _object(_field(receiver, "consumer", "record.receiver"), "record.receiver.consumer")
        _finite_nonnegative(_field(consumer, "preparation_seconds", "record.receiver.consumer"), "record.receiver.consumer.preparation_seconds")
        checks.counts["clocks"] += 1
        checks.equal("execution", _integer(_field(consumer, "completed_executions", "record.receiver.consumer"), "record.receiver.consumer.completed_executions"), 4, "record.receiver.consumer.completed_executions")
        actual_queries = _array(_field(consumer, "queries", "record.receiver.consumer"), "record.receiver.consumer.queries")
        if len(actual_queries) != 4:
            raise ValueError(f"record.receiver.consumer.queries: expected 4 results in frozen order, found {len(actual_queries)}")
        for index, (actual, expected) in enumerate(zip(actual_queries, expected_queries)):
            _check_query_result(checks, actual, expected, f"record.receiver.consumer.queries[{index}]")

        send = _object(_field(transfer, "send", "record.sender.service.transfers[0]"), "record.sender.service.transfers[0].send")
        _finite_nonnegative(_field(send, "encoding_seconds", "record.sender.service.transfers[0].send"), "record.sender.service.transfers[0].send.encoding_seconds")
        checks.counts["clocks"] += 1
        for key in ("application_envelope_copy_bytes", "payload_bytes"):
            value = _integer(_field(send, key, "record.sender.service.transfers[0].send"), f"send.{key}")
            checks.require("transport", value >= 0, f"send.{key}", "counter must be nonnegative")
        receive_frames = _integer(_field(receive, "frames", "record.receiver.receive"), "record.receiver.receive.frames")
        send_frames = _integer(_field(send, "frames", "record.sender.service.transfers[0].send"), "record.sender.service.transfers[0].send.frames")
        checks.equal("transport", send_frames, receive_frames, "sender/receiver frame count")
        receive_wire = _integer(_field(receive, "application_wire_bytes", "record.receiver.receive"), "record.receiver.receive.application_wire_bytes")
        send_wire = _integer(_field(send, "application_wire_bytes", "record.sender.service.transfers[0].send"), "record.sender.service.transfers[0].send.application_wire_bytes")
        checks.equal("transport", send_wire, receive_wire, "sender/receiver application wire bytes")
        request_bytes = _integer(_field(transfer, "request_application_wire_bytes", "record.sender.service.transfers[0]"), "record.sender.service.transfers[0].request_application_wire_bytes")
        checks.equal("transport", _integer(_field(receive, "request_application_wire_bytes", "record.receiver.receive"), "record.receiver.receive.request_application_wire_bytes"), request_bytes, "sender/receiver request application wire bytes")
        checks.equal("transport", _field(receive, "representation", "record.receiver.receive"), mode, "record.receiver.receive.representation")

        assembly = _integer(_field(send, "complete_message_assembly_bytes", "record.sender.service.transfers[0].send"), "record.sender.service.transfers[0].send.complete_message_assembly_bytes")
        receiver_native_messages = _integer(_field(receive, "complete_native_messages", "record.receiver.receive"), "record.receiver.receive.complete_native_messages")
        receiver_native_assembly = _integer(_field(receive, "private_native_assembly_bytes", "record.receiver.receive"), "record.receiver.receive.private_native_assembly_bytes")
        if mode in _NATIVE_MODES:
            native_control = _object(_field(freeze_case, "ordinary_native_ipc", f"freeze.cases[{source_id}]"), f"freeze.cases[{source_id}].ordinary_native_ipc")
            native_messages = _array(_field(native_control, "messages", f"freeze.cases[{source_id}].ordinary_native_ipc"), f"freeze.cases[{source_id}].ordinary_native_ipc.messages")
            if len(native_messages) != 2:
                raise ValueError(f"freeze.cases[{source_id}].ordinary_native_ipc.messages: expected two frozen messages, found {len(native_messages)}")
            expected_native_assembly = sum(_integer(_field(_object(message, f"freeze.cases[{source_id}].ordinary_native_ipc.messages"), "bytes", f"freeze.cases[{source_id}].ordinary_native_ipc.messages"), "frozen native message bytes") for message in native_messages)
            expected_sender_assembly = expected_native_assembly if mode == "native_assembled" else 0
            checks.equal("execution", receiver_native_messages, 2, "record.receiver.receive.complete_native_messages")
            checks.equal("execution", assembly, expected_sender_assembly, "record.sender.service.transfers[0].send.complete_message_assembly_bytes")
            checks.equal("execution", receiver_native_assembly, expected_native_assembly, "record.receiver.receive.private_native_assembly_bytes")
        else:
            checks.equal("execution", receiver_native_messages, 0, "record.receiver.receive.complete_native_messages")
            checks.equal("execution", receiver_native_assembly, 0, "record.receiver.receive.private_native_assembly_bytes")
            checks.equal("execution", assembly, 0, "record.sender.service.transfers[0].send.complete_message_assembly_bytes")

    return {
        "total_checks": sum(checks.counts.values()),
        "check_counts": {name: checks.counts[name] for name in sorted(checks.counts)},
    }
