#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Deliberate peer deaths and restart checks, excluded from transport scoring."""

from __future__ import annotations

import hashlib
import json
import os
import signal


def require(condition: bool, detail: str) -> None:
    if not condition:
        raise ValueError(detail)


def config_bytes(config: dict) -> bytes:
    return json.dumps(config, sort_keys=True, separators=(",", ":")).encode()


def event(pair, peer, name: str) -> dict:
    pair.until(lambda: peer.event(name) is not None or
               (peer.process.returncode is not None and peer.open_pipes == 0))
    value = peer.event(name)
    require(value is not None, f"{peer.role} exited without {name}")
    return value


def exited(pair, peer, expected: int) -> None:
    pair.until(lambda: peer.process.returncode is not None and peer.open_pipes == 0)
    require(peer.process.returncode == expected, f"unexpected {peer.role} exit")


def kill(pair, peer) -> None:
    require(peer.process.returncode is None, "peer exited before deliberate fault")
    pair.allowed_exit_codes[peer.process.pid] = {-signal.SIGKILL}
    os.kill(peer.process.pid, signal.SIGKILL)
    exited(pair, peer, -signal.SIGKILL)


def final(peer, config: dict) -> dict:
    value = peer.event("final")
    require(value is not None, f"surviving {peer.role} has no final evidence")
    report = value["report"]
    require(report["pid"] == peer.process.pid, "peer changed PID")
    require(report["configuration_sha256"] == hashlib.sha256(config_bytes(config)).hexdigest(),
            "surviving peer changed its configuration")
    require(report["final_reserved_bytes"] == 0, "surviving peer retained native memory")
    require(report["fallback_attempted"] is False and report["external_engine_invoked"] is False,
            "failure path attempted fallback")
    return report


def probe(pair_type, peer_type, guard, executable, sender_config: dict,
          receiver_config: dict, kind: str) -> dict:
    pair = pair_type(guard, executable, sender_config)
    record = {"status": "running", "failure_kind": kind, "configuration": sender_config,
              "receiver_configuration": receiver_config, "scored": False}
    try:
        guard.check([], force=True)
        sender = peer_type(executable, "sender", pair.selected)
        pair.peers.append(sender)
        sender.send_bytes(config_bytes(sender_config))
        role = "receiver-stale" if kind == "restart_rejects_old_epoch" else "receiver-hold"
        receiver = peer_type(executable, role, pair.selected)
        pair.peers.append(receiver)
        receiver.send_bytes(config_bytes(receiver_config))
        record.update(sender_pid=sender.process.pid, receiver_pid=receiver.process.pid)
        require(sender.process.pid != receiver.process.pid, "peers share a process")
        ready = event(pair, sender, "ready")
        require(ready["pid"] == sender.process.pid and
                ready["configuration_sha256"] == hashlib.sha256(config_bytes(sender_config)).hexdigest(),
                "sender readiness identity differs")
        receiver.send({"command": "connect", "address": ready["address"], "sender_pid": sender.process.pid})
        record["sender_ready"] = ready
        if kind == "restart_rejects_old_epoch":
            require(sender_config["server_epoch"] != receiver_config["server_epoch"], "restart reused epoch")
            exited(pair, receiver, 0)
            report = final(receiver, receiver_config)
            require(report["expected_refusal"] == "SLX_IDENTITY" and report["published_sources"] == 0
                    and report["completed_executions"] == 0, "old-epoch request was not refused privately")
            record["receiver"] = report
            sender.send({"command": "stop"})
            exited(pair, sender, 0)
            record["sender"] = final(sender, sender_config)
            require(record["sender"]["service"]["transfers"] == [], "stale epoch produced a response payload")
        else:
            partial = event(pair, receiver, "partial")
            require(partial["pid"] == receiver.process.pid and partial["sender_pid"] == sender.process.pid
                    and partial["completed_executions"] == 0 and partial["published_sources"] == 0
                    and partial["configuration_sha256"] == hashlib.sha256(config_bytes(receiver_config)).hexdigest(),
                    "partial-transfer barrier is inconsistent")
            record["partial_transfer_barrier"] = partial
            if kind == "sender_death":
                kill(pair, sender)
                receiver.send({"command": "continue"})
                exited(pair, receiver, 0)
                report = final(receiver, receiver_config)
                require(report["published_sources"] == 0 and report["completed_executions"] == 0
                        and report["expected_refusal"] == "SLX_INVALID_WIRE", "sender death exposed a partial source")
                record["receiver"] = report
            elif kind == "receiver_death":
                kill(pair, receiver)
                sender.send({"command": "stop"})
                exited(pair, sender, 0)
                report = final(sender, sender_config)
                transfers = report["service"]["transfers"]
                require(len(transfers) == 1 and transfers[0]["success"] is False and
                        transfers[0]["error"]["code"] == "SLX_CANCELLED", "receiver death did not cancel active data work")
                record["sender"] = report
            else:
                raise ValueError("unknown process fault")
        if "sender" in record:
            report = record["sender"]
            require(report["final_live_connections"] == 0 and report["service"]["active_attempts"] == 0
                    and report["service"]["queued_bytes"] == 0 and report["service"]["queued_frames"] == 0,
                    "surviving sender retained tasks, connections or payload owners")
        resources = [peer.resource for peer in pair.peers]
        require(all(value is not None for value in resources), "peer resource receipt missing")
        require(0 < pair.peak_rss <= 2 << 30 and sum(value["peak_rss_bytes"] for value in resources) <= 2 << 30,
                "process-fault RSS limit exceeded")
        record.update(status="passed", resources={"peers": resources,
            "peak_combined_sampled_rss_bytes": pair.peak_rss, "rss_samples": pair.rss_samples},
            forced_exit_memory_scope="OS-reaped peer; no zero-credit observation is claimed for the killed process")
    except BaseException as error:
        record.update(status="failed", error=f"{type(error).__name__}: {error}")
    finally:
        try:
            pair.shutdown()
            record["owned_processes_drained"] = True
        except BaseException as error:
            record.update(status="failed", owned_processes_drained=False, cleanup_error=f"{type(error).__name__}: {error}")
        record["peers"] = [{"role": peer.role, "pid": peer.process.pid, "exit_code": peer.process.returncode,
                            "resources": peer.resource, "events": peer.events,
                            "stderr": peer.stderr.decode(errors="replace")} for peer in pair.peers]
        for peer in pair.peers:
            peer.close()
        pair.selected.close()
    return record


def run_failures(pair_type, peer_type, guard, executable, base: dict, manifest: dict, freeze: dict):
    def config():
        return {**base, "source_id": "wide_text", "mode": "native_fragments",
                "server_epoch": os.urandom(16).hex(), "attempt_id": os.urandom(16).hex()}

    old = config()
    death = probe(pair_type, peer_type, guard, executable, old, old, "sender_death")
    yield death
    require(death["status"] == "passed", "sender-death check failed")
    receiver_death = config()
    yield probe(pair_type, peer_type, guard, executable, receiver_death, receiver_death, "receiver_death")
    restarted = config()
    record = probe(pair_type, peer_type, guard, executable, restarted, old, "restart_rejects_old_epoch")
    record["previous_sender_pid"] = death.get("sender_pid")
    require(record.get("sender_pid") != death.get("sender_pid"), "restart did not establish a distinct process")
    yield record
    fresh = pair_type(guard, executable, config()).run(manifest, freeze)
    fresh.update(failure_kind="full_fresh_transfer_after_restart", scored=False)
    yield fresh
