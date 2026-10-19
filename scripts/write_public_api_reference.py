#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Project public Python exports and executable CLI metadata into references.

Narrative remains in Markdown. This generator owns only marked reference blocks
and the application/command facts in the existing user-surface JSON inventory.
Use a freshly built CLI with --binary to update command facts. A source-only
--check verifies the recorded registry identity before using its metadata.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import inspect
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
INDEX = Path("docs/reference/shardloom-user-surface-index.json")
REFERENCE = INDEX.with_suffix(".md")
STATUS = Path("docs/status/cli-command-registry.md")
REGISTRY = Path("shardloom-cli/src/command_registry.rs")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def registry_names(root: Path, constant: str) -> list[str]:
    source = (root / REGISTRY).read_text(encoding="utf-8")
    match = re.search(rf"\b{constant}:\s*&\[&str\]\s*=\s*&\[(.*?)\];", source, re.S)
    if match is None:
        raise ValueError(f"missing command authority: {constant}")
    names = re.findall(r'"([^"]+)"', match[1])
    if len(names) != len(set(names)):
        raise ValueError(f"duplicate command in {constant}")
    return names


def command_fields(binary: Path, *args: str) -> dict[str, str]:
    completed = subprocess.run(
        [str(binary.resolve()), "command-metadata", *args, "--format", "json"],
        capture_output=True, text=True, check=True, timeout=30,
    )
    envelope = json.loads(completed.stdout)
    if envelope.get("status") != "success" or envelope.get("fallback", {}).get("attempted") is not False:
        raise ValueError("CLI metadata must succeed without fallback")
    entries = envelope["fields"]
    fields = {entry["key"]: str(entry["value"]) for entry in entries}
    if len(fields) != len(entries):
        raise ValueError("CLI metadata contains duplicate fields")
    return fields


def command_reference(root: Path, binary: Path | None, previous: dict) -> dict:
    names = registry_names(root, "REGISTERED_COMMANDS")
    application = registry_names(root, "APPLICATION_COMMANDS")
    if not set(application) <= set(names):
        raise ValueError("application registry names must be registered")
    identity = digest(root / REGISTRY)
    if binary is None:
        result = previous.get("command_registry", {})
        if result.get("source_sha256") != identity:
            raise ValueError("command metadata is stale; regenerate with a freshly built --binary")
        expected_audiences = {name: "application" if name in application else "maintainer" for name in names}
        if (result.get("registered_commands") != names or
                result.get("application_commands") != application or
                result.get("audiences") != expected_audiences):
            raise ValueError("recorded command names/audiences disagree with the registry")
        rows = result.get("application_metadata", [])
        if ([row.get("name") for row in rows] != application or
                any(row.get("audience") != "application" for row in rows)):
            raise ValueError("recorded application metadata has missing, duplicate or misclassified commands")
        return result
    full = command_fields(binary)
    public = command_fields(binary, "--audience", "application")
    if full.get("registered_commands", "").split(",") != names:
        raise ValueError("executable command registry differs from this source")
    # The executable emits registry order; the allowlist declares membership.
    expected_application = [name for name in names if name in application]
    if public.get("registered_commands", "").split(",") != expected_application:
        raise ValueError("executable application audience differs from this source")
    if int(full["registered_command_count"]) != len(names) or int(public["registered_command_count"]) != len(application):
        raise ValueError("executable counts disagree with its names")
    audience_map = dict(item.split("=", 1) for item in full["command_registry_audience_map"].split(","))
    expected_audiences = {name: "application" if name in application else "maintainer" for name in names}
    if audience_map != expected_audiences:
        raise ValueError("executable audience map differs from this source")
    rows = []
    for name in application:
        fields = command_fields(binary, name, "--audience", "application")
        row = {key: fields[f"selected_command_{key}"] for key in (
            "audience", "family", "usage_fragment", "side_effect_level", "input_contract", "output_contract",
        )}
        rows.append({"name": name, **row})
    return {
        "source": REGISTRY.as_posix(), "source_sha256": identity,
        "metadata_command": "shardloom command-metadata --format json",
        "registered_commands": names, "application_commands": application,
        "audiences": audience_map, "application_metadata": rows,
    }


def python_reference(root: Path) -> tuple[list[dict], dict[str, str]]:
    sys.path.insert(0, str(root / "python" / "src"))
    from shardloom._exports import EXPORTS, resolve_export
    rows, sources = [], {}
    for name, (module_name, audience) in sorted(EXPORTS.items()):
        module = importlib.import_module(f"shardloom.{module_name}")
        source = Path(module.__file__).resolve()
        relative = source.relative_to(root).as_posix()
        sources[relative] = digest(source)
        value = resolve_export(name)
        signature = None
        if callable(value):
            try:
                signature = str(inspect.signature(value))
            except (TypeError, ValueError):
                pass  # Built-in exception constructors need not have a signature.
        kind = "class" if inspect.isclass(value) else "function" if callable(value) else "constant"
        namespace = "shardloom" if audience == "application" else f"shardloom.{audience}"
        rows.append({
            "name": name, "namespace": namespace, "audience": audience, "kind": kind,
            "source": relative, "signature": signature,
            "summary": (inspect.getdoc(value) or "").split("\n")[0] if callable(value) else "Package constant.",
        })
    authority = "python/src/shardloom/_exports.py"
    sources[authority] = digest(root / authority)
    return rows, dict(sorted(sources.items()))


def marked(text: str, name: str, content: str) -> str:
    opening, closing = f"<!-- BEGIN GENERATED {name} -->", f"<!-- END GENERATED {name} -->"
    if text.count(opening) != 1 or text.count(closing) != 1:
        raise ValueError(f"expected exactly one {name} block")
    start, tail = text.split(opening)
    _, end = tail.split(closing)
    return start + opening + "\n" + content.rstrip() + "\n" + closing + end


def cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def render_reference(application: dict) -> str:
    commands = application["command_registry"]
    lines = [
        f"Development source identity: `{application['source_identity']}` (SHA-256 of the listed source inputs).",
        "This API is unpublished; the package version is held independently of source changes.",
        "", "### Python application exports", "",
        "Names and signatures below come from the actual lazy export authority.",
        "Method contracts are available on the linked classes; normal use starts with `context`.",
        "", "| Export | Kind | Contract |", "| --- | --- | --- |",
    ]
    for row in application["python_symbols"]:
        if row["audience"] != "application":
            continue
        lines.append(f"| [`{row['name']}`](../../{row['source']}) | {row['kind']} | {cell(row['summary'])} |")
    lines.extend([
        "", "Exact callable signatures are generated in the [JSON reference](shardloom-user-surface-index.json)",
        "and package type stubs. `shardloom.diagnostics` contains runtime inspection and compatibility",
        "types; `shardloom.testing` contains maintainer fixtures and rehearsal reports.",
        "", "### CLI application commands", "",
        f"The executable registry contains {len(commands['registered_commands'])} commands:",
        f"{len(commands['application_commands'])} application commands and "
        f"{len(commands['registered_commands']) - len(commands['application_commands'])} maintainer commands.",
        "These counts describe discovery, not the number of implemented SQL operations.",
        "", "| Command | Input contract | Output contract |", "| --- | --- | --- |",
    ])
    for row in commands["application_metadata"]:
        lines.append(f"| `{row['name']}` | {cell(row['input_contract'])} | {cell(row['output_contract'])} |")
    lines.extend([
        "", "```bash", "shardloom help", "shardloom command-metadata --audience application --format json",
        "shardloom command-metadata --audience maintainer --format json",
        "shardloom command-metadata --format json", "shardloom command-metadata run --format json",
        "shardloom command-metadata --completion bash", "```", "",
        "Completion also supports `zsh` and `fish`; its default vocabulary is application commands.",
        "Existing JSON envelopes and the unfiltered metadata inventory remain available.",
    ])
    return "\n".join(lines)


def generated_files(root: Path, binary: Path | None = None) -> dict[Path, str]:
    payload = json.loads((root / INDEX).read_text(encoding="utf-8"))
    symbols, inputs = python_reference(root)
    commands = command_reference(root, binary, payload.get("application", {}))
    inputs[REGISTRY.as_posix()] = commands["source_sha256"]
    identity = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()
    application = {
        "schema_version": "shardloom.application_reference.v1", "availability": "unpublished_source",
        "generator": "scripts/write_public_api_reference.py", "source_identity": identity,
        "source_inputs": dict(sorted(inputs.items())),
        "python_symbols": symbols, "command_registry": commands,
    }
    payload["application"] = application
    payload["cli"]["registered_command_count"] = len(commands["registered_commands"])
    payload["cli"]["application_command_count"] = len(commands["application_commands"])
    payload["cli"]["application_inventory_command"] = "shardloom command-metadata --audience application --format json"
    payload["cli"]["maintainer_inventory_command"] = "shardloom command-metadata --audience maintainer --format json"
    reference = marked((root / REFERENCE).read_text(encoding="utf-8"), "APPLICATION REFERENCE", render_reference(application))
    status_block = (
        f"Registered command count: {len(commands['registered_commands'])}\n\n"
        f"Application command count: {len(commands['application_commands'])}\n\n"
        f"Maintainer command count: {len(commands['registered_commands']) - len(commands['application_commands'])}\n\n"
        f"Registry source SHA-256: `{commands['source_sha256']}`"
    )
    status = marked((root / STATUS).read_text(encoding="utf-8"), "COMMAND COUNTS", status_block)
    return {INDEX: json.dumps(payload, indent=2, ensure_ascii=False) + "\n", REFERENCE: reference, STATUS: status}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = args.repo_root.resolve()
    files = generated_files(root, args.binary)
    stale = [str(path) for path, text in files.items() if (root / path).read_text(encoding="utf-8") != text]
    if args.check:
        if stale:
            print("Stale generated application references: " + ", ".join(stale))
            return 1
        print("Application exports, command metadata, signatures and counts are current.")
    else:
        for path, text in files.items():
            (root / path).write_text(text, encoding="utf-8")
        print("Updated application reference and registry counts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
