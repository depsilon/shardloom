"""Generate or verify the Python package's complete locked dependency notices.

The bundle conservatively includes every registry package in Cargo.lock and local
vendored patches, including build, test and platform-specific dependencies. It is not a linked-component SBOM
or a license-policy decision. Generation consumes independently reviewed local
source evidence and never downloads files or executes dependency code.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 build tooling, not a runtime dependency.
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[1]
BUNDLE = Path("python/THIRD_PARTY_LICENSES")
SCHEMA = "shardloom.python_dependency_notices.v1"


def locked_packages(root: Path) -> dict[tuple[str, str, str], str]:
    lock = tomllib.loads((root / "Cargo.lock").read_text())
    return {(item["name"], item["version"], item["source"]): item["checksum"]
            for item in lock["package"] if item.get("source", "").startswith("registry+")}


def identity(package: dict) -> tuple[str, str, str]:
    return package["name"], package["version"], package["source"]


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def vendored_components(root: Path) -> list[dict]:
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    components = []
    for patches in manifest.get("patch", {}).values():
        for specification in patches.values():
            if not isinstance(specification, dict) or "path" not in specification:
                continue
            relative = PurePosixPath(specification["path"])
            if relative.is_absolute() or ".." in relative.parts or relative.parts[0] != "vendor":
                raise ValueError("local third-party patches require reviewed vendor provenance")
            directory = root / str(relative)
            package = tomllib.loads((directory / "Cargo.toml").read_text())["package"]
            provenance = json.loads((directory / "upstream-provenance.json").read_text())
            data = (directory / "LICENSE").read_bytes()
            if (provenance["provider"], provenance["version"], provenance["license"]) != (
                    package["name"], package["version"], package["license"]):
                raise ValueError("vendored provider identity differs from its provenance")
            sha = digest(data)
            if provenance["license_sha256"] != sha:
                raise ValueError("vendored license differs from its reviewed provenance")
            components.append({"name": package["name"], "version": package["version"],
                               "path": str(relative), "license": package["license"],
                               "provenance": provenance, "files": [{
                                   "text": f"texts/{sha}.txt", "sha256": sha,
                                   "bytes": len(data), "crate_path": "LICENSE",
                                   "upstream_url": provenance["license_url"],
                               }]})
    return sorted(components, key=lambda item: (item["name"], item["version"]))


def generate(root: Path, inventory: Path, *, replace: bool = False) -> None:
    evidence = json.loads(inventory.read_text())
    packages = evidence["packages"]
    expected = locked_packages(root)
    if len(packages) != len(expected) or {identity(p) for p in packages} != set(expected):
        raise ValueError("license inventory does not exactly cover the registry lockfile")
    components, texts = [], {}
    for package in sorted(packages, key=identity):
        if not package.get("license") or not package.get("files"):
            raise ValueError(f"missing license declaration or text for {identity(package)}")
        files = []
        for source in package["files"]:
            path = Path(source["source_path"])
            data = path.read_bytes()
            sha = digest(data)
            if sha != source["sha256"] or len(data) != source["bytes"]:
                raise ValueError(f"license evidence changed: {path}")
            data.decode("utf-8")
            origin = source["evidence_source"]
            if origin == "registry_cache":
                relative = PurePosixPath(source["package_relative_path"])
                if relative.is_absolute() or ".." in relative.parts:
                    raise ValueError("crate license path must remain inside its archive")
                declared = Path(package["manifest_path"]).parent / str(relative)
                if path.resolve() != declared.resolve():
                    raise ValueError("cached license is not from its declared package")
                provenance = {"crate_path": str(relative)}
            else:
                url = source.get("upstream_url")
                if not isinstance(url, str) or not url.startswith("https://"):
                    raise ValueError("supplemental notice needs its reviewed upstream URL")
                provenance = {"upstream_url": url}
            target = f"texts/{sha}.txt"
            texts[target] = data
            files.append({"text": target, "sha256": sha, "bytes": len(data), **provenance})
        components.append({key: package[key] for key in ("name", "version", "source", "license", "repository")} | {
            "crate_checksum_sha256": expected[identity(package)],
            "files": sorted(files, key=lambda item: (item["text"], json.dumps(item, sort_keys=True))),
        })
    vendored = vendored_components(root)
    for component in vendored:
        texts[component["files"][0]["text"]] = (root / component["path"] / "LICENSE").read_bytes()
    bundle = root / BUNDLE
    if (bundle / "manifest.json").exists() and not replace:
        raise ValueError("notice bundle already exists; review replacement explicitly")
    (bundle / "texts").mkdir(parents=True, exist_ok=True)
    for name, data in texts.items():
        (bundle / name).write_bytes(data)
    manifest = {"schema_version": SCHEMA,
                "scope": "all_locked_registry_packages_and_vendored_patches_including_build_test_and_platform_dependencies",
                "components": components, "vendored_components": vendored}
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")


def verify(root: Path) -> dict:
    bundle = root / BUNDLE
    manifest = json.loads((bundle / "manifest.json").read_text())
    if manifest["schema_version"] != SCHEMA:
        raise ValueError("unsupported notice bundle schema")
    expected = locked_packages(root)
    components = manifest["components"]
    if len(components) != len(expected) or {identity(p) for p in components} != set(expected):
        raise ValueError("notice bundle does not exactly cover the registry lockfile")
    vendored = vendored_components(root)
    if manifest.get("vendored_components") != vendored:
        raise ValueError("notice bundle does not match reviewed vendored dependencies")
    texts, records = set(), 0
    for component in components:
        if component["crate_checksum_sha256"] != expected[identity(component)]:
            raise ValueError("notice component archive identity changed")
        if not component.get("license") or not component.get("files"):
            raise ValueError("notice component has no declared license or text")
    for component in components + vendored:
        for item in component["files"]:
            sha = item["sha256"]
            if len(sha) != 64 or any(char not in "0123456789abcdef" for char in sha):
                raise ValueError("notice text has an invalid digest")
            name = f"texts/{sha}.txt"
            if item["text"] != name or not (item.get("crate_path") or item.get("upstream_url")):
                raise ValueError("notice text path or provenance is missing")
            path = bundle / name
            if path.is_symlink() or not path.is_file():
                raise ValueError("notice text must be a regular bundled file")
            data = path.read_bytes()
            if len(data) != item["bytes"] or digest(data) != sha:
                raise ValueError(f"notice text identity mismatch: {name}")
            texts.add(name)
            records += 1
    actual = {str(path.relative_to(bundle)) for path in (bundle / "texts").iterdir()}
    if actual != texts:
        raise ValueError("notice directory has missing or unreferenced texts")
    return {"status": "passed", "registry_packages": len(components),
            "vendored_packages": len(vendored),
            "license_file_records": records, "unique_text_files": len(texts)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--generate-from-inventory", type=Path)
    parser.add_argument("--replace-reviewed-bundle", action="store_true")
    args = parser.parse_args()
    if args.generate_from_inventory is not None:
        generate(args.repo_root, args.generate_from_inventory, replace=args.replace_reviewed_bundle)
    print(json.dumps(verify(args.repo_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
