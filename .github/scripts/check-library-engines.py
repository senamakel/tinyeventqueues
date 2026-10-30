#!/usr/bin/env python3
"""Keep storage implementations out of TinyEventQueues library dependency trees."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
CARGO_TIMEOUT_S = 300
LIBRARY_KINDS = {
    "lib",
    "rlib",
    "dylib",
    "cdylib",
    "staticlib",
    "proc-macro",
}
ALLOWED_STORAGE_PACKAGES = {
    "tinystoragedrivers-api",
    "tinystoragedrivers-types",
    # The busproxy uses this serialized contract; it contains no backend.
    "tinystoragedrivers-bus",
    "tinystoragedrivers-busproxy",
}
STORAGE_IMPLEMENTATION = re.compile(r"^tinystoragedrivers(?:-|$)")
OTHER_ENGINES = {
    "rusqlite",
    "libsqlite3-sys",
    "sqlx",
    "mongodb",
    "bson",
    "aws-sdk-s3",
    "lapin",
    "keyring",
    "sled",
    "redb",
    "rocksdb",
}
FEATURE_MODES = (
    ("default", []),
    ("no-default", ["--no-default-features"]),
    ("all", ["--all-features"]),
)


def tree_names(output: str) -> list[str]:
    """Extract package names from cargo tree's formatted output."""
    names: list[str] = []
    for raw_line in output.splitlines():
        line = raw_line.strip().lstrip("│├└─ ").strip()
        if not line:
            continue
        name = line.split()[0]
        if name:
            names.append(name)
    return names


def forbidden(names: list[str]) -> list[str]:
    """Return storage engines while allowing only the public port contracts."""
    return sorted(
        {
            name
            for name in names
            if (
                STORAGE_IMPLEMENTATION.match(name)
                and name not in ALLOWED_STORAGE_PACKAGES
            )
            or name in OTHER_ENGINES
        }
    )


def library_packages(metadata: dict) -> dict[str, dict]:
    """Select library targets by workspace member ID, failing closed on bad metadata."""
    packages_by_id = {package["id"]: package for package in metadata.get("packages", [])}
    members = metadata.get("workspace_members", [])
    if not members:
        raise RuntimeError("cargo metadata reported no workspace members")

    packages: dict[str, dict] = {}
    for member_id in members:
        package = packages_by_id.get(member_id)
        if package is None:
            raise RuntimeError(f"workspace member ID was not returned: {member_id}")
        if any(set(target["kind"]) & LIBRARY_KINDS for target in package["targets"]):
            packages[package["name"]] = package
    if not packages:
        raise RuntimeError("no workspace library targets found; refusing empty pass")
    return packages


def self_test() -> None:
    parsed = tree_names(
        "tinyeventqueues v0.1.2\r\n"
        "├── tinystoragedrivers-api v2.0.2\r\n"
        "└── tinystoragedrivers-types v2.0.2\r\n"
    )
    assert parsed == [
        "tinyeventqueues",
        "tinystoragedrivers-api",
        "tinystoragedrivers-types",
    ], parsed
    assert forbidden(parsed) == []
    assert forbidden(["tinyeventqueues", "tinystoragedrivers-document"]) == [
        "tinystoragedrivers-document"
    ]
    assert forbidden(["tinyeventqueues", "rusqlite"]) == ["rusqlite"]
    for engine in OTHER_ENGINES:
        assert forbidden(["tinyeventqueues", engine]) == [engine], engine
    assert forbidden(["tinyeventqueues", "rusqlite-helper", "not-mongodb"]) == []
    assert tree_names("\r\n") == []
    if not tree_names("tinyeventqueues v0.1.2\r\n"):
        raise AssertionError("tree parser accepted no root package")

    synthetic = {
        "packages": [
            {
                "id": f"path+tinyeventqueues#{kind}",
                "name": f"tinyeventqueues-{kind}",
                "targets": [{"kind": [kind]}],
            }
            for kind in sorted(LIBRARY_KINDS)
        ]
        + [
            {"id": "path+tinyeventqueues#bin", "name": "tinyeventqueues-cli", "targets": [{"kind": ["bin"]}]},
            {"id": "registry+outside#lib", "name": "unrelated", "targets": [{"kind": ["lib"]}]},
        ],
        "workspace_members": [f"path+tinyeventqueues#{kind}" for kind in sorted(LIBRARY_KINDS)]
        + ["path+tinyeventqueues#bin"],
    }
    assert set(library_packages(synthetic)) == {
        *(f"tinyeventqueues-{kind}" for kind in LIBRARY_KINDS),
    }
    for broken_metadata in (
        {"packages": [], "workspace_members": []},
        {"packages": [], "workspace_members": ["missing"]},
        {
            "packages": [{"id": "bin", "name": "tinyeventqueues-cli", "targets": [{"kind": ["bin"]}]}],
            "workspace_members": ["bin"],
        },
    ):
        try:
            library_packages(broken_metadata)
        except RuntimeError:
            pass
        else:
            raise AssertionError("library target discovery accepted empty/bad metadata")

    for malformed in ("", "not json", "[]"):
        try:
            parsed_metadata = json.loads(malformed)
            library_packages(parsed_metadata)
        except (json.JSONDecodeError, RuntimeError, AttributeError, TypeError):
            pass
        else:
            raise AssertionError("malformed cargo metadata was accepted")
    class FakeResult:
        def __init__(self, returncode: int, stdout: str = "", stderr: str = ""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    try:
        cargo_json(
            ["metadata"],
            lambda *args, **kwargs: FakeResult(2, stderr="metadata failed"),
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("failed cargo metadata command was accepted")
    try:
        cargo_tree_names(
            "tinyeventqueues",
            ["tree"],
            lambda *args, **kwargs: FakeResult(1, stderr="tree failed"),
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("failed cargo tree command was accepted")
    try:
        cargo_tree_names(
            "tinyeventqueues",
            ["tree"],
            lambda *args, **kwargs: FakeResult(0, "other-package v0.1.0\n"),
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("dependency tree without requested root was accepted")

    def timeout_runner(*args, **kwargs):
        if kwargs.get("timeout") != CARGO_TIMEOUT_S:
            raise AssertionError("cargo subprocess did not receive the timeout")
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    for run in (
        lambda: cargo_json(["metadata"], timeout_runner),
        lambda: cargo_tree_names("tinyeventqueues", ["tree"], timeout_runner),
    ):
        try:
            run()
        except RuntimeError as error:
            if "timed out" not in str(error):
                raise AssertionError(f"timeout error lost its context: {error}")
        else:
            raise AssertionError("timed out cargo subprocess was accepted")


def run_cargo(args: list[str], runner: Callable = subprocess.run) -> str:
    command = ["cargo", *args]
    try:
        result = runner(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=CARGO_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(f"cargo {' '.join(args)} timed out") from error
    if result.returncode:
        raise RuntimeError(
            f"cargo {' '.join(args)} failed ({result.returncode}):\n{result.stderr}"
        )
    return result.stdout


def cargo_json(args: list[str], runner: Callable = subprocess.run) -> dict:
    return json.loads(run_cargo(args, runner))


def validate_tree(package_name: str, output: str) -> list[str]:
    """Require the queried package to be present before accepting its graph."""
    names = tree_names(output)
    if package_name not in names:
        raise RuntimeError(f"{package_name}: tree omitted its root package")
    return names


def cargo_tree_names(
    package_name: str, args: list[str], runner: Callable = subprocess.run
) -> list[str]:
    return validate_tree(package_name, run_cargo(args, runner))


def main() -> int:
    self_test()
    if sys.argv[1:] == ["--self-test"]:
        print("library engine gate matcher self-tests passed")
        return 0
    if sys.argv[1:]:
        print("usage: check-library-engines.py [--self-test]", file=sys.stderr)
        return 2

    metadata = cargo_json(["metadata", "--locked", "--format-version", "1", "--no-deps"])
    packages = library_packages(metadata)

    failures: list[str] = []
    for package_name in sorted(packages):
        for feature_mode, feature_args in FEATURE_MODES:
            command = [
                "tree",
                "--locked",
                "--format",
                "{p}",
                "--prefix",
                "none",
                "-e",
                "normal,build",
                "-p",
                package_name,
                *feature_args,
            ]
            try:
                names = cargo_tree_names(package_name, command)
            except RuntimeError:
                failures.append(
                    f"{package_name} ({feature_mode}): cargo tree failed or omitted its root"
                )
                continue
            blocked = forbidden(names)
            if blocked:
                failures.append(
                    f"{package_name} ({feature_mode}): forbidden storage engines: "
                    + ", ".join(blocked)
                )
            print(f"{package_name} ({feature_mode}): {len(names)} packages, clean")

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"checked {len(packages)} workspace libraries in default, no-default, and all features")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, json.JSONDecodeError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
