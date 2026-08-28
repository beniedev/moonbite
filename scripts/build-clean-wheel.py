#!/usr/bin/env python3
"""Build a Moonbite wheel from an allowlisted, fresh source staging tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile


BUILD_INPUTS = ("pyproject.toml", "MANIFEST.in", "README.md", "LICENSE")
IGNORED_NAMES = {"__pycache__", ".pytest_cache", ".ruff_cache"}


class CleanBuildError(RuntimeError):
    """A wheel cannot be proven to match a fresh source staging tree."""


def _copy_file(source: Path, destination: Path) -> None:
    if not source.is_file() or source.is_symlink():
        raise CleanBuildError(
            f"required build input is unsafe or missing: {source.name}"
        )
    shutil.copy2(source, destination)


def _copy_package(source: Path, destination: Path) -> None:
    if not source.is_dir() or source.is_symlink():
        raise CleanBuildError("moonbite_plugin source directory is unsafe or missing")
    destination.mkdir()
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if any(part in IGNORED_NAMES for part in relative.parts):
            continue
        if path.is_symlink():
            raise CleanBuildError(f"package source contains a symlink: {relative}")
        if path.is_dir():
            (destination / relative).mkdir(exist_ok=True)
        elif path.is_file() and path.suffix != ".pyc":
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)


def _package_files(root: Path) -> list[str]:
    return sorted(
        path.relative_to(root).as_posix()
        for path in (root / "moonbite_plugin").rglob("*")
        if path.is_file()
    )


def _wheel_package_files(wheel: Path) -> list[str]:
    with zipfile.ZipFile(wheel) as archive:
        return sorted(
            name
            for name in archive.namelist()
            if name.startswith("moonbite_plugin/") and not name.endswith("/")
        )


def build_clean_wheel(repo: Path, output_root: Path, uv: str) -> dict[str, object]:
    repo = repo.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="moonbite-clean-build-") as temporary:
        stage = Path(temporary)
        for name in BUILD_INPUTS:
            _copy_file(repo / name, stage / name)
        _copy_package(repo / "moonbite_plugin", stage / "moonbite_plugin")

        stage_dist = stage / "dist"
        completed = subprocess.run(
            [uv, "build", "--wheel", "--out-dir", str(stage_dist)],
            cwd=stage,
            check=False,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "PYTHONHASHSEED": "0",
                "SOURCE_DATE_EPOCH": "315532800",
            },
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout)[-4000:].strip()
            raise CleanBuildError(f"clean wheel build failed: {detail}")
        wheels = sorted(stage_dist.glob("*.whl"))
        if len(wheels) != 1:
            raise CleanBuildError("clean build must produce exactly one wheel")
        wheel = wheels[0]
        source_files = _package_files(stage)
        wheel_files = _wheel_package_files(wheel)
        if source_files != wheel_files:
            source_only = sorted(set(source_files) - set(wheel_files))
            wheel_only = sorted(set(wheel_files) - set(source_files))
            raise CleanBuildError(
                "wheel package does not exactly match source: "
                f"source_only={source_only!r}, wheel_only={wheel_only!r}"
            )

        payload = wheel.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        destination_dir = output_root.resolve() / digest[:16]
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / wheel.name
        try:
            with destination.open("xb") as handle:
                handle.write(payload)
        except FileExistsError:
            if destination.read_bytes() != payload:
                raise CleanBuildError("existing digest destination has different bytes")
        return {
            "wheel": str(destination),
            "sha256": digest,
            "package_files": len(source_files),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--output-dir", type=Path, default=Path("dist/clean"))
    parser.add_argument("--uv", default=shutil.which("uv"))
    args = parser.parse_args()
    if not args.uv:
        raise CleanBuildError("uv executable is required")
    result = build_clean_wheel(args.repo, args.output_dir, args.uv)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
