#!/usr/bin/env python3
"""
Simple version bump script.

Assumptions (per project layout):
  - One `pyproject.toml` at repo root
  - The version lives under the `[project]` section as `version = "..."`

Usage:
  python scripts/bump_versions.py 1.2.3 [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def get_pyproject_paths(root: Path) -> list[Path]:
    paths: list[Path] = []
    root_py = root / "pyproject.toml"
    if root_py.exists():
        paths.append(root_py)
    return paths


def bump_in_text(text: str, new_version: str) -> tuple[str, bool]:
    """Update `version` inside the `[project]` section. Returns (new_text, changed)."""
    # Locate the [project] section block
    section_re = re.compile(r"^\[project\]\s*$", re.MULTILINE)
    match = section_re.search(text)
    if not match:
        return text, False

    start = match.end()
    next_section = re.search(r"^\[.*?\]\s*$", text[start:], flags=re.MULTILINE)
    end = start + next_section.start() if next_section else len(text)
    block = text[start:end]

    # Replace version line within the block, preserving quote style
    version_line_re = re.compile(r"^(\s*version\s*=\s*)([\"\'])(.+?)(\2)\s*$", re.MULTILINE)
    if not version_line_re.search(block):
        return text, False

    new_block = version_line_re.sub(lambda m: f"{m.group(1)}{m.group(2)}{new_version}{m.group(2)}", block)
    if new_block == block:
        return text, False

    new_text = text[:start] + new_block + text[end:]
    return new_text, True


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Bump version in pyproject.toml")
    parser.add_argument("version", help="New version string, e.g. 1.2.3")
    parser.add_argument("--dry-run", action="store_true", help="Only print changes, do not write")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    targets = get_pyproject_paths(repo_root)
    if not targets:
        print("No pyproject.toml files found.")
        return 1

    exit_code = 0
    for path in targets:
        try:
            original = path.read_text(encoding="utf-8")
        except Exception as e:
            print(f"Skipping {path}: {e}")
            exit_code = 1
            continue

        updated, changed = bump_in_text(original, args.version)
        if not changed:
            continue

        rel = path.relative_to(repo_root)
        if args.dry_run:
            print(f"Would update {rel} -> {args.version}")
        else:
            path.write_text(updated, encoding="utf-8")
            print(f"Updated {rel} -> {args.version}")

    return exit_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
