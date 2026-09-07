#!/usr/bin/env python
"""Pre-commit hook: check for unjustified loops in src/ files.

Exit 1 if any unjustified loops are found (blocks the commit).
"""
import subprocess
import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    checker = repo_root / "scripts" / "check_loops.py"
    src_dir = repo_root / "src"

    if not checker.exists():
        print("Warning: scripts/check_loops.py not found, skipping loop check.")
        return 0

    if not src_dir.exists():
        print("Warning: src/ directory not found, skipping loop check.")
        return 0

    result = subprocess.run(
        [sys.executable, str(checker), "--src-dir", str(src_dir)],
        capture_output=False,
    )
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
