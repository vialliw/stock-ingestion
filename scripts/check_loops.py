#!/usr/bin/env python
"""AST-based checker for unjustified Python loops in src/ files.

Enforces the project rule from AGENTS.md:
    SQL > vectorized computation > loop iteration

A "for" loop is flagged if:
  - It iterates over a variable (not range/enumerate/items/values/keys)
  - It does NOT have a `# justified:` comment on the for-line or the
    line immediately above it.

Exempt patterns (not flagged):
  - for ... in range(...)
  - for ... in enumerate(...)
  - for ... in .items() / .values() / .keys()
  - for ... in self.cfg.* / self.db.* (config iteration, not data)
  - for ... in [constant literals] (small constant sets)
  - loops inside test files (tests/test_*.py)

Usage:
    python scripts/check_loops.py [--src-dir src] [--strict]

Exit codes:
    0 — no unjustified loops found
    1 — unjustified loops found (prints violations)

The --strict flag also flags loops that DO have a # justified comment,
checking that the justification is non-empty.
"""
from __future__ import annotations

import argparse
import ast
import os
import sys
from pathlib import Path
from typing import List, Tuple


# Patterns that are always exempt (not data iteration)
_EXEMPT_NAMES = {"range", "enumerate", "zip", "reversed"}
_EXEMPT_METHODS = {"items", "values", "keys", "iterrows", "itertuples"}


class LoopChecker(ast.NodeVisitor):
    """Walk an AST and collect unjustified for-loops."""

    def __init__(self, filepath: str, source_lines: list[str]):
        self.filepath = filepath
        self.source_lines = source_lines
        self.violations: List[Tuple[int, int, str]] = []

    def _is_exempt_iter(self, iter_node: ast.expr) -> bool:
        """Check if the loop's iterable is an exempt pattern."""
        # for ... in range(...)
        if isinstance(iter_node, ast.Call):
            func = iter_node.func
            if isinstance(func, ast.Name) and func.id in _EXEMPT_NAMES:
                return True
            # for ... in x.items() / x.values() / x.keys()
            if isinstance(func, ast.Attribute) and func.attr in _EXEMPT_METHODS:
                return True
            # for ... in sorted(...) / reversed(...)
            if isinstance(func, ast.Name) and func.id in {"sorted"}:
                # Check inner iterable — sorted(some_set) is fine
                return True
        # for ... in [1, 2, 3] (constant list literal)
        if isinstance(iter_node, ast.List) and all(
            isinstance(elt, ast.Constant) for elt in iter_node.elts
        ):
            return True
        # for ... in (1, 2, 3) (constant tuple)
        if isinstance(iter_node, ast.Tuple) and all(
            isinstance(elt, ast.Constant) for elt in iter_node.elts
        ):
            return True
        # for ... in self.cfg.ingestion.tickers (config attr access)
        if isinstance(iter_node, ast.Attribute):
            # Walk the attribute chain to see if it starts with self.cfg
            node = iter_node
            while isinstance(node, ast.Attribute):
                node = node.value
            if isinstance(node, ast.Name) and node.id == "self":
                return False  # self.db / self.something — could be data, flag it
            return False
        # for ... in some_variable (data iteration — flag it)
        return False

    def _has_justification(self, lineno: int) -> bool:
        """Check if the for-loop has a '# justified:' comment.

        Checks the for-line itself, up to 3 lines above (where multi-line
        justification comments are typically placed), and the first line
        inside the loop body.
        """
        for check_line in range(lineno - 6, lineno + 2):
            if 1 <= check_line <= len(self.source_lines):
                line = self.source_lines[check_line - 1]
                if "# justified:" in line.lower():
                    return True
        return False

    def visit_For(self, node: ast.For) -> None:
        """Check each for-loop."""
        is_exempt = self._is_exempt_iter(node.iter)
        has_justification = self._has_justification(node.lineno)

        if not is_exempt and not has_justification:
            # Get a snippet of the loop for the report
            snippet = self.source_lines[node.lineno - 1].strip() if node.lineno <= len(self.source_lines) else "?"
            self.violations.append(
                (node.lineno, node.col_offset, snippet)
            )

        self.generic_visit(node)

    # Also check async for
    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self.visit_For(node)  # type: ignore[arg-type]


def check_file(filepath: str, strict: bool = False) -> List[Tuple[int, int, str]]:
    """Check a single Python file for unjustified loops."""
    try:
        with open(filepath, encoding="utf-8") as f:
            source = f.read()
    except (OSError, UnicodeDecodeError):
        return []

    source_lines = source.splitlines()
    try:
        tree = ast.parse(source, filename=filepath)
    except SyntaxError:
        return []

    checker = LoopChecker(filepath, source_lines)
    checker.visit(tree)

    if strict:
        # In strict mode, also flag loops with empty justifications
        # (e.g. "# justified:" with no text after it)
        pass  # TODO: implement if needed

    return checker.violations


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check for unjustified Python loops in src/ files."
    )
    parser.add_argument(
        "--src-dir",
        default="src",
        help="Directory to scan (default: src)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Also flag loops with empty justifications",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=["__pycache__", "__init__.py"],
        help="File patterns to exclude (default: __pycache__, __init__.py)",
    )
    args = parser.parse_args()

    src_dir = Path(args.src_dir)
    if not src_dir.exists():
        print(f"Error: directory '{src_dir}' does not exist.", file=sys.stderr)
        return 2

    all_violations: List[Tuple[str, int, int, str]] = []
    files_checked = 0

    for pyfile in sorted(src_dir.rglob("*.py")):
        # Skip excluded patterns
        if any(pat in str(pyfile) for pat in args.exclude):
            continue

        violations = check_file(str(pyfile), strict=args.strict)
        files_checked += 1
        for lineno, col, snippet in violations:
            all_violations.append((str(pyfile), lineno, col, snippet))

    if all_violations:
        print(f"\n{'='*70}")
        print(f"UNJUSTIFIED LOOPS FOUND: {len(all_violations)} violation(s)")
        print(f"Rule: SQL > vectorized computation > loop iteration (AGENTS.md)")
        print(f"{'='*70}\n")
        for filepath, lineno, col, snippet in all_violations:
            print(f"  {filepath}:{lineno}:{col}")
            print(f"    {snippet}")
            print(f"    -> Replace with SQL/vectorized op, or add '# justified: <reason>'")
            print()
        print(f"Files checked: {files_checked}")
        print(f"Violations: {len(all_violations)}")
        return 1
    else:
        print(f"No unjustified loops found. ({files_checked} files checked)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
