"""Databricks runtime entrypoint for applying a compiled pipeline."""

from __future__ import annotations


def main() -> None:
    raise RuntimeError(
        "SQLMesh runtime integration is not implemented yet. "
        "This entrypoint will be wired once SQLMesh execution semantics are finalized."
    )
