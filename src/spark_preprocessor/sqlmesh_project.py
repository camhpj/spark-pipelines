"""Helpers for generating SQLMesh project artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import yaml

from spark_preprocessor.features.base import SqlmeshModelSpec


@dataclass(frozen=True)
class SqlmeshConfig:
    """Minimal SQLMesh project configuration."""

    engine_type: str = "databricks"
    dialect: str = "spark"


def render_sqlmesh_model(spec: SqlmeshModelSpec) -> str:
    """Render a SQLMesh model file content."""

    header_items = [f"name {spec.name}", f"kind {spec.kind}"]
    if spec.tags:
        tags = ", ".join(spec.tags)
        header_items.append(f"tags [{tags}]")
    header_body = ",\n  ".join(header_items)
    header = f"MODEL (\n  {header_body}\n)"
    return f"{header}\n\n{spec.sql.strip()}\n"


def render_sqlmesh_config(config: SqlmeshConfig) -> str:
    """Render sqlmesh.yaml content."""

    payload = {
        "model_defaults": {"dialect": config.dialect},
        "engine": {"type": config.engine_type},
    }
    return yaml.safe_dump(payload, sort_keys=False)


def render_models(specs: Iterable[SqlmeshModelSpec]) -> dict[str, str]:
    """Render a mapping from model name to SQL content."""

    return {spec.name: render_sqlmesh_model(spec) for spec in specs}
