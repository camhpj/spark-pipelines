"""Scaffold utilities for generating pipeline templates."""

from pathlib import Path

import yaml

from spark_preprocessor.errors import ConfigurationError
from spark_preprocessor.schema import MappingSpec, load_mapping_spec


def scaffold_pipeline(mapping_path: Path, out_dir: Path) -> Path:
    """Create a starter pipeline YAML from a mapping file.

    Args:
        mapping_path: Path to the mapping YAML (mapping-only or full document).
        out_dir: Directory where the scaffolded pipeline.yaml will be written.

    Returns:
        Path to the generated pipeline.yaml.

    Raises:
        ConfigurationError: If the mapping file is invalid.
    """

    mapping = load_mapping_spec(mapping_path)
    spine_entity, spine_key, spine_columns = _default_spine(mapping)

    payload = {
        "mapping": mapping.model_dump(),
        "pipeline": {
            "name": "pipeline_name",
            "version": "v0.1.0",
            "grain": "PERSON",
            "spine": {
                "entity": spine_entity,
                "key": spine_key,
                "columns": spine_columns,
            },
            "output": {
                "table": "catalog.schema.output_table",
                "materialization": "table",
            },
            "naming": {
                "prefixing": {"enabled": False, "scheme": "feature", "separator": "__"},
                "collision_policy": "fail",
            },
            "validation": {"on_missing_required_column": "fail"},
        },
        "features": [],
        "profiling": {
            "enabled": False,
            "sample_rows": 100000,
            "sampling_mode": "random",
            "sampling_seed": 42,
            "profile_raw_entities": [],
            "profile_output": True,
            "output_dir": None,
        },
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    pipeline_path = out_dir / "pipeline.yaml"
    pipeline_path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return pipeline_path


def _default_spine(mapping: MappingSpec) -> tuple[str, str, list[str]]:
    if not mapping.entities:
        raise ConfigurationError("Mapping must define at least one entity.")

    if "patients" in mapping.entities:
        entity = "patients"
    else:
        entity = sorted(mapping.entities.keys())[0]

    columns = mapping.entities[entity].columns
    if "person_id" in columns:
        key = "person_id"
    else:
        key = sorted(columns.keys())[0]

    return entity, key, [key]
