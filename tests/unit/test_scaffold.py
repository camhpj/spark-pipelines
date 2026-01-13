from pathlib import Path

import pytest
import yaml

from spark_preprocessor.errors import ConfigurationError
from spark_preprocessor.scaffold import _default_spine, scaffold_pipeline
from spark_preprocessor.schema import MappingSpec


def test_default_spine_prefers_patients_person_id() -> None:
    mapping = MappingSpec.model_validate(
        {
            "entities": {
                "patients": {"table": "t", "columns": {"person_id": "pid", "x": "x"}}
            }
        }
    )
    assert _default_spine(mapping) == ("patients", "person_id", ["person_id"])


def test_default_spine_falls_back_to_sorted_entity_and_column() -> None:
    mapping = MappingSpec.model_validate(
        {
            "entities": {
                "z": {"table": "t", "columns": {"b": "b"}},
                "a": {"table": "t", "columns": {"c": "c", "b": "b"}},
            }
        }
    )
    assert _default_spine(mapping) == ("a", "b", ["b"])


def test_default_spine_requires_at_least_one_entity() -> None:
    mapping = MappingSpec.model_validate({"entities": {}})
    with pytest.raises(ConfigurationError):
        _default_spine(mapping)


def test_scaffold_pipeline_writes_pipeline_yaml(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.yaml"
    mapping_path.write_text(
        yaml.safe_dump(
            {
                "mapping": {
                    "entities": {
                        "patients": {
                            "table": "patients_raw",
                            "columns": {"person_id": "person_id"},
                        }
                    }
                }
            }
        )
    )
    out = tmp_path / "out"
    pipeline_path = scaffold_pipeline(mapping_path, out)
    assert pipeline_path.exists()
    data = yaml.safe_load(pipeline_path.read_text())
    assert data["pipeline"]["spine"]["entity"] == "patients"

