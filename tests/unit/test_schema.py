from contextlib import nullcontext as does_not_raise
from pathlib import Path

import pytest
import yaml

from spark_preprocessor.errors import ConfigurationError
from spark_preprocessor.schema import load_mapping_spec, load_pipeline_document


@pytest.mark.parametrize(
    "payload,expectation",
    [
        (
            {"mapping": {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}}},
            does_not_raise(),
        ),
        (
            {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}},
            does_not_raise(),
        ),
        ("nope", pytest.raises(ConfigurationError)),
    ],
)
def test_load_mapping_spec_parses_or_rejects(tmp_path: Path, payload, expectation) -> None:
    path = tmp_path / "mapping.yaml"
    path.write_text(yaml.safe_dump(payload))
    with expectation:
        spec = load_mapping_spec(path)
        assert spec.entities["patients"].table == "t"


def test_load_mapping_spec_missing_file() -> None:
    with pytest.raises(ConfigurationError):
        load_mapping_spec(Path("does_not_exist.yaml"))


def test_load_pipeline_document_rejects_non_mapping_yaml(tmp_path: Path) -> None:
    path = tmp_path / "pipeline.yaml"
    path.write_text(yaml.safe_dump("nope"))
    with pytest.raises(ConfigurationError):
        load_pipeline_document(path)


def test_load_pipeline_document_missing_file() -> None:
    with pytest.raises(ConfigurationError, match="Pipeline file not found"):
        load_pipeline_document(Path("does_not_exist.yaml"))


def test_load_pipeline_document_validation_error_is_wrapped(tmp_path: Path) -> None:
    path = tmp_path / "pipeline.yaml"
    path.write_text(yaml.safe_dump({"mapping": {"entities": {"patients": {"table": "t", "columns": {}}}}}))
    with pytest.raises(ConfigurationError, match="Pipeline YAML failed validation"):
        load_pipeline_document(path)


def test_load_mapping_spec_validation_error_is_wrapped(tmp_path: Path) -> None:
    path = tmp_path / "mapping.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "mapping": {
                    "entities": {
                        "patients": {
                            "table": "t",
                            "columns": "not-a-mapping",
                        }
                    }
                }
            }
        )
    )
    with pytest.raises(ConfigurationError, match="Mapping YAML failed validation"):
        load_mapping_spec(path)


def test_mapping_spec_reference_helpers() -> None:
    from spark_preprocessor.schema import MappingSpec

    spec = MappingSpec.model_validate(
        {
            "entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}},
            "references": {"icd10": {"table": "t2", "columns": {"code": "code"}}},
        }
    )
    assert spec.has_reference("icd10") is True
    assert spec.has_column("icd10", "code") is True
    assert spec.entity_table("icd10") == "t2"
    assert spec.entity_columns("icd10") == {"code": "code"}


def test_mapping_spec_unknown_entity_raises() -> None:
    from spark_preprocessor.schema import MappingSpec

    spec = MappingSpec.model_validate(
        {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}}
    )
    with pytest.raises(ConfigurationError):
        spec.entity_table("nope")
