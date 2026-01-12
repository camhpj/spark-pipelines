from contextlib import nullcontext as does_not_raise
from pathlib import Path

import json
import pytest
import yaml
from sqlglot import parse_one

from spark_preprocessor.compiler import compile_pipeline
from spark_preprocessor.errors import ConfigurationError, ValidationError
from spark_preprocessor.features.base import (
    ColumnSpec,
    FeatureAssets,
    FeatureMetadata,
)
from spark_preprocessor.features.registry import register_feature


class _DupFeatureA:
    meta = FeatureMetadata(
        key="test.dup_a",
        description=None,
        params=(),
        requirements=(),
        provides=(ColumnSpec(name="dup", dtype="int"),),
        compatible_grains=("PERSON",),
    )

    def build(self, ctx, params):  # noqa: D401 - testing helper
        return FeatureAssets(
            models=[],
            join_models=[],
            select_expressions=["1 AS dup"],
            tests=[],
        )


class _DupFeatureB:
    meta = FeatureMetadata(
        key="test.dup_b",
        description=None,
        params=(),
        requirements=(),
        provides=(ColumnSpec(name="dup", dtype="int"),),
        compatible_grains=("PERSON",),
    )

    def build(self, ctx, params):  # noqa: D401 - testing helper
        return FeatureAssets(
            models=[],
            join_models=[],
            select_expressions=["2 AS dup"],
            tests=[],
        )


class _BaseFeature:
    meta = FeatureMetadata(
        key="test.base",
        description=None,
        params=(),
        requirements=(),
        provides=(ColumnSpec(name="base_val", dtype="int"),),
        compatible_grains=("PERSON",),
    )

    def build(self, ctx, params):  # noqa: D401 - testing helper
        return FeatureAssets(
            models=[],
            join_models=[],
            select_expressions=["1 AS base_val"],
            tests=[],
        )


class _DerivedFeature:
    meta = FeatureMetadata(
        key="test.derived",
        description=None,
        params=(),
        requirements=(),
        provides=(ColumnSpec(name="derived_val", dtype="int"),),
        compatible_grains=("PERSON",),
    )

    def build(self, ctx, params):  # noqa: D401 - testing helper
        return FeatureAssets(
            models=[],
            join_models=[],
            select_expressions=["base_val + 1 AS derived_val"],
            tests=[],
        )


register_feature(_DupFeatureA())
register_feature(_DupFeatureB())
register_feature(_BaseFeature())
register_feature(_DerivedFeature())


def _write_pipeline(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return path


def _base_payload() -> dict:
    return {
        "mapping": {
            "entities": {
                "patients": {
                    "table": "catalog.schema.patients_raw",
                    "columns": {
                        "person_id": "member_id",
                        "date_of_birth": "dob",
                        "as_of_date": "as_of_date",
                    },
                }
            }
        },
        "pipeline": {
            "name": "client_x_enriched",
            "version": "v1.0.0",
            "grain": "PERSON",
            "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
            "output": {"table": "catalog.schema.enriched_client_x", "materialization": "table"},
            "naming": {
                "prefixing": {"enabled": False, "scheme": "feature", "separator": "__"},
                "collision_policy": "fail",
            },
            "validation": {"on_missing_required_column": "fail"},
        },
        "features": [
            {"key": "age", "params": {"start": "date_of_birth", "end": "as_of_date"}},
            {"key": "age_bucket"},
        ],
        "profiling": {
            "enabled": True,
            "sample_rows": 1000,
            "sampling_mode": "deterministic",
            "sampling_seed": 1234,
            "profile_raw_entities": ["patients"],
            "profile_output": True,
            "output_dir": "dbfs:/FileStore/profiles/client_x",
        },
    }


def test_compile_outputs_and_renders_sql(tmp_path: Path) -> None:
    pipeline_path = _write_pipeline(tmp_path / "pipeline.yaml", _base_payload())
    out_dir = tmp_path / "out"

    report = compile_pipeline(pipeline_path, out_dir)

    assert (out_dir / "sqlmesh.yaml").exists()
    assert (out_dir / "models" / "semantic" / "patients.sql").exists()
    assert (out_dir / "models" / "marts" / "enriched__client_x_enriched.sql").exists()
    assert (out_dir / "rendered" / "enriched__client_x_enriched.sql").exists()
    assert (out_dir / "manifest" / "compile_report.json").exists()
    assert (out_dir / "notebooks" / "profile__client_x_enriched.py").exists()

    sql = (out_dir / "rendered" / "enriched__client_x_enriched.sql").read_text()
    assert "age" in sql
    assert "age_bucket" in sql
    parse_one(sql, dialect="spark")

    assert report.included_features == ["age", "age_bucket"]


def test_warn_skip_on_missing_column_ref(tmp_path: Path) -> None:
    payload = _base_payload()
    payload["mapping"]["entities"]["patients"]["columns"].pop("as_of_date")
    payload["pipeline"]["validation"]["on_missing_required_column"] = "warn_skip"

    pipeline_path = _write_pipeline(tmp_path / "pipeline.yaml", payload)
    out_dir = tmp_path / "out"

    report = compile_pipeline(pipeline_path, out_dir)

    assert report.included_features == []
    assert "age" in report.skipped_features
    assert "age_bucket" in report.skipped_features

    report_path = out_dir / "manifest" / "compile_report.json"
    data = json.loads(report_path.read_text())
    assert data["skipped_features"]


def test_collision_policy_auto_prefix(tmp_path: Path) -> None:
    payload = _base_payload()
    payload["features"] = [{"key": "test.dup_a"}, {"key": "test.dup_b"}]
    payload["pipeline"]["naming"]["collision_policy"] = "auto_prefix"

    pipeline_path = _write_pipeline(tmp_path / "pipeline.yaml", payload)
    out_dir = tmp_path / "out"

    compile_pipeline(pipeline_path, out_dir)

    sql = (out_dir / "rendered" / "enriched__client_x_enriched.sql").read_text()
    assert "test_dup_a__dup" in sql
    assert "test_dup_b__dup" in sql


def test_missing_feature_dependency_warn_skip(tmp_path: Path) -> None:
    payload = _base_payload()
    payload["features"] = [{"key": "test.derived"}]
    payload["pipeline"]["validation"]["on_missing_required_column"] = "warn_skip"

    pipeline_path = _write_pipeline(tmp_path / "pipeline.yaml", payload)
    out_dir = tmp_path / "out"

    report = compile_pipeline(pipeline_path, out_dir)

    assert report.included_features == []
    assert "test.derived" in report.skipped_features


def _payload_invalid_canonical_name() -> dict:
    payload = _base_payload()
    payload["mapping"]["entities"]["patients"]["columns"]["PersonID"] = "person_id"
    return payload


def _payload_non_person_grain() -> dict:
    payload = _base_payload()
    payload["pipeline"]["grain"] = "ENCOUNTER"
    payload["pipeline"]["spine"]["entity"] = "encounters"
    payload["pipeline"]["spine"]["key"] = "encounter_id"
    payload["pipeline"]["spine"]["columns"] = ["encounter_id"]
    payload["mapping"]["entities"]["encounters"] = {
        "table": "catalog.schema.encounters_raw",
        "columns": {"encounter_id": "encounter_id", "person_id": "person_id"},
    }
    return payload


@pytest.mark.parametrize(
    "payload,expectation",
    [
        (_base_payload(), does_not_raise()),
        (_payload_invalid_canonical_name(), pytest.raises(ConfigurationError)),
        (_payload_non_person_grain(), pytest.raises(ValidationError)),
    ],
)
def test_compile_validation_cases(
    payload: dict, expectation, tmp_path: Path
) -> None:
    pipeline_path = _write_pipeline(tmp_path / "pipeline.yaml", payload)
    with expectation:
        compile_pipeline(pipeline_path, tmp_path / "out")


def test_profiling_notebook_sampling_mode(tmp_path: Path) -> None:
    payload = _base_payload()
    payload["profiling"]["sampling_mode"] = "random"
    payload["profiling"]["sampling_seed"] = 9876

    pipeline_path = _write_pipeline(tmp_path / "pipeline.yaml", payload)
    out_dir = tmp_path / "out"

    compile_pipeline(pipeline_path, out_dir)

    notebook = (out_dir / "notebooks" / "profile__client_x_enriched.py").read_text()
    assert "sampling_mode = 'random'" in notebook
    assert "df = df.orderBy(F.rand())" in notebook
