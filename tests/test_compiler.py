from pathlib import Path

import json
import yaml
from sqlglot import parse_one

from spark_preprocessor.compiler import compile_pipeline


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
