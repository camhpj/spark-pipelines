from pathlib import Path

import yaml

from spark_preprocessor.compiler import compile_pipeline


def _write_pipeline(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return path


def test_forward_65_flag_emits_join_model(tmp_path: Path) -> None:
    payload = {
        "mapping": {
            "entities": {
                "patients": {
                    "table": "catalog.schema.patients_raw",
                    "columns": {"person_id": "person_id", "pcp_name": "pcp_name"},
                }
            },
            "references": {
                "physicians_65_forward": {
                    "table": "geisinger.reference_data.physicians_65_forward",
                    "columns": {"pcp_name": "pcp_name", "active65f": "active65F"},
                }
            },
        },
        "pipeline": {
            "name": "geisinger_enriched",
            "slug": "geisinger_enriched",
            "version": "v1.0.0",
            "grain": "PERSON",
            "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
            "output": {"table": "catalog.schema.enriched_geisinger", "materialization": "table"},
            "naming": {
                "prefixing": {"enabled": False, "scheme": "feature", "separator": "__"},
                "collision_policy": "fail",
            },
            "validation": {"on_missing_required_column": "fail"},
        },
        "features": [{"key": "geisinger.forward_65_flag"}],
        "profiling": {"enabled": False},
    }

    pipeline_path = _write_pipeline(tmp_path / "pipeline.yaml", payload)
    out_dir = tmp_path / "out"
    compile_pipeline(pipeline_path, out_dir)

    rendered_sql = (out_dir / "rendered" / "enriched__geisinger_enriched.sql").read_text()
    assert "forward_65_flag" in rendered_sql

    feature_model_dir = out_dir / "models" / "features" / "geisinger.forward_65_flag"
    model_files = list(feature_model_dir.glob("*.sql"))
    assert model_files


def test_forward_65_flag_uses_databricks_namespaced_schemas(tmp_path: Path) -> None:
    payload = {
        "mapping": {
            "entities": {
                "patients": {
                    "table": "geisinger.ma_20260101.patients_raw",
                    "columns": {"person_id": "person_id", "pcp_name": "pcp_name"},
                }
            },
            "references": {
                "physicians_65_forward": {
                    "table": "geisinger.ma_20260101.physicians_65_forward",
                    "columns": {"pcp_name": "pcp_name", "active65f": "active65F"},
                }
            },
        },
        "pipeline": {
            "name": "geisinger_enriched",
            "slug": "geisinger_enriched",
            "execution_target": "databricks",
            "version": "v1.0.0",
            "grain": "PERSON",
            "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
            "output": {
                "table": "geisinger.ma_20260101.enriched_geisinger",
                "materialization": "table",
            },
            "naming": {
                "prefixing": {"enabled": False, "scheme": "feature", "separator": "__"},
                "collision_policy": "fail",
            },
            "validation": {"on_missing_required_column": "fail"},
        },
        "features": [{"key": "geisinger.forward_65_flag"}],
        "profiling": {"enabled": False},
    }

    pipeline_path = _write_pipeline(tmp_path / "pipeline.yaml", payload)
    out_dir = tmp_path / "out"
    compile_pipeline(pipeline_path, out_dir)

    rendered_sql = (out_dir / "rendered" / "enriched__geisinger_enriched.sql").read_text()
    assert (
        "FROM geisinger.ma_20260101__geisinger_enriched_semantic.patients p"
        in rendered_sql
    )
    assert (
        "JOIN geisinger.ma_20260101__geisinger_enriched_features.geisinger__forward_65_flag__65_forward_pcp f65"
        in rendered_sql
    )

    feature_model_path = (
        out_dir
        / "models"
        / "features"
        / "geisinger.forward_65_flag"
        / "geisinger__forward_65_flag__65_forward_pcp.sql"
    )
    feature_sql = feature_model_path.read_text()
    assert (
        "FROM geisinger.ma_20260101__geisinger_enriched_semantic.reference__physicians_65_forward"
        in feature_sql
    )
