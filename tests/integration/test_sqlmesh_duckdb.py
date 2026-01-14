from pathlib import Path

import duckdb
import yaml
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.config.connection import DuckDBConnectionConfig
from sqlmesh.core.config.gateway import GatewayConfig
from sqlmesh.core.context import Context

from spark_preprocessor.compiler import compile_pipeline
from spark_preprocessor.features.base import ColumnSpec, FeatureAssets, FeatureMetadata
from spark_preprocessor.features.registry import register_feature


class _DuckDBBaseFeature:
    meta = FeatureMetadata(
        key="test.duckdb_base",
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


register_feature(_DuckDBBaseFeature())


def _write_pipeline(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return path


def test_sqlmesh_duckdb_smoke(tmp_path: Path) -> None:
    payload = {
        "mapping": {
            "entities": {
                "patients": {
                    "table": "patients_raw",
                    "columns": {"person_id": "person_id"},
                }
            }
        },
        "pipeline": {
            "name": "duckdb_enriched",
            "slug": "duckdb_enriched",
            "version": "v1.0.0",
            "grain": "PERSON",
            "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
            "output": {"table": "semantic.enriched_output", "materialization": "table"},
            "naming": {
                "prefixing": {"enabled": False, "scheme": "feature", "separator": "__"},
                "collision_policy": "fail",
            },
            "validation": {"on_missing_required_column": "fail"},
        },
        "features": [{"key": "test.duckdb_base"}],
        "profiling": {"enabled": False},
    }

    pipeline_path = _write_pipeline(tmp_path / "pipeline.yaml", payload)
    out_dir = tmp_path / "out"
    compile_pipeline(pipeline_path, out_dir)

    db_path = tmp_path / "duckdb.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE patients_raw (person_id VARCHAR)")
    conn.execute("INSERT INTO patients_raw VALUES ('p1')")
    conn.close()

    config = Config(
        gateways={
            "": GatewayConfig(
                connection=DuckDBConnectionConfig(database=str(db_path)),
            )
        },
        model_defaults=ModelDefaultsConfig(dialect="spark"),
    )
    context = Context(paths=out_dir, config=config)
    plan = context.plan(no_prompts=True)
    context.apply(plan)

    conn = duckdb.connect(str(db_path))
    rows = conn.execute("SELECT person_id, base_val FROM semantic.enriched_output").fetchall()
    conn.close()

    assert rows == [("p1", 1)]
