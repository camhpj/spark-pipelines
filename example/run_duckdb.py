"""Compile a pipeline and apply it against DuckDB for a local smoke run."""

from pathlib import Path

import duckdb
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.config.connection import DuckDBConnectionConfig
from sqlmesh.core.config.gateway import GatewayConfig
from sqlmesh.core.context import Context

from spark_preprocessor.compiler import compile_pipeline


def main() -> None:
    """Compile the example pipeline and apply it using DuckDB."""

    base_dir = Path(__file__).resolve().parent
    data_path = base_dir / "data" / "patients.csv"
    if not data_path.exists():
        raise SystemExit(
            "Missing data. Run `uv run python example/generate_data.py` first."
        )

    pipeline_path = base_dir / "pipeline.yaml"
    out_dir = base_dir / "out"
    compile_pipeline(pipeline_path, out_dir)

    db_path = base_dir / "duckdb.db"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE OR REPLACE TABLE patients_raw AS SELECT * FROM read_csv_auto(?)",
        [str(data_path)],
    )
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
    rows = conn.execute(
        "SELECT person_id, age, age_bucket FROM semantic.enriched_example ORDER BY person_id LIMIT 5"
    ).fetchall()
    conn.close()

    print("Sample rows:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
