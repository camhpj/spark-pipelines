"""Databricks runtime entrypoint for applying a compiled pipeline."""

import argparse
import importlib
import logging
from pathlib import Path

import structlog

from spark_preprocessor.errors import SparkPreprocessorError
from spark_preprocessor.model_naming import (
    databricks_namespaces,
    quote_databricks_identifier_part,
)
from spark_preprocessor.schema import load_pipeline_document


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spark-preprocessor-apply")
    parser.add_argument("--pipeline", required=True, type=Path)
    parser.add_argument("--project", required=True, type=Path)
    parser.add_argument("--environment", default=None)
    return parser


def main(argv: list[str] | None = None) -> None:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    log = structlog.get_logger()
    try:
        # Import at runtime to keep module import light-weight and easier to unit-test.
        from sqlmesh.core.context import Context

        document = load_pipeline_document(args.pipeline)
        if document.pipeline.execution_target == "databricks":
            namespaces = databricks_namespaces(
                output_table=document.pipeline.output.table,
                pipeline_slug_value=document.pipeline.slug,
                semantic_schema_suffix=document.pipeline.databricks.semantic_schema_suffix,
                features_schema_suffix=document.pipeline.databricks.features_schema_suffix,
            )
            try:
                spark_sql = importlib.import_module("pyspark.sql")
                spark_session = getattr(spark_sql, "SparkSession")
            except Exception as exc:
                raise SparkPreprocessorError(
                    "Databricks execution_target requires pyspark at runtime"
                ) from exc

            spark = spark_session.builder.getOrCreate()
            catalog_sql = quote_databricks_identifier_part(namespaces.catalog)
            semantic_sql = quote_databricks_identifier_part(namespaces.semantic_schema)
            features_sql = quote_databricks_identifier_part(namespaces.features_schema)

            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{semantic_sql}")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{features_sql}")

        context = Context(paths=args.project)
        plan = context.plan(environment=args.environment, no_prompts=True)
        context.apply(plan)
        log.info(
            "apply_complete",
            pipeline=document.pipeline.name,
            version=document.pipeline.version,
            output_table=document.pipeline.output.table,
        )
    except SparkPreprocessorError as exc:
        log.error("apply_failed", error=str(exc))
        raise SystemExit(1) from exc
    except Exception as exc:  # noqa: BLE001 - boundary logging for Databricks runtime
        log.error("apply_failed", error=str(exc))
        raise
