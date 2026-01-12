"""Databricks runtime entrypoint for applying a compiled pipeline."""

import argparse
import logging
from pathlib import Path

import structlog
from sqlmesh.core.context import Context

from spark_preprocessor.errors import SparkPreprocessorError
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
        document = load_pipeline_document(args.pipeline)
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
