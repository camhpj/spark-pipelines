"""Command-line interface for spark-preprocessor."""

import argparse
import logging
from pathlib import Path

import structlog
from sqlglot import parse_one

from spark_preprocessor.compiler import compile_pipeline
from spark_preprocessor.errors import SparkPreprocessorError
from spark_preprocessor.scaffold import scaffold_pipeline


def _configure_logging() -> None:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spark-preprocessor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    compile_parser = subparsers.add_parser("compile", help="Compile a pipeline")
    compile_parser.add_argument("--pipeline", required=True, type=Path)
    compile_parser.add_argument("--out", required=True, type=Path)

    render_parser = subparsers.add_parser(
        "render-sql", help="Render SQL from a pipeline"
    )
    render_parser.add_argument("--pipeline", required=True, type=Path)
    render_parser.add_argument("--out", required=True, type=Path)

    test_parser = subparsers.add_parser("test", help="Validate rendered SQL")
    test_parser.add_argument("--pipeline", required=True, type=Path)
    test_parser.add_argument("--project", required=True, type=Path)

    scaffold_parser = subparsers.add_parser(
        "scaffold", help="Generate a starter pipeline YAML from a mapping"
    )
    scaffold_parser.add_argument("--mapping", required=True, type=Path)
    scaffold_parser.add_argument("--out", required=True, type=Path)

    return parser


def _run_compile(args: argparse.Namespace) -> None:
    report = compile_pipeline(args.pipeline, args.out)
    structlog.get_logger().info(
        "compile_complete",
        pipeline=report.pipeline_name,
        version=report.pipeline_version,
        output_table=report.output_table,
    )


def _run_render(args: argparse.Namespace) -> None:
    report = compile_pipeline(args.pipeline, args.out)
    rendered_path = args.out / "rendered" / f"enriched__{report.pipeline_name}.sql"
    structlog.get_logger().info(
        "render_complete",
        pipeline=report.pipeline_name,
        sql_path=str(rendered_path),
    )


def _run_test(args: argparse.Namespace) -> None:
    report = compile_pipeline(args.pipeline, args.project)
    rendered_path = args.project / "rendered" / f"enriched__{report.pipeline_name}.sql"
    sql = rendered_path.read_text()
    parse_one(sql, dialect="spark")
    structlog.get_logger().info(
        "test_complete",
        pipeline=report.pipeline_name,
        sql_path=str(rendered_path),
    )


def _run_scaffold(args: argparse.Namespace) -> None:
    pipeline_path = scaffold_pipeline(args.mapping, args.out)
    structlog.get_logger().info(
        "scaffold_complete",
        pipeline_path=str(pipeline_path),
    )


def main(argv: list[str] | None = None) -> None:
    _configure_logging()
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "compile":
            _run_compile(args)
        elif args.command == "render-sql":
            _run_render(args)
        elif args.command == "test":
            _run_test(args)
        elif args.command == "scaffold":
            _run_scaffold(args)
        else:
            raise ValueError(f"Unknown command: {args.command}")
    except SparkPreprocessorError as exc:
        structlog.get_logger().error("command_failed", error=str(exc))
        raise SystemExit(1) from exc
