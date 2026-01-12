"""Profiling notebook generation."""

from __future__ import annotations

from spark_preprocessor.schema import PipelineDocument


def render_profiling_notebook(document: PipelineDocument) -> str:
    """Render a Databricks notebook that profiles selected entities and output."""

    profiling = document.profiling
    if profiling is None:
        return ""

    output_dir = (
        profiling.output_dir or f"dbfs:/FileStore/profiles/{document.pipeline.name}"
    )
    sample_rows = profiling.sample_rows

    lines: list[str] = []
    lines.append("# Databricks notebook source")
    lines.append("# COMMAND ----------")
    lines.append("from ydata_profiling import ProfileReport")
    lines.append("# COMMAND ----------")
    lines.append(f"output_dir = '{output_dir}'")
    lines.append(f"sample_rows = {sample_rows}")
    lines.append("dbutils.fs.mkdirs(output_dir)")

    def add_profile_block(table_name: str, title: str) -> None:
        safe_name = table_name.replace(".", "_")
        lines.append("# COMMAND ----------")
        lines.append(f"df = spark.table('{table_name}').limit(sample_rows)")
        lines.append(f"report = ProfileReport(df, title='{title}')")
        lines.append(f"report.to_file(f'{output_dir}/{safe_name}.html')")
        lines.append("displayHTML(report.to_html())")

    for entity in profiling.profile_raw_entities:
        table_name = f"semantic.{entity}"
        add_profile_block(table_name, f"{entity} (semantic)")

    if profiling.profile_output:
        add_profile_block(document.pipeline.output.table, "enriched output")

    return "\n".join(lines) + "\n"
