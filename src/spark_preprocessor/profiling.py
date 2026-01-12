"""Profiling notebook generation."""

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
    sampling_mode = profiling.sampling_mode
    sampling_seed = profiling.sampling_seed

    lines: list[str] = []
    lines.append("# Databricks notebook source")
    lines.append("# COMMAND ----------")
    lines.append("from ydata_profiling import ProfileReport")
    lines.append("from pyspark.sql import functions as F")
    lines.append("from time import time")
    lines.append("# COMMAND ----------")
    lines.append("# Configuration")
    lines.append(f"output_dir = '{output_dir}'")
    lines.append(f"sample_rows = {sample_rows}")
    lines.append(f"sampling_mode = '{sampling_mode}'")
    lines.append(f"sampling_seed = {sampling_seed}")
    lines.append("dbutils.fs.mkdirs(output_dir)")
    lines.append("run_started_at = time()")
    lines.append("# COMMAND ----------")
    lines.append("def sample_table(table_name: str):")
    lines.append("    df = spark.table(table_name)")
    lines.append("    if sampling_mode == 'deterministic':")
    lines.append("        df = df.orderBy(F.rand(sampling_seed))")
    lines.append("    else:")
    lines.append("        df = df.orderBy(F.rand())")
    lines.append("    return df.limit(sample_rows)")

    def add_profile_block(table_name: str, title: str) -> None:
        safe_name = table_name.replace(".", "_")
        lines.append("# COMMAND ----------")
        lines.append(f"df = sample_table('{table_name}')")
        lines.append(f"report = ProfileReport(df, title='{title}')")
        lines.append(f"report.to_file(f'{output_dir}/{safe_name}.html')")
        lines.append("displayHTML(report.to_html())")

    for entity in profiling.profile_raw_entities:
        table_name = f"semantic.{entity}"
        add_profile_block(table_name, f"{entity} (semantic)")

    if profiling.profile_output:
        add_profile_block(document.pipeline.output.table, "enriched output")

    lines.append("# COMMAND ----------")
    lines.append("summary = []")
    lines.append("def add_summary(table_name: str) -> None:")
    lines.append("    df = sample_table(table_name)")
    lines.append(
        "    summary.append({'table': table_name, 'rows': df.count(), 'schema': df.schema.simpleString()})"
    )
    for entity in profiling.profile_raw_entities:
        lines.append(f"add_summary('semantic.{entity}')")
    if profiling.profile_output:
        lines.append(f"add_summary('{document.pipeline.output.table}')")
    lines.append("summary.append({'run_seconds': time() - run_started_at})")
    lines.append("display(summary)")

    return "\n".join(lines) + "\n"
