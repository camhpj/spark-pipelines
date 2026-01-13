from spark_preprocessor.profiling import render_profiling_notebook
from spark_preprocessor.schema import PipelineDocument


def test_render_profiling_notebook_returns_empty_when_disabled() -> None:
    doc = PipelineDocument.model_validate(
        {
            "mapping": {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}},
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
            },
            "features": [],
            "profiling": None,
        }
    )
    assert render_profiling_notebook(doc) == ""


def test_render_profiling_notebook_uses_default_output_dir_and_includes_tables() -> None:
    doc = PipelineDocument.model_validate(
        {
            "mapping": {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}},
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "semantic.out", "materialization": "table"},
            },
            "features": [],
            "profiling": {
                "enabled": True,
                "sample_rows": 10,
                "sampling_mode": "deterministic",
                "sampling_seed": 7,
                "profile_raw_entities": ["patients"],
                "profile_output": True,
                "output_dir": None,
            },
        }
    )
    text = render_profiling_notebook(doc)
    assert "output_dir = 'dbfs:/FileStore/profiles/p'" in text
    assert "df = sample_table('semantic.patients')" in text
    assert "df = sample_table('semantic.out')" in text
