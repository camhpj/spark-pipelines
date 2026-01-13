from contextlib import nullcontext as does_not_raise
from pathlib import Path
from types import SimpleNamespace

import pytest

import spark_preprocessor.cli as cli
from spark_preprocessor.errors import ConfigurationError


def test_build_parser_requires_command() -> None:
    """Parser requires a subcommand."""
    parser = cli._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


@pytest.mark.parametrize(
    "argv,expectation",
    [
        (["compile", "--pipeline", "p.yaml", "--out", "out"], does_not_raise()),
        (["render-sql", "--pipeline", "p.yaml", "--out", "out"], does_not_raise()),
        (
            ["test", "--pipeline", "p.yaml", "--project", "proj"],
            does_not_raise(),
        ),
        (["scaffold", "--mapping", "m.yaml", "--out", "out"], does_not_raise()),
    ],
)
def test_main_dispatches_subcommands(tmp_path: Path, argv: list[str], expectation) -> None:
    """CLI dispatch calls the correct handler without doing real work."""
    called = {"compile": 0, "scaffold": 0, "parse": 0}

    def fake_compile_pipeline(pipeline: Path, out: Path):
        called["compile"] += 1
        out.mkdir(parents=True, exist_ok=True)
        report = SimpleNamespace(
            pipeline_name="p",
            pipeline_version="v",
            output_table="t",
        )
        # For `test` command, main reads rendered SQL.
        (out / "rendered").mkdir(parents=True, exist_ok=True)
        (out / "rendered" / "enriched__p.sql").write_text("SELECT 1 AS x")
        return report

    def fake_scaffold_pipeline(mapping: Path, out: Path) -> Path:
        called["scaffold"] += 1
        out.mkdir(parents=True, exist_ok=True)
        p = out / "pipeline.yaml"
        p.write_text("pipeline: {}")
        return p

    def fake_parse_one(sql: str, dialect: str) -> None:
        called["parse"] += 1
        assert sql
        assert dialect == "spark"

    # Avoid logging config affecting global state.
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(cli, "_configure_logging", lambda: None)
    monkeypatch.setattr(cli, "compile_pipeline", fake_compile_pipeline)
    monkeypatch.setattr(cli, "scaffold_pipeline", fake_scaffold_pipeline)
    monkeypatch.setattr(cli, "parse_one", fake_parse_one)

    try:
        # Normalize paths so the fake compiler writes somewhere real.
        normalized: list[str] = []
        for part in argv:
            if part in {"p.yaml", "m.yaml"}:
                normalized.append(str(tmp_path / part))
            elif part in {"out", "proj"}:
                normalized.append(str(tmp_path / part))
            else:
                normalized.append(part)

        with expectation:
            cli.main(normalized)
    finally:
        monkeypatch.undo()

    if argv[0] in {"compile", "render-sql", "test"}:
        assert called["compile"] == 1
    else:
        assert called["compile"] == 0

    if argv[0] == "scaffold":
        assert called["scaffold"] == 1
    else:
        assert called["scaffold"] == 0

    if argv[0] == "test":
        assert called["parse"] == 1
    else:
        assert called["parse"] == 0


def test_main_exits_nonzero_on_domain_error() -> None:
    """Domain errors become exit code 1."""
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(cli, "_configure_logging", lambda: None)

    def boom(*_args, **_kwargs):
        raise ConfigurationError("nope")

    monkeypatch.setattr(cli, "compile_pipeline", boom)
    try:
        with pytest.raises(SystemExit) as excinfo:
            cli.main(["compile", "--pipeline", "p.yaml", "--out", "out"])
        assert excinfo.value.code == 1
    finally:
        monkeypatch.undo()

