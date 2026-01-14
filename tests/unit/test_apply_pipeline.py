from pathlib import Path
from types import SimpleNamespace

import pytest

import spark_preprocessor.runtime.apply_pipeline as apply_pipeline
from spark_preprocessor.errors import ConfigurationError


def test_build_parser_defaults_environment() -> None:
    parser = apply_pipeline._build_parser()
    args = parser.parse_args(["--pipeline", "p.yaml", "--project", "proj"])
    assert args.environment is None


def test_main_applies_plan(tmp_path: Path) -> None:
    calls: list[str] = []

    class FakeContext:
        def __init__(self, *, paths: Path):
            assert paths == tmp_path / "proj"

        def plan(self, *, environment, no_prompts: bool):
            assert environment == "dev"
            assert no_prompts is True
            calls.append("plan")
            return "plan"

        def apply(self, plan):
            assert plan == "plan"
            calls.append("apply")

    def fake_load_pipeline_document(_path: Path):
        return SimpleNamespace(
            pipeline=SimpleNamespace(
                name="p",
                version="v",
                output=SimpleNamespace(table="t"),
            )
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(apply_pipeline, "load_pipeline_document", fake_load_pipeline_document)

    # Patch the runtime import inside main by injecting into sys.modules.
    import sys
    import types

    fake_sqlmesh = types.SimpleNamespace(
        core=types.SimpleNamespace(context=types.SimpleNamespace(Context=FakeContext))
    )
    module_keys = ("sqlmesh", "sqlmesh.core", "sqlmesh.core.context")
    original_modules = {key: sys.modules.get(key) for key in module_keys}
    sys.modules["sqlmesh"] = fake_sqlmesh  # type: ignore[assignment]
    sys.modules["sqlmesh.core"] = fake_sqlmesh.core
    sys.modules["sqlmesh.core.context"] = fake_sqlmesh.core.context

    try:
        apply_pipeline.main(
            [
                "--pipeline",
                str(tmp_path / "p.yaml"),
                "--project",
                str(tmp_path / "proj"),
                "--environment",
                "dev",
            ]
        )
    finally:
        for key, original in original_modules.items():
            if original is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = original
        monkeypatch.undo()

    assert calls == ["plan", "apply"]


def test_main_exits_nonzero_on_domain_error() -> None:
    monkeypatch = pytest.MonkeyPatch()

    def boom(_path: Path):
        raise ConfigurationError("bad")

    monkeypatch.setattr(apply_pipeline, "load_pipeline_document", boom)
    try:
        with pytest.raises(SystemExit) as excinfo:
            apply_pipeline.main(["--pipeline", "p.yaml", "--project", "proj"])
        assert excinfo.value.code == 1
    finally:
        monkeypatch.undo()


def test_main_reraises_unexpected_exception(tmp_path: Path) -> None:
    class FakeContext:
        def __init__(self, *, paths: Path):
            assert paths == tmp_path / "proj"

        def plan(self, *, environment, no_prompts: bool):
            raise RuntimeError("boom")

        def apply(self, plan):
            raise AssertionError("unreachable")

    def fake_load_pipeline_document(_path: Path):
        return SimpleNamespace(
            pipeline=SimpleNamespace(
                name="p",
                version="v",
                output=SimpleNamespace(table="t"),
            )
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(apply_pipeline, "load_pipeline_document", fake_load_pipeline_document)

    import sys
    import types

    fake_sqlmesh = types.SimpleNamespace(
        core=types.SimpleNamespace(context=types.SimpleNamespace(Context=FakeContext))
    )
    module_keys = ("sqlmesh", "sqlmesh.core", "sqlmesh.core.context")
    original_modules = {key: sys.modules.get(key) for key in module_keys}
    sys.modules["sqlmesh"] = fake_sqlmesh  # type: ignore[assignment]
    sys.modules["sqlmesh.core"] = fake_sqlmesh.core
    sys.modules["sqlmesh.core.context"] = fake_sqlmesh.core.context

    try:
        with pytest.raises(RuntimeError, match="boom"):
            apply_pipeline.main(
                [
                    "--pipeline",
                    str(tmp_path / "p.yaml"),
                    "--project",
                    str(tmp_path / "proj"),
                ]
            )
    finally:
        for key, original in original_modules.items():
            if original is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = original
        monkeypatch.undo()
