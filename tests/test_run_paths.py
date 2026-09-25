from pathlib import Path

from jevymarket.run_paths import RUNS_DIR, run_output_path


def test_default_output_goes_under_runs_and_creates_parent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = run_output_path(None, "sample.db")
    assert path == RUNS_DIR / "sample.db"
    assert (tmp_path / "runs").is_dir()


def test_explicit_output_path_is_respected_and_parent_created(tmp_path):
    explicit = tmp_path / "custom" / "report.json.gz"
    path = run_output_path(explicit, "ignored.json.gz")
    assert path == explicit
    assert explicit.parent.is_dir()


def test_helper_does_not_create_output_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = run_output_path(None, "reserved.json.gz")
    assert not path.exists()
