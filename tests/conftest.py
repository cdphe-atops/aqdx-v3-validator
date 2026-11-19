import runpy
from pathlib import Path


def pytest_configure(config):
    """Regenerate deterministic test fixtures before pytest collects test files."""
    if not hasattr(
        config, "workerinput"
    ):  # Avoid duplicate runs if pytest-xdist is used
        script_path = (
            Path(__file__).resolve().parent.parent
            / "scripts"
            / "generate_test_files.py"
        )
        runpy.run_path(str(script_path), run_name="__main__")
