"""Import helper so the same tests can check `tasks/` or `solutions/`."""

import importlib
import os

PACKAGE = os.environ.get("PYPRACTICE_PACKAGE", "tasks")


def load(module_name):
    """Import e.g. tasks.level1_basics (or solutions.level1_basics)."""
    return importlib.import_module(f"{PACKAGE}.{module_name}")
