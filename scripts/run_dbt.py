"""
Run dbt for this repo with the settings from .env.

dbt reads connection details from environment variables (dbt/profiles.yml),
but nothing loads .env automatically. This wrapper loads it, points dbt at
dbt/ for both the project and the profile, and passes everything else through.
Works the same in PowerShell, Git Bash and on macOS/Linux.

Usage (venv active):
    python scripts/run_dbt.py debug          # check the connection
    python scripts/run_dbt.py build          # run models and tests
    python scripts/run_dbt.py ls --resource-type source
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = REPO_ROOT / "dbt"


def unquote(value: str) -> str:
    """Drop one matching pair of surrounding quotes, nothing else."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def load_dotenv(path: Path) -> None:
    """Same rules as the pipeline scripts: KEY=value lines, real env vars win.

    A missing .env is fine when the variables come from the environment
    (Airflow, CI, containers); dbt reports any that are still missing.
    """
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), unquote(value.strip()))


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")
    # Always this repo's project and profile, even if the shell has others set.
    os.environ["DBT_PROFILES_DIR"] = str(DBT_DIR)
    os.environ["DBT_PROJECT_DIR"] = str(DBT_DIR)

    from dbt.cli.main import dbtRunner  # imported late: dbt is slow to import

    result = dbtRunner().invoke(sys.argv[1:] or ["--help"])
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
