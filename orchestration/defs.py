from __future__ import annotations
import sys
import subprocess
from pathlib import Path

from dagster import (
    Definitions,
    asset,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
    get_dagster_logger
)

from dagster_dbt import DbtCliResource

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]

DBT_PROJECT_DIR = REPO_ROOT / "dbt" / "ecom_dbt"
INGEST_SCRIPT = REPO_ROOT / "ingest" / "generate_and_load.py"

# ------------------------------------------------------------------------------
# Assets
# ------------------------------------------------------------------------------


from dagster import asset, get_dagster_logger
import subprocess
import sys

@asset(description="Generate synthetic data and load it into Postgres raw schema")
def raw_refresh():
    log = get_dagster_logger()

    cmd = [sys.executable, "-u", str(INGEST_SCRIPT)]  # -u = unbuffered output
    log.info(f"Running: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        cwd=str(INGEST_SCRIPT.parent),   # important if script uses relative paths
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,        # merge stderr into stdout so nothing is lost
        env=None,                        # inherit Dagster env
    )

    log.info(result.stdout or "")

    if result.returncode != 0:
        raise Exception(f"Ingest script failed with exit code {result.returncode}")

@asset(
    description="Run dbt build (models + tests) after raw_refresh",
    deps=[raw_refresh],
)
def dbt_build():
    subprocess.run(
        ["dbt", "build"],
        cwd=str(DBT_PROJECT_DIR),
        check=True,
    )

# ------------------------------------------------------------------------------
# Job + Schedule
# ------------------------------------------------------------------------------

daily_job = define_asset_job(
    name="daily_ecom_pipeline",
    selection=AssetSelection.assets(raw_refresh, dbt_build),
)

daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="*/2 * * * *",
)

# ------------------------------------------------------------------------------
# Definitions
# ------------------------------------------------------------------------------

defs = Definitions(
    assets=[raw_refresh, dbt_build],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
    schedules=[daily_schedule],
)