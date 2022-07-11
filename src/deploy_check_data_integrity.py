import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from prefect.deployments import Deployment, FlowScript
from prefect.orion.schemas.schedules import IntervalSchedule

FLOW_DIR = (Path(__file__).parent).resolve()
sys.path.append(FLOW_DIR.as_posix())

Deployment(
    flow=FlowScript(path=FLOW_DIR / "check_data_integrity.py"),
    schedule=IntervalSchedule(
        interval=timedelta(days=1),
        anchor_date=pendulum.datetime(
            2022, 7, 11, 11, 31, 0, tz="America/Chicago"
        ),
    ),
    tags=["dev"],
)
