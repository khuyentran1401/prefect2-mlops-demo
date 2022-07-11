import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from prefect.deployments import Deployment, FlowScript
from prefect.orion.schemas.schedules import IntervalSchedule

FLOW_DIR = (Path(__file__).parent).resolve()
sys.path.append(FLOW_DIR.as_posix())

Deployment(
    name="pet-flow-dev",
    flow=FlowScript(path=FLOW_DIR / "development.py", name="development"),
    schedule=IntervalSchedule(
        interval=timedelta(days=1),
        anchor_date=pendulum.datetime(
            2022, 7, 11, 10, 40, 0, tz="America/Chicago"
        ),
    ),
    tags=["dev"],
)
