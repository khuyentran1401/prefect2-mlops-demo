from datetime import timedelta
from pathlib import Path

import pendulum
from prefect.deployments import Deployment, FlowScript
from prefect.orion.schemas.schedules import IntervalSchedule

Deployment(
    flow=FlowScript(
        path=Path(__file__).parent / "train_model.py"
    ),
    schedule=IntervalSchedule(
        interval=timedelta(days=1),
        anchor_date=pendulum.datetime(
            2022, 7, 11, 16, 5, 0, tz="America/Chicago"
        ),
    ),
    tags=["dev"],
)
