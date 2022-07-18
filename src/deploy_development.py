from datetime import timedelta
from pathlib import Path

import pendulum
from prefect.deployments import Deployment, FlowScript
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.packaging import OrionPackager
from prefect.packaging.serializers import PickleSerializer

Deployment(
    name="pet-flow-dev",
    flow=FlowScript(
        path=Path(__file__).parent / "development.py", name="development"
    ),
    schedule=IntervalSchedule(
        interval=timedelta(days=1),
        anchor_date=pendulum.datetime(
            2022, 7, 11, 13, 58, 0, tz="America/Chicago"
        ),
    ),
    packager=OrionPackager(serializer=PickleSerializer()),
    tags=["dev"],
)
