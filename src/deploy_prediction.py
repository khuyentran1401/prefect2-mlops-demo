from datetime import timedelta
from pathlib import Path

import pendulum
from prefect.deployments import Deployment, FlowScript
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.packaging import OrionPackager
from prefect.packaging.serializers import PickleSerializer

Deployment(
    name="pet-flow-production",
    flow=FlowScript(path=Path(__file__).parent / "predict.py", name="predict"),
    schedule=IntervalSchedule(
        interval=timedelta(days=30),
        anchor_date=pendulum.datetime(
            2022, 7, 11, 13, 56, 0, tz="America/Chicago"
        ),
    ),
    packager=OrionPackager(serializer=PickleSerializer()),
    tags=["prod"],
)
