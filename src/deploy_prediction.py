import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from prefect.deployments import Deployment, FlowScript
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.packaging import OrionPackager
from prefect.packaging.serializers import PickleSerializer

FLOW_DIR = (Path(__file__).parent).resolve()
sys.path.append(FLOW_DIR.as_posix())

Deployment(
    name="pet-flow-production",
    packager=OrionPackager(serializer=PickleSerializer()),
    flow=FlowScript(path=FLOW_DIR / "predict.py", name="predict"),
    schedule=IntervalSchedule(
        interval=timedelta(days=30),
        anchor_date=pendulum.datetime(
            2022, 6, 15, 11, 0, 0, tz="America/Chicago"
        ),
    ),
    tags=["prod"],
)
