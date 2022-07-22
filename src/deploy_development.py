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
    flow=FlowScript(path=FLOW_DIR / "development.py", name="development"),
    packager=OrionPackager(serializer=PickleSerializer()),
    schedule=IntervalSchedule(
        interval=timedelta(days=1),
        anchor_date=pendulum.datetime(
            2022, 7, 21, 17, 0, 0, tz="America/Chicago"
        ),
    ),
    tags=["dev"],
)
