from datetime import timedelta

import pendulum
from prefect.deployments import DeploymentSpec
from prefect.flow_runners import SubprocessFlowRunner
from prefect.orion.schemas.schedules import IntervalSchedule

DeploymentSpec(
    name="pet-flow-dev",
    flow_location="./development.py",
    flow_name="development",
    schedule=IntervalSchedule(
        interval=timedelta(days=1),
        anchor_date=pendulum.datetime(
            2022, 6, 15, 10, 29, 0, tz="America/Chicago"
        ),
    ),
    flow_runner=SubprocessFlowRunner(),
    tags=["dev"],
)
