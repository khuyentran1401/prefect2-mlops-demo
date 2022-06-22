from datetime import timedelta

import pendulum
from prefect.deployments import DeploymentSpec
from prefect.flow_runners import SubprocessFlowRunner
from prefect.orion.schemas.schedules import IntervalSchedule

DeploymentSpec(
    name="pet-flow-production",
    flow_location="./predict.py",
    flow_name="predict",
    schedule=IntervalSchedule(
        interval=timedelta(days=30),
        anchor_date=pendulum.datetime(
            2022, 6, 15, 11, 0, 0, tz="America/Chicago"
        ),
        timezone="America/Chicago",
    ),
    flow_runner=SubprocessFlowRunner(),
    tags=["prod"],
)
