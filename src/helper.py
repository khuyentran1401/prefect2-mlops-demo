import yaml
from box import Box
from prefect import task


@task
def load_config():
    with open("../config/main.yaml") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    box_config = Box(config)
    return box_config


@task
def is_suite_passed(result):
    assert result.passed()


@task
def always_passed():
    return True
