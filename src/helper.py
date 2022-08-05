from hydra import compose, initialize
from prefect import task


@task
def load_config():
    """Load configurations from the file `main.yaml` under the `config` directory"""
    with initialize(version_base=None, config_path="../config"):
        config = compose(config_name="main")
    return config


@task
def is_suite_passed(result):
    assert result.passed()


@task
def always_passed():
    return True
