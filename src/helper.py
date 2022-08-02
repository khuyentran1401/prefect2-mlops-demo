import pandas as pd
from hydra import compose, initialize
from omegaconf import DictConfig
from prefect import task
from sqlalchemy import create_engine


@task
def load_config():
    with initialize(version_base=None, config_path="../config"):
        config = compose(config_name="main")
    return config
