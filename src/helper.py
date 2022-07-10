import joblib
import pandas as pd
from hydra import compose, initialize
from omegaconf import DictConfig
from prefect import task


@task
def load_config():
    with initialize(version_base=None, config_path="../config"):
        config = compose(config_name="main")
    return config


@task
def load_raw_data(config: DictConfig):
    return pd.read_csv(config.data.raw.path, index_col=0)


@task
def load_train_test(config: DictConfig):
    data = {}
    names = ["X_train", "y_train", "X_valid", "y_valid"]
    for name in names:
        save_path = config.data.processed + name + ".csv"
        data[name] = pd.read_csv(save_path)
    return data


@task
def load_model(config: DictConfig):
    return joblib.load(config.model.save_path)
