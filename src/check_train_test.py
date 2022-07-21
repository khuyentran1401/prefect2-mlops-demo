import pandas as pd
from deepchecks.tabular import Dataset
from omegaconf import DictConfig
from prefect import flow, task

from helper import load_config
from deepchecks.tabular.suites import train_test_validation


@task 
def load_train_test(config):
    data = {}
    names = ["X_train", "X_valid", "y_train", "y_valid"]
    for name in names:
        save_path = config.data.training + name + ".csv"
        data[name] = pd.read_csv(save_path)
    return data

def merge_X_y(X: pd.DataFrame, y: pd.DataFrame):
    return X.merge(y, left_index=True, right_index=True)


@task
def merge_X_y_all(data: dict):
    train_valid = {}
    train_valid["train"] = merge_X_y(data["X_train"], data["y_train"])
    train_valid["valid"] = merge_X_y(data["X_valid"], data["y_valid"])
    return train_valid


@task
def initialize_dataset(data: dict, config: DictConfig):
    cat_features = list(config.cat_cols)
    ds_train = Dataset(
        data["train"], label=config.label, cat_features=cat_features
    )
    ds_valid = Dataset(
        data["valid"], label=config.label, cat_features=cat_features
    )
    return {"train": ds_train, "valid": ds_valid}


@flow
def check_train_test():
    ...
