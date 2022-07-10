import joblib
import pandas as pd
from deepchecks.tabular import Dataset
from deepchecks.tabular.suites import full_suite
from omegaconf import DictConfig
from prefect import flow, task
from xgboost import XGBClassifier

from helper import load_config


@task
def load_data(config: DictConfig):
    data = {}
    names = ["X_train", "y_train", "X_valid", "y_valid"]
    for name in names:
        save_path = config.data.processed + name + ".csv"
        data[name] = pd.read_csv(save_path)
    return data


@task
def load_model(config: DictConfig):
    return joblib.load(config.model.save_path)


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
        data["train"], label="AdoptionSpeed", cat_features=cat_features
    )
    ds_valid = Dataset(
        data["valid"], label="AdoptionSpeed", cat_features=cat_features
    )
    return {"train": ds_train, "valid": ds_valid}


@task
def check_train_test(dataset: dict, model: XGBClassifier, config: DictConfig):
    suite = full_suite()
    result = suite.run(
        train_dataset=dataset["train"],
        test_dataset=dataset["valid"],
        model=model,
    )
    result.save_as_html(config.report.full_suite)


@flow
def check():
    config = load_config()
    data = load_data(config)
    model = load_model(config)
    train_valid = merge_X_y_all(data)
    dataset = initialize_dataset(train_valid, config)
    check_train_test(dataset, model, config)


if __name__ == "__main__":
    check()
