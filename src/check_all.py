import pandas as pd
from deepchecks.tabular import Dataset
from deepchecks.tabular.suites import full_suite
from omegaconf import DictConfig
from prefect import flow, task
from xgboost import XGBClassifier

from helper import load_config, load_model, load_train_test


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


@task
def create_full_suite(dataset: dict, model: XGBClassifier, config: DictConfig):
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
    data = load_train_test(config)
    model = load_model(config)
    train_valid = merge_X_y_all(data)
    dataset = initialize_dataset(train_valid, config)
    create_full_suite(dataset, model, config)


if __name__ == "__main__":
    check()
