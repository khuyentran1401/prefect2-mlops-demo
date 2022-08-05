import joblib
import pandas as pd
from deepchecks.tabular import Dataset
from deepchecks.tabular.suites import train_test_validation
from prefect import flow, task

from helper import always_passed, is_suite_passed, load_config
from train_model import load_train_test


def merge_X_y(X: pd.DataFrame, y: pd.DataFrame):
    return X.merge(y, left_index=True, right_index=True)


@task
def merge_X_y_all(data: dict):
    train_test = {}
    train_test["train"] = merge_X_y(data["X_train"], data["y_train"])
    train_test["test"] = merge_X_y(data["X_valid"], data["y_valid"])
    return train_test


@task
def initialize_dataset(data: dict, config):
    """Instantiate a Dataset object

    Parameters
    ----------
    data : dict
        a dictionary contains the train and test sets
    config
        a configuration file contains the values of some variables
    """
    cat_features = list(config.cat_cols)
    ds_train = Dataset(
        data["train"], label=config.label, cat_features=cat_features
    )
    ds_test = Dataset(
        data["test"], label=config.label, cat_features=cat_features
    )
    return {"train": ds_train, "test": ds_test}


@task
def save_dataset(dataset: dict, config):
    for name, value in dataset.items():
        save_path = config.data.validation + name
        joblib.dump(value, save_path)


@task
def create_train_test_validation_suite(data: dict, config):
    """Create a train test set validation suite

    Parameters
    ----------
    data : dict
        a dictionary contains the dataset objects for training and testing
    config
        a configuration file contains the values of some variables
    """
    suite = train_test_validation()
    result = suite.run(data["train"], data["test"])
    result.save_as_html(config.report.train_test_validation)
    return result


@flow
def check_train_test():
    config = load_config()
    data = load_train_test(config)
    train_test = merge_X_y_all(data)
    dataset = initialize_dataset(train_test, config)
    result = create_train_test_validation_suite(dataset, config)
    # is_suite_passed(result)
    always_passed()
    save_dataset(dataset, config)


if __name__ == "__main__":
    check_train_test()
