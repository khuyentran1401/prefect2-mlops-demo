from datetime import timedelta

import pandas as pd
from deepchecks.tabular import Dataset
from deepchecks.tabular.suites import data_integrity
from omegaconf import DictConfig
from prefect import flow, task
from prefect.tasks import task_input_hash

from helper import load_config, load_raw_data


@task
def init_dataset(data: pd.DataFrame, config: DictConfig):
    return Dataset(
        data, cat_features=list(config.cat_cols), label=config.label
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def create_data_integrity_suite(dataset: Dataset, config: DictConfig):
    integ_suite = data_integrity()
    result = integ_suite.run(dataset)
    result.save_as_html(config.report.data_integrity)
    return result


@task
def test_data_integrity(result):
    assert result.passed()


@flow
def check_data_integrity():
    config = load_config()
    df = load_raw_data(config)
    dataset = init_dataset(df, config)
    result = create_data_integrity_suite(dataset, config)
    test_data_integrity(result)


if __name__ == "__main__":
    check_data_integrity()
