from datetime import timedelta

import pandas as pd
from deepchecks.tabular import Dataset
from deepchecks.tabular.suites import data_integrity
from prefect import flow, task
from prefect.tasks import task_input_hash
from deepchecks.tabular import Suite
from deepchecks.tabular.checks import *

from helper import load_config


@task
def load_new_data(config):
    return pd.read_csv(config.data.raw.new)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def init_dataset(data: pd.DataFrame, config):
    return Dataset(
        data, cat_features=list(config.cat_cols), label=config.label
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def create_data_integrity_suite(dataset: Dataset, config):
    integ_suite = Suite("Suite for evaluating data integrity",
        FeatureLabelCorrelation(),
        IsSingleValue(columns=[config.check_integrity_cols]),
        DataDuplicates(columns=[config.check_integrity_cols]),
        ConflictingLabels(columns=[config.check_integrity_cols])
    )
    result = integ_suite.run(dataset)
    result.save_as_html(config.report.data_integrity)
    return result


@task
def test_data_integrity(result):
    assert result.passed()

@task
def always_succeeds_task():
    return "foo"


@flow
def check_data_integrity():
    config = load_config()
    df = load_new_data(config)
    dataset = init_dataset(df, config)
    # result = create_data_integrity_suite(dataset, config)
    # test_data_integrity(result)
    always_succeeds_task()


if __name__ == "__main__":
    check_data_integrity()
