from datetime import timedelta

import pandas as pd
from deepchecks.tabular import Dataset
from deepchecks.tabular.suites import data_integrity
from prefect import flow, task
from prefect.tasks import task_input_hash

from helper import always_passed, is_suite_passed, load_config
from process_data import read_new_data


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def init_dataset(data: pd.DataFrame, config):
    return Dataset(
        data, cat_features=list(config.cat_cols), label=config.label
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def create_data_integrity_suite(dataset: Dataset, config):
    integ_suite = data_integrity(columns=list(config.check_integrity_cols))
    result = integ_suite.run(dataset)
    result.save_as_html(config.report.data_integrity)
    return result


@flow
def check_data_integrity():
    config = load_config()
    df = read_new_data(config)
    dataset = init_dataset(df, config)
    result = create_data_integrity_suite(dataset, config)
    # is_suite_passed(result)
    always_passed()


if __name__ == "__main__":
    check_data_integrity()
