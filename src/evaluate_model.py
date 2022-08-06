import joblib
import pandas as pd
from deepchecks.tabular.suites import model_evaluation
from prefect import flow, task

from helper import always_passed, is_suite_passed, load_config


@task
def load_dataset(config):
    datasets = {}
    for name in ["train", "test"]:
        save_path = config.data.validation + name
        datasets[name] = joblib.load(save_path)
    return datasets


@task
def load_model(config):
    return joblib.load(config.model.save_path)


@task
def create_model_evaluation_suite(model, datasets: dict, config):
    suite = model_evaluation()
    result = suite.run(
        train_dataset=datasets["train"],
        test_dataset=datasets["test"],
        model=model,
    )
    result.save_as_html(config.report.model_evaluation)
    return result


@flow
def evaluate_model():
    config = load_config()
    datasets = load_dataset(config)
    model = load_model(config)
    result = create_model_evaluation_suite(model, datasets, config)
    is_suite_passed(result)


if __name__ == "__main__":
    evaluate_model()
