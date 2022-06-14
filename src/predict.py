import hydra
import joblib
import pandas as pd
from hydra.utils import to_absolute_path as abspath
from prefect import flow, task
from xgboost import XGBClassifier


@task
def load_test(save_dir: str):
    save_path = abspath(save_dir + "X_test.csv")
    return pd.read_csv(save_path)


@task
def load_model(save_path: str):
    return joblib.load(abspath(save_path))


@task
def get_prediction(data: pd.DataFrame, model: XGBClassifier):
    return model.predict(data)


@hydra.main(
    config_path="../config", config_name="train_model", version_base=None
)
@flow
def predict(config):
    test = load_test(config.data.processed)
    model = load_model(config.model.save_path)
    prediction = get_prediction(test, model)


if __name__ == "__main__":
    predict()
