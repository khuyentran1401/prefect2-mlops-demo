import joblib
import numpy as np
import pandas as pd
from prefect import flow, task
from xgboost import XGBClassifier

from helper import load_config


@task
def load_test(save_dir: str):
    save_path = save_dir + "X_test.csv"
    return pd.read_csv(save_path)


@task
def load_model(save_path: str):
    return joblib.load(save_path)


@task
def get_prediction(data: pd.DataFrame, model: XGBClassifier):
    return model.predict(data)


@task
def save_prediction(predictions: np.ndarray, save_path: str):
    predictions = pd.Series(predictions)
    predictions.to_csv(save_path, index=False)


@flow
def predict():
    config = load_config().result()
    test = load_test(config.data.processed)
    model = load_model(config.model.save_path)
    prediction = get_prediction(test, model)
    save_prediction(prediction, config.data.final)


if __name__ == "__main__":
    predict()
