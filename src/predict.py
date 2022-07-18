import joblib
import numpy as np
import pandas as pd
from prefect import flow, task
from xgboost import XGBClassifier

from helper import load_config


@task
def load_test(config):
    save_path = config.data.processed + "test.csv"
    return pd.read_csv(save_path)


@task
def load_model(config):
    return joblib.load(config.model.save_path)


@task
def get_prediction(data: pd.DataFrame, model: XGBClassifier):
    return model.predict(data)


@task
def save_prediction(predictions: np.ndarray, config):
    predictions = pd.Series(predictions)
    predictions.to_csv(config.data.prediction, index=False)


@flow
def predict():
    config = load_config().result()
    test = load_test(config)
    model = load_model(config)
    prediction = get_prediction(test, model)
    save_prediction(prediction, config)


if __name__ == "__main__":
    predict()
