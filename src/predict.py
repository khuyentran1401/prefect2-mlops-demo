import joblib
import numpy as np
import pandas as pd
from prefect import flow, task
from xgboost import XGBClassifier

from helper import load_config
from process_data import process_data


@task
def load_test(config):
    save_path = config.data.final + "test.csv"
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
    predictions.to_csv(config.data.final + "prediction.csv", index=False)


@flow
def predict():
    config = load_config()
    test = load_test(config)
    processed = process_data(config, test)
    model = load_model(config)
    prediction = get_prediction(processed, model)
    save_prediction(prediction, config)


if __name__ == "__main__":
    predict()
