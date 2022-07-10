import joblib
import numpy as np
import pandas as pd
from omegaconf import DictConfig
from prefect import flow, task
from xgboost import XGBClassifier

from helper import load_config, load_model


@task
def load_test(config: DictConfig):
    save_path = config.data.processed + "X_test.csv"
    return pd.read_csv(save_path)


@task
def get_prediction(data: pd.DataFrame, model: XGBClassifier):
    return model.predict(data)


@task
def save_prediction(predictions: np.ndarray, config: DictConfig):
    predictions = pd.Series(predictions)
    predictions.to_csv(config.data.final, index=False)


@flow
def predict():
    config = load_config().result()
    test = load_test(config)
    model = load_model(config)
    prediction = get_prediction(test, model)
    save_prediction(prediction, config)


if __name__ == "__main__":
    predict()
