import joblib
import pandas as pd
from omegaconf import DictConfig
from prefect import flow, task
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier

from helper import load_config


@task
def load_data(config: DictConfig):
    data = {}
    names = ["X_train", "y_train", "X_valid", "y_valid"]
    for name in names:
        save_path = config.data.processed + name + ".csv"
        data[name] = pd.read_csv(save_path)
    return data


@task
def train_model(config: DictConfig, data: dict):
    params = dict(config.params)
    clf = XGBClassifier(**params)
    clf.fit(data["X_train"], data["y_train"])
    return clf


@task
def get_prediction(data: dict, model: XGBClassifier):
    return model.predict(data["X_valid"])


@task
def evaluate_model(data: dict, prediction: pd.DataFrame):
    score = accuracy_score(data["y_valid"], prediction)
    print(f"The accuracy score is {score}")


@task
def save_model(config: DictConfig, model: XGBClassifier):
    joblib.dump(model, config.model.save_path)


@flow
def train():
    config = load_config()
    data = load_data(config)
    clf = train_model(config, data)
    predictions = get_prediction(data, clf)
    evaluate_model(data, predictions)
    save_model(config, clf)


if __name__ == "__main__":
    train()
