import joblib
import mlflow
import pandas as pd
from mlflow import log_metric, log_params
from omegaconf import DictConfig
from prefect import flow, task
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier

from helper import load_config


@task
def setup_mlflow():

    mlflow.set_tracking_uri("http://127.0.0.1:5000")
    mlflow.set_experiment("pet-model")


@task
def load_data(save_dir: str):
    data = {}
    names = ["X_train", "y_train", "X_valid", "y_valid"]
    for name in names:
        save_path = save_dir + name + ".csv"
        data[name] = pd.read_csv(save_path)

    return data


@task
def train_model(params: DictConfig, X_train: pd.DataFrame, y_train: pd.Series):
    params = dict(params)
    clf = XGBClassifier(**params)
    clf.fit(X_train, y_train)
    return clf


@task
def get_prediction(data: pd.DataFrame, model: XGBClassifier):
    return model.predict(data)


@task
def evaluate_model(y_valid: pd.DataFrame, prediction: pd.DataFrame):
    return accuracy_score(y_valid, prediction)


@task
def save_model(save_path: str, model: XGBClassifier):
    joblib.dump(model, save_path)


@task
def log_w_mlflow(score: int, params: DictConfig, model: XGBClassifier):
    log_metric("accuracy_score", score)
    log_params(dict(params))


@flow
def train():
    config = load_config().result()
    setup_mlflow()
    data = load_data(config.data.processed).result()
    clf = train_model(config.params, data["X_train"], data["y_train"])
    predictions = get_prediction(data["X_valid"], clf)
    score = evaluate_model(data["y_valid"], predictions)
    log_w_mlflow(score, config.params, clf)
    save_model(config.model.save_path, clf)


if __name__ == "__main__":
    train()
