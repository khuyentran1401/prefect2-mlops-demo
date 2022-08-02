from datetime import timedelta

import joblib
import pandas as pd
from omegaconf import DictConfig
from prefect import flow, task
from prefect.tasks import task_input_hash
from sklearn.metrics import accuracy_score
from sqlalchemy import create_engine
from xgboost import XGBClassifier

from helper import load_config


@task(retries=3, retry_delay_seconds=5)
def load_data(config: DictConfig):
    connection = config.connection
    engine = create_engine(
        f"postgresql://{connection.user}:{connection.password}@{connection.host}/{connection.database}",
    )
    data = {}
    names = ["X_train", "y_train", "X_valid", "y_valid"]
    for name in names:
        query = f'SELECT * FROM "{name}"'
        data[name] = pd.read_sql(query, con=engine)
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
    joblib.dump(model, config.model.dir + config.model.save_path)


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
