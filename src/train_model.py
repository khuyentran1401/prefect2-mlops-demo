import joblib
import pandas as pd
from omegaconf import DictConfig
from prefect import flow, task
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
import yaml 
from box import Box 
from prefect.tasks import task_input_hash
from datetime import timedelta

@task
def load_config():
    with open('../config/main.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    box_config = Box(config)
    return box_config


@task
def load_data(config: DictConfig):
    data = {}
    names = ["train", "test"]
    for name in names:
        save_path = config.data.processed + name + ".csv"
        data[name] = pd.read_csv(save_path)
    return data


@task
def split_train(data: dict):
    train = data['train']
    X_train = train.drop(columns=["AdoptionSpeed"])
    y_train = train["AdoptionSpeed"]
    X_train, X_valid, y_train, y_valid = train_test_split(X_train, y_train, test_size=0.2, random_state=0)

    return {
        "X_train": X_train,
        "X_valid": X_valid,
        "y_train": y_train,
        "y_valid": y_valid,
    }


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

@task
def save_data(data: dict, config):

    for name, value in data.items():
        save_path = config.data.final + name + ".csv"
        value.to_csv(save_path, index=False)

@flow
def train():
    config = load_config()
    data = load_data(config)
    train_set = split_train(data)
    clf = train_model(config, train_set)
    predictions = get_prediction(train_set, clf)
    evaluate_model(train_set, predictions)
    save_model(config, clf)
    save_data(train_set, config)


if __name__ == "__main__":
    train()
