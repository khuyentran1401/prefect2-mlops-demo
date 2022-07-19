import joblib
import pandas as pd
from prefect import flow
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier
import yaml 
from box import Box 

def load_config():
    with open('../config/main.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    box_config = Box(config)
    return box_config


def load_data(config):
    data = {}
    names = ["X_train", "X_valid", "y_train", "y_valid"]
    for name in names:
        save_path = config.data.training + name + ".csv"
        data[name] = pd.read_csv(save_path)
    return data

def train_model(config, data: dict):
    params = dict(config.params)
    clf = XGBClassifier(**params)
    clf.fit(data["X_train"], data["y_train"])
    return clf

def get_prediction(data: dict, model: XGBClassifier):
    return model.predict(data["X_valid"])

def evaluate_model(data: dict, prediction: pd.DataFrame):
    score = accuracy_score(data["y_valid"], prediction)
    print(f"The accuracy score is {score}")

def save_model(config, model: XGBClassifier):
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
