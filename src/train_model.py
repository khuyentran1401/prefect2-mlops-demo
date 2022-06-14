import hydra
import joblib
import pandas as pd
from hydra.utils import to_absolute_path as abspath
from omegaconf import DictConfig
from prefect import flow, get_run_logger, task
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier


@task
def load_data(save_dir: str):
    data = {}
    names = ["X_train", "y_train", "X_valid", "y_valid"]
    for name in names:
        save_path = abspath(save_dir + name + ".csv")
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
    score = accuracy_score(y_valid, prediction)
    logger = get_run_logger()
    logger.info(f"Model accuracy score is {score:.4f}")


@task
def save_model(save_path: str, model: XGBClassifier):
    joblib.dump(model, abspath(save_path))


@hydra.main(
    config_path="../config", config_name="train_model", version_base=None
)
@flow
def train(config):
    data = load_data(config.data.processed).result()
    clf = train_model(config.params, data["X_train"], data["y_train"])
    predictions = get_prediction(data["X_valid"], clf)
    score = evaluate_model(data["y_valid"], predictions)
    save_model(config.model.save_path, clf)


if __name__ == "__main__":
    train()
