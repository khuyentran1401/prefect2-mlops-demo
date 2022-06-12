import hydra
import pandas as pd
from hydra.utils import to_absolute_path as abspath
from omegaconf import DictConfig
from prefect import flow, task
from sklearn.model_selection import StratifiedKFold
from xgboost import XGBClassifier
import joblib 

@task
def load_data(save_dir: str):
    data = {}
    names = ["X_train", "X_test", "y_train"]
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
def save_model(save_path: str, model: XGBClassifier):
    joblib.dump(model, abspath(save_path))


@hydra.main(config_path="../config", config_name="train_model", version_base=None)
@flow
def train(config):
    data = load_data(config.data.processed).result()
    clf = train_model(config.params, data["X_train"], data["y_train"])
    save_model(config.model.save_path, clf)


if __name__ == "__main__":
    train()
