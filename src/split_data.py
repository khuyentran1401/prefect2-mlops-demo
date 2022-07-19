import pandas as pd
from prefect import flow, task
from sklearn.model_selection import train_test_split
from helper import load_config

@task 
def load_data(config):
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
def save_data(data: dict, config):

    for name, value in data.items():
        save_path = config.data.training + name + ".csv"
        value.to_csv(save_path, index=False)

@flow
def split_data():
    config = load_config()
    data = load_data(config)
    train_set = split_train(data)
    save_data(train_set, config)

if __name__ == "__main__":
    split_data()
