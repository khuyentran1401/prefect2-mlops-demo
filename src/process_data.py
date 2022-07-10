import pandas as pd
from omegaconf import DictConfig
from prefect import flow, task
from sklearn.model_selection import train_test_split

from helper import load_config, load_raw_data

pd.options.mode.chained_assignment = None
# ---------------------------------------------------------------------------- #
#                                 Create tasks                                 #
# ---------------------------------------------------------------------------- #


def fill_missing_description(data: pd.DataFrame):
    data["Description"] = data["Description"].fillna("")
    return data


def get_desc_length(data: pd.DataFrame):
    data["desc_length"] = data["Description"].str.len()
    return data


def get_desc_words(data: pd.DataFrame):
    data["desc_words"] = data["Description"].apply(lambda x: len(x.split()))
    return data


def get_average_word_length(data: pd.DataFrame):
    data["average_word_length"] = data["desc_length"] / data["desc_words"]
    return data


@task
def get_description_features(data: pd.DataFrame):
    return (
        data.pipe(fill_missing_description)
        .pipe(get_desc_length)
        .pipe(get_desc_words)
        .pipe(get_average_word_length)
    )


@task
def filter_cols(config: DictConfig, data: pd.DataFrame):
    return data[config.use_cols]


@task
def encode_cat_cols(config: DictConfig, data: pd.DataFrame):
    cat_cols = list(config.cat_cols)
    data[cat_cols] = data[cat_cols].astype(str)
    for col in cat_cols:
        _, indexer = pd.factorize(data[col])
        data[col] = indexer.get_indexer(data[col])
    return data


@task
def get_train_test(data: pd.DataFrame, config: DictConfig):
    train = data.dropna(subset=[config.label])
    test = data[data[config.label].isna()]
    return {"train": train, "test": test}


def split_X_y_train(train: pd.DataFrame, label: str):
    X_train = train.drop(columns=[label])
    y_train = train[label]
    return X_train, y_train


@task
def train_test_validation_split(data: dict, config: DictConfig):
    X_train, y_train = split_X_y_train(data["train"], config.label)
    X_train, X_valid, y_train, y_valid = train_test_split(
        X_train, y_train, test_size=0.2, random_state=0
    )
    return {
        "X_train": X_train,
        "X_test": data["test"],
        "X_valid": X_valid,
        "y_train": y_train,
        "y_valid": y_valid,
    }


@task
def save_data(data: dict, config: DictConfig):

    for name, value in data.items():
        save_path = config.data.processed + name + ".csv"
        value.to_csv(save_path, index=False)


@flow
def process_data():
    config = load_config()
    data = load_raw_data(config)
    processed = get_description_features(data)
    filtered = filter_cols(config, processed)
    encoded = encode_cat_cols(config, filtered)
    train_test_data = get_train_test(encoded, config)
    split = train_test_validation_split(train_test_data, config)
    save_data(split, config)


# ---------------------------------------------------------------------------- #
#                                 Create a flow                                #
# ---------------------------------------------------------------------------- #


if __name__ == "__main__":
    process_data()
