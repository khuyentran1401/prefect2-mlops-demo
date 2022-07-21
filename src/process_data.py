import pandas as pd
from prefect import flow, task
from helper import load_config
from sklearn.model_selection import train_test_split

pd.options.mode.chained_assignment = None
# ---------------------------------------------------------------------------- #
#                                 Create tasks                                 #
# ---------------------------------------------------------------------------- #


@task
def load_new_data(config):
    return pd.read_csv(config.data.raw.new)


@task
def fill_missing_description(data: pd.DataFrame):
    data["Description"] = data["Description"].fillna("")
    return data

@task
def get_desc_length(data: pd.DataFrame):
    data["desc_length"] = data["Description"].str.len()
    return data

@task
def get_desc_words(data: pd.DataFrame):
    data["desc_words"] = data["Description"].apply(lambda x: len(x.split()))
    return data

@task
def get_average_word_length(data: pd.DataFrame):
    data["average_word_length"] = data["desc_length"] / data["desc_words"]
    return data


@task
def filter_cols(data: pd.DataFrame, config):
    return data[config.use_cols]

@task
def encode_cat_cols(data: pd.DataFrame, config):
    cat_cols = list(config.cat_cols)
    data[cat_cols] = data[cat_cols].astype(str)
    for col in cat_cols:
        _, indexer = pd.factorize(data[col])
        data[col] = indexer.get_indexer(data[col])
    return data

@task 
def split_data(data: pd.DataFrame, config):
    X_train = data.drop(columns=[config.label])
    y_train = data[config.label]
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
        save_path = config.data.processed + name + ".csv"
        value.to_csv(save_path, index=False)

@flow 
def process_data():
    config = load_config()
    data = load_new_data(config)
    processed = (
        data.pipe(fill_missing_description)
        .pipe(get_desc_length)
        .pipe(get_desc_words)
        .pipe(get_average_word_length)
        .pipe(filter_cols, config=config)
        .pipe(encode_cat_cols, config=config)
        .pipe(split_data, config=config)
    )
    save_data(processed, config)

# ---------------------------------------------------------------------------- #
#                                 Create a flow                                #
# ---------------------------------------------------------------------------- #


if __name__ == "__main__":
    process_data()
