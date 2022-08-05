import pandas as pd
from omegaconf import DictConfig
from prefect import flow, task
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine

from helper import load_config

pd.options.mode.chained_assignment = None
# ---------------------------------------------------------------------------- #
#                                 Create tasks                                 #
# ---------------------------------------------------------------------------- #


@task(retries=3, retry_delay_seconds=5)
def read_new_data(config: DictConfig):
    connection = config.connection
    engine = create_engine(
        f"postgresql://{connection.user}:{connection.password}@{connection.host}/{connection.database}",
    )
    query = f'SELECT * FROM "{config.data.raw}"'
    df = pd.read_sql(query, con=engine)
    return df


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
def filter_cols(data: pd.DataFrame, config: DictConfig):
    use_cols = list(config.use_cols)
    try:
        return data[use_cols]
    except KeyError:
        use_cols.remove(config.label)
        return data[use_cols]


@task
def encode_cat_cols(data: pd.DataFrame, config: DictConfig):
    cat_cols = list(config.cat_cols)
    data[cat_cols] = data[cat_cols].astype(str)
    for col in cat_cols:
        _, indexer = pd.factorize(data[col])
        data[col] = indexer.get_indexer(data[col])
    return data


@task
def split_data(data: pd.DataFrame, config: DictConfig):
    X_train = data.drop(columns=[config.label])
    y_train = data[config.label]
    X_train, X_valid, y_train, y_valid = train_test_split(
        X_train, y_train, test_size=0.2, random_state=0
    )

    return {
        "X_train": X_train,
        "X_valid": X_valid,
        "y_train": y_train,
        "y_valid": y_valid,
    }


@task
def save_processed_data(data: dict, config: DictConfig):
    connection = config.connection
    engine = create_engine(
        f"postgresql://{connection.user}:{connection.password}@{connection.host}/{connection.database}",
    )

    for name, value in data.items():
        value.to_sql(name=name, con=engine, if_exists="replace", index=False)


@flow
def process_data(config, data):
    return (
        data.pipe(fill_missing_description)
        .pipe(get_desc_length)
        .pipe(get_desc_words)
        .pipe(get_average_word_length)
        .pipe(filter_cols, config=config)
        .pipe(encode_cat_cols, config=config)
    )


@flow
def prepare_for_training():
    config = load_config()
    data = read_new_data(config)
    processed = process_data(config, data)
    X_y = split_data(processed, config=config)
    save_processed_data(X_y, config)


# ---------------------------------------------------------------------------- #
#                                 Create a flow                                #
# ---------------------------------------------------------------------------- #


if __name__ == "__main__":
    prepare_for_training()
