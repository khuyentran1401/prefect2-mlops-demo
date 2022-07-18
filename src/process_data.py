import pandas as pd
from prefect import flow, task
from helper import load_config

pd.options.mode.chained_assignment = None
# ---------------------------------------------------------------------------- #
#                                 Create tasks                                 #
# ---------------------------------------------------------------------------- #


@task
def get_data(config, data_name: str):
    if data_name == 'train':
        return pd.read_csv(config.data.raw.train)
    else:
        return pd.read_csv(config.data.raw.test)


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
def filter_cols(config, data: pd.DataFrame, data_name: str):
    use_cols = config.use_cols
    if data_name == 'test':
        use_cols.remove('AdoptionSpeed')
    return data[config.use_cols]


@task
def encode_cat_cols(config, data: pd.DataFrame):
    cat_cols = list(config.cat_cols)
    data[cat_cols] = data[cat_cols].astype(str)
    for col in cat_cols:
        _, indexer = pd.factorize(data[col])
        data[col] = indexer.get_indexer(data[col])
    return data


@task
def save_data(data: dict, config):

    for name, value in data.items():
        save_path = config.data.processed + name + ".csv"
        value.to_csv(save_path, index=False)

@flow 
def process_data():
    config = load_config()
    train_test = {}
    for data_name in ['train', 'test']:
        data = get_data(config, data_name)
        processed = get_description_features(data)
        filtered = filter_cols(config, processed, data_name)
        encoded = encode_cat_cols(config, filtered)
        train_test[data_name] = encoded 
    save_data(train_test, config)

# ---------------------------------------------------------------------------- #
#                                 Create a flow                                #
# ---------------------------------------------------------------------------- #


if __name__ == "__main__":
    process_data()
