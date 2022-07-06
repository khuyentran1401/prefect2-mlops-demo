import pandas as pd
from omegaconf import DictConfig
from prefect import flow, task
from sqlalchemy import create_engine

from helper import load_config


@task
def read_data(config: DictConfig):
    connection = config.connection
    engine = create_engine(
        f"postgresql://{connection.user}:{connection.password}@{connection.host}/{connection.database}",
    )
    query = f"SELECT * FROM {config.data.raw.name}"
    df = pd.read_sql(query, con=engine)
    return df


@task
def save_data(df: pd.DataFrame, config: DictConfig):
    df.to_csv(config.data.raw.path)


@flow
def get_data():
    config = load_config()
    df = read_data(config)
    save_data(df, config)


if __name__ == "__main__":
    get_data()
