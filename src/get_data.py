import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine

from helper import load_config


@task(retries=3)
def read_data(connection, database: str):
    engine = create_engine(
        f"postgresql://{connection.user}:{connection.password}@{connection.host}/{connection.database}",
    )
    query = f"SELECT * FROM {database}"
    df = pd.read_sql(query, con=engine)
    return df


@task
def save_data(df: pd.DataFrame, save_path: str):
    df.to_csv(save_path)


@flow
def get_data():
    config = load_config().result()
    df = read_data(config.connection, config.data.raw.name)
    save_data(df, config.data.raw.path)


if __name__ == "__main__":
    get_data()
