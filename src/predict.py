import joblib
import numpy as np
import pandas as pd
from prefect import flow
from sqlalchemy import create_engine
from xgboost import XGBClassifier

from helper import load_config
from process_data import process_data


def load_test(config):
    connection = config.connection
    engine = create_engine(
        f"postgresql://{connection.user}:{connection.password}@{connection.host}/{connection.database}",
    )
    query = f'SELECT * FROM "{config.data.test}"'
    return pd.read_sql(query, con=engine)


def load_model(config):
    return joblib.load(config.model.save_path)


def get_prediction(data: pd.DataFrame, model: XGBClassifier):
    return model.predict(data)


def save_prediction(predictions: np.ndarray, config):
    predictions = pd.Series(predictions)

    connection = config.connection
    engine = create_engine(
        f"postgresql://{connection.user}:{connection.password}@{connection.host}/{connection.database}",
    )

    predictions.to_sql(
        name=config.data.prediction,
        con=engine,
        if_exists="replace",
        index=False,
    )


@flow
def predict():
    config = load_config()
    test = load_test(config)
    processed = process_data(config, test)
    model = load_model(config)
    prediction = get_prediction(processed, model)
    save_prediction(prediction, config)


if __name__ == "__main__":
    predict()
