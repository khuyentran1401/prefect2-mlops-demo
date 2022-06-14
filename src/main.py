import hydra
from prefect import flow

from get_data import get_data
from predict import predict
from process_data import process_data
from train_model import train


@hydra.main(config_path="../config", config_name="main", version_base=None)
@flow
def main_flow(config):
    get_data(config)
    process_data(config)
    train(config)
    predict(config)


if __name__ == "__main__":
    main_flow()
