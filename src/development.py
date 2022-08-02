from get_data import get_data
from prefect import flow

from process_data import prepare_for_training
from train_model import train


@flow
def development():
    get_data()
    prepare_for_training()
    train()


if __name__ == "__main__":
    development()
