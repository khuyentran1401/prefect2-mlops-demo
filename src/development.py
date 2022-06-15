from prefect import flow

from get_data import get_data
from process_data import process_data
from train_model import train


@flow
def development():
    get_data()
    process_data()
    train()


if __name__ == "__main__":
    development()
