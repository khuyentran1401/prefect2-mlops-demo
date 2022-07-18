from prefect import flow

from process_data import process_data
from train_model import train


@flow
def development():
    process_data()
    train()


if __name__ == "__main__":
    development()
