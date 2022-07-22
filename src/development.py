from prefect import flow

from check_data_integrity import check_data_integrity
from check_train_test import check_train_test
from evaluate_model import evaluate_model
from process_data import process_data
from train_model import train


@flow
def development():
    check_data_integrity()
    process_data()
    check_train_test()
    train()
    evaluate_model()


if __name__ == "__main__":
    development()
