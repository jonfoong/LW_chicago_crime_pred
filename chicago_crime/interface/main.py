from chicago_crime.params import *
from chicago_crime.ml_logic.extract import load_raw_data, load_postproc_data
from chicago_crime.ml_logic.transform import add_missing_communities, clean_data_frame
from chicago_crime.ml_logic.load import upload_dt_to_bigquery
from chicago_crime.ml_logic.model import initialize_model, train_model, compile_model, data_split, get_metrics
from chicago_crime.ml_logic.registry import save_model, load_model
import pandas as pd
import time


def preprocess_data() -> None:

    # Add missing communities to the data and upload to gbq
    add_missing_communities()

    # Load Raw Data from Google Big Query. It is grouped by Days
    df = load_raw_data()
    postproc_df = clean_data_frame(df)
    upload_dt_to_bigquery(postproc_df)

    print("✅ preprocess_data() done !!!\n")


def train():

    # get data from gcp

    df = load_postproc_data()

    # split train val

    # get a smaller dataset first, expand later

    sequence_length = 365
    n_communities = len(set(df.community_area))

    X_test_scaled, X_test, y_test, X_train, y_train, X_val, y_val = data_split(df, sequence_length, 0.9, 0.2)

    # initialize model

    model = initialize_model(sequence_length, n_communities)
    model = compile_model(model)

    # fit and train model

    start_time = time.time()
    history, model = train_model(model, X_train, y_train, X_val,
                                 y_val, epochs = 10, batch_size = 16)
    train_time = time.time() - start_time

    test_mae, base_mae = get_metrics(model, X_test_scaled, X_test, y_test)

    # save model to mlflow

    save_model(model, test_mae, base_mae, sequence_length, train_time)

    print("Model trained and saved to mlflow")

# TODO: predict on new data

def predict():

    model = load_model()
    return model.predict()

if __name__ == '__main__':
    #preprocess_data()
    train()
    #predict()
