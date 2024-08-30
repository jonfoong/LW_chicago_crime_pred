from chicago_crime.params import *
from chicago_crime.ml_logic.extract import load_raw_data, load_postproc_data
from chicago_crime.ml_logic.transform import add_missing_communities, clean_data_frame
from chicago_crime.ml_logic.load import upload_dt_to_bigquery
from chicago_crime.ml_logic.model import initialize_model, train_model, compile_model
from chicago_crime.ml_logic.registry import save_model

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np


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

    df_small = df[pd.to_datetime(df.Date_day).dt.year==2024].query("community_area=='25'")

    # specify sequence length and split data into train and val
    sequence_length = 7
    crime_count_list = list(df_small["crime_count"])
    X, y = [], []
    for i in range(sequence_length, len(crime_count_list)):
        X.append(crime_count_list[i-sequence_length:i])
        y.append(crime_count_list[i])

    X = np.array(X)
    y = np.array(y)

    X_train, X_val, y_train, y_val = train_test_split(X, y,
                                                      test_size=0.25, shuffle=False)    
    # rescale data

    scaler=MinMaxScaler()
    scaler.fit(X_train)

    X_train_scaled=scaler.transform(X_train)
    X_val_scaled=scaler.transform(X_val)

    # initialize model

    model = initialize_model(sequence_length)   
    model = compile_model(model)

    # fit and train model

    history, model = train_model(model, X_train_scaled, y_train, X_val_scaled, 
                y_val, epochs = 10, batch_size = 16)    

    mae = np.min(history.history['val_loss'])

    # save model to mlflow

    save_model(model, mae)
    print("Model trained and saved to mlflow")

# TODO: predict on new data

def predict():
    pass

if __name__ == '__main__':
    preprocess_data()
    train()
    predict()
