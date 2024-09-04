# fast api
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


import numpy as np
from datetime import timedelta
import pandas as pd

from chicago_crime.ml_logic.extract import load_training_data, load_minmax_train
from chicago_crime.ml_logic.registry import load_model


app = FastAPI()
# cache data
app.state.model, app.state.sequence_length_model, app.state.train_max_date = load_model()
# load min and max data
app.state.df_minmax = load_minmax_train()
# load training data
app.state.train_df = load_training_data(app.state.train_max_date, app.state.sequence_length_model)

# Allowing all middleware is optional, but good practice for dev purposes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# http://127.0.0.1:8000/predict?predict_day=2024-08-27
@app.get("/predict")
def predict(date = "2024-08-15"):

    # load data the model was trained on

    df = app.state.train_df.pivot(index='Date_day', columns='community_area', values='crime_count')

    # pivot the data from long to wide

    old_length = len(df)
    t = df.index[-1]

    date = pd.to_datetime(date)

    while t < date.date():
        # 1. turn data into format to feed model:
        X_list = []
        areas = np.sort([int(i) for i in df.columns.unique()])
        for area in areas:
            df_com = df[f"{area}"]
            # turn crimes into list
            crime_count_list = list(df_com)
            # create sequences
            X= np.array(crime_count_list[-int(app.state.sequence_length_model):])
            # scaling:
            mini=app.state.df_minmax.iloc[area-1, :].min_crime_count
            maxi=app.state.df_minmax.iloc[area-1, :].max_crime_count
            X = (X - mini) / (maxi - mini)
            # append to list
            X_list.append(X)

        X_final = np.array(X_list).transpose().reshape(1, 365, len(areas))
        # 2. predicting next days crime count:
        y = app.state.model.predict(X_final)
        # 3. time + 1 and add new row to df
        t = t + timedelta(days = 1)
        df.loc[t] = y.flatten()

    pred_days=len(df) - old_length
    return df[-pred_days:]

@app.get("/")
def root():
    return "Welcome to the Chicago crime prediction center!"
