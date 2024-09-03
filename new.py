from datetime import date, timedelta
from chicago_crime.ml_logic import *
import numpy as np
import pandas as pd


""" NEED TO SCALER! """


def get_forecast(date):
    model=model.load_model()
    df = extract.load_postproc_data()
    old_length=len(df)

    t=df.Date_day[-1]

    while t < date:

        # 1. turn data into format to feed model:

        # X_list = []
        X_scaled_list = []


        areas = np.sort([int(i) for i in df.community_area.unique()])

        for area in areas:

            df_com = df.query(f"community_area=='{str(area)}'")

            # turn crimes into list
            crime_count_list = list(df_com["crime_count"])

            # create sequences
            sequence_length = 365
            X= np.array( crime_count_list[-365:] )

            #scaler? to get X_scaled

            # append to lists

            X_scaled_list.append(X)

        X_final=np.array(X_scaled_list).transpose().reshape(1,365,len(areas))

        # 2. loading model and predicting next days crime count:

        y=model.predict(X_final)

        # 3.
        t = t + timedelta(days=1)
        new_row=np.concatenate( np.array(t), y.reshape(1,-1), axis=1  )
        new_row=new_row.flatten()
        df.loc[len(df)]= new_row

    pred_days=len(df) - old_length

    return df[- pred_days:]


