from chicago_crime.params import *
from chicago_crime.ml_logic.extract import load_raw_data, load_postproc_data, load_training_data, load_minmax_train
from chicago_crime.ml_logic.transform import add_missing_communities, clean_data_frame
from chicago_crime.ml_logic.load import upload_dt_to_bigquery
from chicago_crime.ml_logic.model import initialize_model, train_model, compile_model, data_split, get_metrics
from chicago_crime.ml_logic.registry import save_model, load_model
import time
import pandas as pd


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

    # get max training date

    max_date = pd.to_datetime(df.Date_day).max().strftime('%Y-%m-%d')

    # split train val

    # get a smaller dataset first, expand later

    n_communities = len(set(df.community_area))

    X_test_scaled, X_test, y_test, X_train, y_train, X_val, y_val = data_split(df, SEQUENCE_LENGTH, 0.9, 0.2)

    # initialize model

    model = initialize_model(SEQUENCE_LENGTH, n_communities)
    model = compile_model(model)

    # fit and train model

    start_time = time.time()
    history, model = train_model(model, X_train, y_train, X_val,
                                 y_val, epochs = 10, batch_size = 16)
    train_time = time.time() - start_time

    test_mae, base_mae = get_metrics(model, X_test_scaled, X_test, y_test)

    # save model to mlflow

    save_model(model, test_mae, base_mae, SEQUENCE_LENGTH, train_time, max_date)

    print("Model trained and saved to mlflow")

# TODO: predict on new data

def predict():

    model, sequence_length_model, train_max_date = load_model()

    # load data the model was trained on
    df = load_training_data(train_max_date, sequence_length_model)
    df = df.pivot(index='Date_day', columns='community_area', values='crime_count')

    # load min and max data
    df_minmax = load_minmax_train()
    df_pivot = df.pivot(index='date', columns='community_area', values='crime_count')

    # pivot the data from long to wide

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

    return model.predict()

if __name__ == '__main__':
    #preprocess_data()
    train()
    #predict()
