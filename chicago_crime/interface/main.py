from chicago_crime.params import *
from chicago_crime.ml_logic.extract import load_raw_data
from chicago_crime.ml_logic.transform import add_missing_communities, clean_data_frame
from chicago_crime.ml_logic.load import upload_dt_to_bigquery
from google.oauth2 import service_account


# Load Credentials
credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)

def preprocess_data() -> None:

    # Add missing communities to the data
    add_missing_communities(project_id = GCP_PROJECT, credentials = credentials )

    # Load Raw Data from Google Big Query. It is grouped by Days
    df = load_raw_data(
        credentials = credentials
    )

    print(df.info())
    print(df.head(10))

    postproc_df = clean_data_frame(df)
    upload_dt_to_bigquery(postproc_df)

    print("✅ preprocess_data() done !!!\n")


# TODO: train on preprocessed data

def train():
    pass

# TODO: predict on new data

def predict():
    pass

if __name__ == '__main__':
    preprocess_data()
    train()
    predict()
