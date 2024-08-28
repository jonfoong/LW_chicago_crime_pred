# upload transformed data to GBQ

from chicago_crime.params import *
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account

def upload_dt_to_bigquery(
        data: pd.DataFrame,
        gcp_project:str,
        bq_dataset:str,
        table: str,
        credentials: service_account.Credentials
    ) -> None:
    """
    - Save the DataFrame to BigQuery
    - Empty the table beforehand if `truncate` is True, append otherwise
    """

    destination_table = f"{gcp_project}.{bq_dataset}.{table}"
    to_gbq(data, destination_table, if_exists='replace', credentials=credentials)

    print(f"✅ Data saved to BQ successfully")
