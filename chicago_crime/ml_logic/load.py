# upload transformed data to GBQ

from chicago_crime.params import *
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account
from google.cloud import bigquery

def upload_dt_to_bigquery(
        data: pd.DataFrame
    ) -> None:
    """
    - Save the DataFrame to BigQuery
    - Empty the table beforehand (WRITE_TRUNCATE)
    """

    # Define write mode
    write_mode = "WRITE_TRUNCATE"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode)

    table_schema = [{'name': 'Date_day', 'type': 'DATE'},
                    {'name': 'Community Area', 'type': 'STRING'},
                    {'name': 'crime_count', 'type': 'INTEGER'}]

    data.to_gbq('wagon-bootcamp-428814.chicago_crime.post_proc', project_id = GCP_PROJECT,
                if_exists = 'replace', table_schema = table_schema)

    print(f"✅ Data saved to BQ successfully, N of rows: {data.shape[0]}")
