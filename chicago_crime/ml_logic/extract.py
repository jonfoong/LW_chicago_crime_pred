# get raw data
import pandas as pd
from colorama import Fore, Style # type: ignore
from google.cloud import bigquery
from chicago_crime.params import *
from pathlib import Path
from google.oauth2 import service_account


def load_raw_data(
        credentials: service_account.Credentials
    ) -> pd.DataFrame:

    # TODO: do we need exception handling? eg check whether query contains proj_id etc

    query_load_raw_data = f"""
    SELECT
        DATE(`Date`) AS Date_day,
        `Community Area`,
        COUNT(*) AS crime_count
    FROM
        `{GCP_PROJECT}.{BQ_DATASET}.chicago_crime_temp`
    GROUP BY
        Date_day, `Community Area`
    ORDER BY
        Date_day, `Community Area`
    LIMIT 1000
    """

    df = pd.read_gbq(query_load_raw_data, credentials = credentials, dialect='standard')
    print(f"✅ Loaded {df.shape[0]} rows from GCloud.chicago_crime_temp\n")

    return df
