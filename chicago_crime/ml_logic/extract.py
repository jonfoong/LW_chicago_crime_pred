# get raw data
import pandas as pd
from google.cloud import bigquery
from chicago_crime.params import *


def load_raw_data() -> pd.DataFrame:

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
    """

    client = bigquery.Client()
    query_out = client.query(query_load_raw_data)
    query_out.result()

    df = query_out.to_dataframe()
    print(f"✅ Loaded {df.shape[0]} rows from GCloud.chicago_crime_temp\n")

    return df

def load_postproc_data() -> pd.DataFrame:

    query_postproc_data = f"""
    SELECT *
    FROM `{GCP_PROJECT}.{BQ_DATASET}.post_proc`
    """

    # query and wait
    client = bigquery.Client()
    query_out = client.query(query_postproc_data)
    query_out.result()

    # convert to df
    df = query_out.to_dataframe()
    print(f"✅ Loaded {df.shape[0]} rows from GCloud.post_proc\n")

    return df
