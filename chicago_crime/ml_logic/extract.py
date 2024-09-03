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
    WHERE community_area !='0'
    """

    # query and wait
    client = bigquery.Client()
    query_out = client.query(query_postproc_data)
    query_out.result()

    # convert to df
    df = query_out.to_dataframe()
    print(f"✅ Loaded {df.shape[0]} rows from GCloud.post_proc\n")

    return df

def load_training_data(last_train_date, sequence_length) -> pd.DataFrame:

    query_training_data = f"""
    SELECT *
    FROM
    `{GCP_PROJECT}.{BQ_DATASET}.post_proc`
    WHERE
    Date_day BETWEEN DATE_SUB('{last_train_date}', INTERVAL {int(sequence_length)-1} DAY) AND '{last_train_date}'
    AND community_area !='0'
    """

    # query and wait
    client = bigquery.Client()
    query_out = client.query(query_training_data)
    query_out.result()

    # convert to df
    df = query_out.to_dataframe()
    print(f"✅ Loaded {df.shape[0]} rows from GCloud.post_proc\n")

    return df

def load_minmax_train() -> pd.DataFrame:

    query_minmax_data = f"""
    SELECT
    community_area,
    MIN(crime_count) AS min_crime_count,
    MAX(crime_count) AS max_crime_count
    FROM
    `{GCP_PROJECT}.{BQ_DATASET}.post_proc`
    WHERE community_area !='0'
    GROUP BY
    community_area
    """

    # query and wait
    client = bigquery.Client()
    query_out = client.query(query_minmax_data)
    query_out.result()

    # convert to df
    df = query_out.to_dataframe()
    print(f"✅ Loaded {df.shape[0]} rows from GCloud.post_proc\n")

    return df
