# upload transformed data to GBQ
from chicago_crime.params import *
import pandas as pd
from google.cloud import bigquery

def upload_dt_to_bigquery(
        df: pd.DataFrame
    ) -> None:
    """
    - Save the DataFrame to BigQuery
    - Empty the table beforehand (WRITE_TRUNCATE)
    """

    client = bigquery.Client()

    # Define write mode
    write_mode = "WRITE_TRUNCATE"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        schema=[
            bigquery.SchemaField("Date_day", "DATE"),
            bigquery.SchemaField("community_area", "STRING"),
            bigquery.SchemaField("crime_count", "INTEGER"),
        ]
    )

    load_job = client.load_table_from_dataframe(
        df,  # This is the pandas DataFrame you want to load
        f'{GCP_PROJECT}.{BQ_DATASET}.post_proc',
        job_config=job_config
        )

    load_job.result()

    print(f"✅ Data saved to BQ successfully, N of rows: {df.shape[0]}")
