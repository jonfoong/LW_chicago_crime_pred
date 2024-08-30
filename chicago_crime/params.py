# add params here
import os

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_REGION = os.environ.get("BQ_REGION")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
<<<<<<< HEAD
RAW_DATA = os.environ.get("RAW_DATA")
MODEL_TARGET = os.environ.get("MODEL_TARGET")

BUCKET_NAME = os.environ.get["BUCKET_NAME"]
=======
QUERY_NROWS = os.environ.get("QUERY_NROWS")
import os

# Read the Databricks API token from the file
with open('secrets/databricks_api.txt', 'r') as file:
    databricks_token = file.read().strip()

# Set the environment variables for Databricks
os.environ['DATABRICKS_HOST'] = 'https://4095687731202574.4.gcp.databricks.com'
os.environ['DATABRICKS_TOKEN'] = databricks_token
>>>>>>> aa7737bec5ba1953b65c11d6cec00633614c5547
