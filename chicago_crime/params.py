# load dotenv
from dotenv import load_dotenv
load_dotenv()

# add params here
import os

# GCP

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_REGION = os.environ.get("BQ_REGION")
QUERY_NROWS = os.environ.get("QUERY_NROWS")

# databricks
DATABRICKS_EXP_ID = os.environ.get("DATABRICKS_EXP_ID")
DATABRICKS_EXP_URI = os.environ.get("DATABRICKS_EXP_URI")
DATABRICKS_EXP_PATH = os.environ.get("DATABRICKS_EXP_PATH")
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
SEQUENCE_LENGTH = int(os.environ.get("SEQUENCE_LENGTH"))

# Read the Databricks API token from the file
with open('secrets/databricks_api.txt', 'r') as file:
    databricks_token = file.read().strip()

# Set the environment variables for Databricks
os.environ['DATABRICKS_TOKEN'] = databricks_token
