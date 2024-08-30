# add params here
import os

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_REGION = os.environ.get("BQ_REGION")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
RAW_DATA = os.environ.get("RAW_DATA")
MODEL_TARGET = os.environ.get("MODEL_TARGET")

BUCKET_NAME = os.environ.get["BUCKET_NAME"]
