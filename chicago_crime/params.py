# add params here
import os 

# Read the Databricks API token from the file
with open('secrets/databricks_api.txt', 'r') as file:
    databricks_token = file.read().strip()

# Set the environment variables for Databricks
os.environ['DATABRICKS_HOST'] = 'https://4095687731202574.4.gcp.databricks.com'
os.environ['DATABRICKS_TOKEN'] = databricks_token