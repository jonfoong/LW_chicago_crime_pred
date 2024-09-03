import mlflow
from mlflow import MlflowClient
import mlflow.tensorflow
from chicago_crime.params import *

# save results

# save model

def save_model(model,
               test_metric,
               base_metric,
               sequence_length,
               train_time):
    # Set the tracking URI to Databricks
    mlflow.set_tracking_uri(DATABRICKS_EXP_URI)
    mlflow.set_experiment(DATABRICKS_EXP_PATH)

    client = MlflowClient()
    # get previous runs and compare performance
    runs = client.search_runs(experiment_ids=[DATABRICKS_EXP_ID])

    # check if previous runs do better
    if len(runs) > 0:
        for run in runs:
            run_id = run.info.run_id
            run_metric = run.data.metrics.get("test mae", None)
            if test_metric < run_metric:
                # if old model is worse, move to staging and current to production
                client.set_tag(run_id, "stage", "staging")
                stage = "production"
            else:
                stage = "staging"
    else:
        stage = "production" # default production if no models exist

    # Start an MLflow run to log parameters and save model to mlflow
    with mlflow.start_run():
        # Log parameters, metrics, and the model itself
        mlflow.log_params({"sequence_length": sequence_length})
        mlflow.log_metric("test mae", test_metric)
        mlflow.log_metric("base mae", base_metric)
        mlflow.log_metric("train time", train_time)
        mlflow.tensorflow.log_model(model, "tensorflow_model")
        mlflow.set_tag("stage", stage)
        mlflow.end_run()

        print(f"Model trained and logged with test mae: {test_metric:.4f} and stage: {stage}")


# load model

def load_model():

    # load from GCS
    # client = storage.Client()
    # blobs = client.get_bucket("lw_chicago_crime_pred").list_blobs(prefix="dummy")
    # latest_blob = max(blobs)

    # load to python
    # with tempfile.NamedTemporaryFile(delete=True) as temp_file:
    #     # Download the blob into the temporary file
    #     latest_blob.download_to_filename(temp_file.name)

    #     # Load the model from the temporary file
    #     with open(temp_file.name, 'rb') as file:
    #         model = pickle.load(file)
    mlflow.set_tracking_uri(DATABRICKS_EXP_URI)
    mlflow.set_experiment(DATABRICKS_EXP_PATH)

    client = MlflowClient()

    mlflow_runs = client.search_runs(
        experiment_ids=[DATABRICKS_EXP_ID],
        filter_string="tags.stage = 'production'"
        )

    production_run = mlflow_runs[0]
    run_id = production_run.info.run_id

    # Load the model from the selected run
    model_uri = f"runs:/{run_id}/tensorflow_model"
    model = mlflow.tensorflow.load_model(model_uri)

    return model
