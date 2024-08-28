from google.cloud import storage
import pickle
from statsmodels.tsa.arima.model import ARIMAResults
import tempfile

# save results

# save model

# load model

def load_model():

    # load from GCS
    client = storage.Client()
    blobs = client.get_bucket("lw_chicago_crime_pred").list_blobs(prefix="dummy")
    latest_blob = max(blobs)

    # load to python
    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        # Download the blob into the temporary file
        latest_blob.download_to_filename(temp_file.name)

        # Load the model from the temporary file
        with open(temp_file.name, 'rb') as file:
            model = pickle.load(file)

    return model
