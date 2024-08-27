# fast api
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from chicago_crime.ml_logic.registry import load_model

app = FastAPI()
app.state.model = load_model()

# Allowing all middleware is optional, but good practice for dev purposes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# http://127.0.0.1:8000/predict?predict_day=2024-08-27
@app.get("/predict")
def predict(
        predict_day: str = '2024-08-27',
    ):
    """
    Make a single prediction.
    """
    pred = app.state.model.predict(predict_day)
    return {'n_crimes': pred[0]}

@app.get("/")
def root():
    return "Hello to the chicago crime prediction api!"
