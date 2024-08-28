FROM python:3.10.6-buster

COPY chicago_crime /chicago_crime
COPY requirements.txt /requirements.txt
COPY .env /.env

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD uvicorn chocago_crime.api.fast:app --host 0.0.0.0 --port $PORT
