FROM python:3.9

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]