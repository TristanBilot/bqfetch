import os
from google.oauth2 import service_account
from google.cloud import bigquery

ROOT = os.path.abspath(os.getcwd())
BQ_CREDS_PATH = os.path.join(ROOT, "secrets", "bq_service_account.json")
CREDS_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/devstorage.full_control"
]
BQ_LOCATION = "EU"
                
CREDENTIALS = service_account.Credentials.from_service_account_file(
    BQ_CREDS_PATH, scopes=CREDS_SCOPES
)
BQ_CLIENT = bigquery.Client(
    credentials=CREDENTIALS,
    project=CREDENTIALS.project_id
)