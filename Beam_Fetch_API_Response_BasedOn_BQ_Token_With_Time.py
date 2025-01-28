import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
import requests
import time
from datetime import datetime, timedelta
from threading import Lock


class BearerTokenManager:
    """
    Manages the Bearer token and refreshes it if expired.
    """
    def __init__(self, token_api_url, token_api_payload, max_retries=3, retry_delay=5):
        self.token_api_url = token_api_url
        self.token_api_payload = token_api_payload
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.bearer_token = None
        self.token_expiry = None
        self.lock = Lock()

    def fetch_token(self):
        """
        Fetches a new Bearer token and updates the expiration time.
        """
        attempt = 0
        while attempt < self.max_retries:
            try:
                response = requests.post(self.token_api_url, json=self.token_api_payload)
                if response.status_code == 200:
                    data = response.json()
                    self.bearer_token = data.get("access_token")
                    expires_in = data.get("expires_in", 3600)  # Default expiry to 1 hour if not provided
                    self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)  # Refresh 1 min early
                    print(f"Fetched new Bearer token, valid until {self.token_expiry}")
                    return
                else:
                    print(f"Failed to fetch Bearer token: {response.status_code}, {response.text}")
            except requests.RequestException as e:
                print(f"Error fetching Bearer token: {e}")
            attempt += 1
            time.sleep(self.retry_delay)

        raise RuntimeError("Failed to fetch Bearer token after multiple attempts.")

    def get_token(self):
        """
        Returns the current token, refreshing it if expired.
        """
        with self.lock:  # Ensure thread safety
            if not self.bearer_token or datetime.now() >= self.token_expiry:
                print("Refreshing Bearer token...")
                self.fetch_token()
            return self.bearer_token


class FetchAPIDataWithPreemptiveTokenRefresh(beam.DoFn):
    """
    Fetches data from the API with preemptive token refresh.
    """
    def __init__(self, base_api_url, token_manager):
        self.base_api_url = base_api_url
        self.token_manager = token_manager

    def process(self, element):
        id_value = element['id']
        api_url = f"{self.base_api_url}/{id_value}"

        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.get(api_url, headers=headers)
            if response.status_code == 200:
                yield response.json()
            else:
                print(f"API call failed for ID {id_value}: {response.status_code}, {response.text}")
        except requests.RequestException as e:
            print(f"Error contacting API for ID {id_value}: {e}")


def run():
    # Parameters
    project_id = "your-project-id"
    input_table = "your-project-id:dataset.input_table"  # BigQuery table containing IDs
    output_table = "your-project-id:dataset.output_table"  # BigQuery table to store API responses
    temp_location = "gs://your-bucket/temp"
    token_api_url = "https://auth.example.com/token"  # URL to fetch the Bearer token
    token_api_payload = {  # Payload for the token API
        "client_id": "your-client-id",
        "client_secret": "your-client-secret",
        "grant_type": "client_credentials",
    }
    base_api_url = "https://api.example.com/resource"  # Base API URL

    # SQL query to fetch IDs from BigQuery
    query = """
    SELECT id
    FROM `your-project-id.dataset.input_table`
    WHERE condition_column > 5
    """

    # Apache Beam pipeline options
    pipeline_options = PipelineOptions(
        project=project_id,
        temp_location=temp_location,
        region="your-region",  # Replace with your GCP region
    )

    # Step 1: Initialize the BearerTokenManager
    token_manager = BearerTokenManager(token_api_url, token_api_payload)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Step 2: Read IDs from BigQuery
        ids = (
            pipeline
            | "Read IDs from BigQuery" >> ReadFromBigQuery(query=query, use_standard_sql=True)
        )

        # Step 3: Fetch data from the API with preemptive token refresh
        api_responses = (
            ids
            | "Fetch API Data with Token Refresh" >> beam.ParDo(
                FetchAPIDataWithPreemptiveTokenRefresh(base_api_url, token_manager)
            )
        )

        # Step 4: Write API responses to BigQuery
        api_responses | "Write to BigQuery" >> WriteToBigQuery(
            output_table,
            schema="SCHEMA_AUTODETECT",  # Auto-detect schema from the API response
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )


if __name__ == "__main__":
    run()
