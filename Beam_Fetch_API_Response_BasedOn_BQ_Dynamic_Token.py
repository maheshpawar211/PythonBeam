import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
import requests
from requests.exceptions import RequestException
import time


# Function to fetch Bearer token (called only once or during token refresh)
def fetch_bearer_token(token_api_url, token_api_payload, max_retries=3, retry_delay=5):
    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.post(token_api_url, json=token_api_payload)
            if response.status_code == 200:
                token = response.json().get("access_token")
                if token:
                    return token
                else:
                    print("Token not found in the response.")
            else:
                print(f"Failed to fetch Bearer token: {response.status_code}, {response.text}")
        except RequestException as e:
            print(f"Error fetching Bearer token: {e}")
        attempt += 1
        time.sleep(retry_delay)
    raise RuntimeError("Failed to fetch Bearer token after multiple attempts.")


# Custom DoFn to fetch data from the API
class FetchAPIData(beam.DoFn):
    def __init__(self, base_api_url):
        self.base_api_url = base_api_url
        self.bearer_token = None

    def process(self, element, bearer_token):
        id_value = element['id']
        api_url = f"{self.base_api_url}/{id_value}"

        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.get(api_url, headers=headers)

            if response.status_code == 200:
                yield response.json()  # Parse and yield the API response
            elif response.status_code == 401:  # Unauthorized
                print(f"Token expired for ID {id_value}.")
                raise RuntimeError("Bearer token expired.")  # Trigger retry at a higher level
            else:
                print(f"API call failed for ID {id_value}: {response.status_code}, {response.text}")
        except RequestException as e:
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

    # Step 1: Fetch the Bearer token once
    try:
        bearer_token = fetch_bearer_token(token_api_url, token_api_payload)
    except RuntimeError as e:
        print(f"Error fetching initial Bearer token: {e}")
        return

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Step 2: Read IDs from BigQuery
        ids = (
            pipeline
            | "Read IDs from BigQuery" >> ReadFromBigQuery(query=query, use_standard_sql=True)
        )

        # Step 3: Fetch data from the API using the Bearer token
        api_responses = (
            ids
            | "Fetch API Data" >> beam.ParDo(FetchAPIData(base_api_url), bearer_token=beam.pvalue.AsSingleton([bearer_token]))
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
