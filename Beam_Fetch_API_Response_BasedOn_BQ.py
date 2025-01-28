import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
import requests
from requests.exceptions import RequestException
import time

# Custom DoFn to call the API with error handling
class FetchAPIData(beam.DoFn):
    def __init__(self, api_url, max_retries=3, retry_delay=5):
        self.api_url = api_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def process(self, element):
        token = element['token']
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        attempt = 0
        while attempt < self.max_retries:
            try:
                # Send request to the API
                response = requests.get(self.api_url, headers=headers)

                if response.status_code == 200:
                    # Parse and yield the API response
                    data = response.json()
                    for item in data:  # Assuming the response is a list of objects
                        yield item
                    return
                elif response.status_code == 401:  # Unauthorized (expired token)
                    print(f"Token expired for token {token}. Skipping...")
                    return
                else:
                    print(f"API call failed for token {token}: {response.status_code}, {response.text}")
                    return

            except RequestException as e:
                print(f"Error contacting API for token {token}: {e}. Retrying...")
                attempt += 1
                time.sleep(self.retry_delay)

        print(f"API call failed after {self.max_retries} attempts for token {token}. Skipping...")

# Custom DoFn to process batches
class ProcessBatch(beam.DoFn):
    def process(self, batch):
        # Perform operations on the batch
        print(f"Processing batch: {batch}")
        # Example: Add metadata to each item in the batch
        for item in batch:
            item['processed'] = True  # Add a flag to indicate processing
        yield batch

def run():
    # Parameters
    project_id = "your-project-id"
    input_table = "your-project-id:dataset.input_table"  # Input BigQuery table
    output_table = "your-project-id:dataset.output_table"  # Output BigQuery table
    temp_location = "gs://your-bucket/temp"
    api_url = "https://api.example.com/data"  # Replace with your API endpoint

    # SQL query to fetch tokens from BigQuery
    query = """
    SELECT token
    FROM `your-project-id.dataset.input_table`
    WHERE condition_column > 5
    """

    pipeline_options = PipelineOptions(
        project=project_id,
        temp_location=temp_location,
        region="your-region",  # Replace with your GCP region
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Step 1: Read data from BigQuery
        records = (
            pipeline
            | "Read Tokens from BigQuery" >> ReadFromBigQuery(query=query, use_standard_sql=True)
        )

        # Step 2: Call the API for each record
        api_results = (
            records
            | "Fetch API Data" >> beam.ParDo(FetchAPIData(api_url))
        )

        # Step 3: Group API responses into batches of 10
        batched_results = (
            api_results
            | "Batch API Responses" >> beam.BatchElements(min_batch_size=10, max_batch_size=10)
        )

        # Step 4: Process each batch
        processed_batches = (
            batched_results
            | "Process Batches" >> beam.ParDo(ProcessBatch())
        )

        # Step 5: Flatten processed batches and write to BigQuery
        processed_batches | "Flatten Batches" >> beam.FlatMap(lambda batch: batch) | "Write to BigQuery" >> WriteToBigQuery(
            output_table,
            schema="SCHEMA_AUTODETECT",  # Auto-detect schema from API response
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )


if __name__ == "__main__":
    run()
