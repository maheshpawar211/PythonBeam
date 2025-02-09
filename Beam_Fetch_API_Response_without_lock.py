import apache_beam as beam
import requests
import time

class FetchAPIWithRefresh(beam.DoFn):
    api_token = None  # Shared per worker instance
    token_expiry = 0  # Epoch time for token expiry

    def get_new_token(self):
        """Fetch a new Bearer token from an authentication server."""
        auth_url = "https://your-auth-server.com/token"
        response = requests.post(auth_url, json={"client_id": "your_client", "client_secret": "your_secret"})
        
        if response.status_code == 200:
            data = response.json()
            self.api_token = data["access_token"]
            self.token_expiry = time.time() + data["expires_in"] - 60  # Refresh 1 min before expiry
        else:
            raise Exception(f"Failed to fetch token: {response.status_code} {response.text}")

    def process(self, url):
        """Fetch data from API using the token, refreshing it if needed."""
        if not self.api_token or time.time() >= self.token_expiry:
            self.get_new_token()

        headers = {"Authorization": f"Bearer {self.api_token}"}
        response = requests.get(url, headers=headers)
        
        yield {"url": url, "status": response.status_code, "data": response.json()}

# Beam Pipeline
with beam.Pipeline() as pipeline:
    urls = [
        "https://jsonplaceholder.typicode.com/todos/1",
        "https://jsonplaceholder.typicode.com/todos/2"
    ]

    results = (
        pipeline
        | "Create URLs" >> beam.Create(urls)
        | "Fetch API" >> beam.ParDo(FetchAPIWithRefresh())
        | "Print" >> beam.Map(print)
    )
