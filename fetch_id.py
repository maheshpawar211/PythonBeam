import requests
import apache_beam as beam

BASE_URL = "https://api.example.com/ids"
TOKEN_URL = "https://auth.example.com/token"

class FetchID(beam.DoFn):
    """Fetches IDs from API using OAuth2 authentication."""

    def get_token(self):
        """Fetch OAuth2 Bearer Token"""
        response = requests.post(TOKEN_URL, data={"grant_type": "client_credentials"})
        response.raise_for_status()
        return response.json().get("access_token")

    def fetch_ids(self, token):
        """Fetch IDs using the provided token"""
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(BASE_URL, headers=headers)
        response.raise_for_status()
        return response.json().get("ids", [])

    def process(self, element):
        """Apache Beam DoFn process method"""
        try:
            token = self.get_token()
            ids = self.fetch_ids(token)
            for _id in ids:
                yield _id
        except requests.exceptions.RequestException as e:
            yield {"error": str(e)}
