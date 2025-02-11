import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from unittest.mock import patch
import responses
from fetch_id import FetchID, BASE_URL, TOKEN_URL

VALID_TOKEN = "valid_token_123"
INVALID_TOKEN = "invalid_token_456"
EXPIRED_TOKEN = "expired_token_789"

# Mock API responses
@pytest.fixture
def mock_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


def mock_get_token(self):
    """Mock get_token method"""
    return VALID_TOKEN


def mock_get_token_expired(self):
    """Mock expired token method"""
    return EXPIRED_TOKEN


@pytest.mark.parametrize("scenario, token, response_status, response_body, expected_output", [
    ("Valid Token", VALID_TOKEN, 200, {"ids": [1, 2, 3]}, [1, 2, 3]),
    ("Invalid Token", INVALID_TOKEN, 401, {"error": "Unauthorized"}, [{"error": "401 Client Error"}]),
    ("Expired Token", EXPIRED_TOKEN, 401, {"error": "Token Expired"}, [{"error": "401 Client Error"}]),
    ("Missing Token", None, 401, {"error": "Missing Token"}, [{"error": "401 Client Error"}]),
])
def test_fetch_id(mock_responses, scenario, token, response_status, response_body, expected_output):
    """Test FetchID class for different authentication scenarios"""

    # Mock token endpoint
    mock_responses.add(
        responses.POST, TOKEN_URL,
        json={"access_token": token} if token else {},
        status=200 if token else 401
    )

    # Mock API response for fetching IDs
    mock_responses.add(
        responses.GET, BASE_URL,
        json=response_body,
        status=response_status
    )

    # Use patch to replace the get_token method
    token_patch = patch.object(FetchID, "get_token", return_value=token if token else None)
    
    with token_patch, TestPipeline() as p:
        test_input = p | "Create Test Input" >> beam.Create(["dummy"])
        results = test_input | "Run FetchID" >> beam.ParDo(FetchID())

        # Validate output
        assert_that(results, equal_to(expected_output))
