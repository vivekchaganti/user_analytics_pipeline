import pytest
import pandas as pd
from api_client import flatten_user, fetch_users_data

def test_flatten_user():
    """Test that a raw API user record is correctly flattened."""
    mock_user = {
        "gender": "female",
        "name": {"title": "Ms", "first": "Jane", "last": "Doe"},
        "location": {
            "street": {"number": 123, "name": "Main St"},
            "city": "Springfield",
            "state": "Illinois",
            "country": "USA",
            "postcode": "62704",
            "coordinates": {"latitude": "39.78", "longitude": "-89.65"},
            "timezone": {"offset": "-5:00", "description": "CST"}
        },
        "email": "jane.doe@example.com",
        "login": {
            "uuid": "abc-123-def",
            "username": "janedoe"
        },
        "dob": {"date": "1990-01-01T00:00:00.000Z", "age": 34},
        "registered": {"date": "2020-01-01T00:00:00.000Z", "age": 4},
        "phone": "123-456-7890",
        "cell": "987-654-3210",
        "nat": "US"
    }

    result = flatten_user(mock_user)

    assert result["uuid"] == "abc-123-def"
    assert result["username"] == "janedoe"
    assert result["first_name"] == "Jane"
    assert result["last_name"] == "Doe"
    assert result["city"] == "Springfield"
    assert result["country"] == "USA"
    assert result["postcode"] == "62704"
    assert result["dob_date"] == "1990-01-01T00:00:00.000Z"

def test_flatten_user_missing_fields():
    """Test flatten_user with missing nested fields to ensure robustness."""
    mock_user = {
        "login": {"uuid": "abc-123"},
        "name": {}
    }
    result = flatten_user(mock_user)
    assert result["uuid"] == "abc-123"
    assert result["first_name"] is None
    assert result["city"] is None

def test_fetch_users_data_mock(monkeypatch):
    """Test API fetching using a mock response."""
    def mock_get(*args, **kwargs):
        class MockResponse:
            def json(self):
                return {"results": [{"login": {"username": "testuser"}}]}
            def raise_for_status(self):
                pass
        return MockResponse()

    monkeypatch.setattr("requests.get", mock_get)
    
    raw_results = fetch_users_data(1)
    flattened = [flatten_user(u) for u in raw_results]
    assert len(flattened) == 1
    assert flattened[0]["username"] == "testuser"
