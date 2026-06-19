import os
import requests
import pandas as pd

API_URL = "https://randomuser.me/api/?results=1000"  # Adjusted to avoid quota
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
PARQUET_FILE_PREFIX = "users_raw"
PARQUET_FILE_GLOB = f"{PARQUET_FILE_PREFIX}_*.parquet"


def parquet_file_path(execution_date: str) -> str:
    return os.path.join(DATA_DIR, f"{PARQUET_FILE_PREFIX}_{execution_date}.parquet")


def flatten_user(user: dict) -> dict:
    """Flattens nested user data into a clean dictionary."""
    location = user.get("location", {})
    street = location.get("street", {})
    coordinates = location.get("coordinates", {})
    timezone = location.get("timezone", {})
    login = user.get("login", {})
    name = user.get("name", {})
    dob = user.get("dob", {})
    registered = user.get("registered", {})

    return {
        "uuid": login.get("uuid"),
        "username": login.get("username"),
        "title": name.get("title"),
        "first_name": name.get("first"),
        "last_name": name.get("last"),
        "gender": user.get("gender"),
        "email": user.get("email"),
        "phone": user.get("phone"),
        "cell": user.get("cell"),
        "nationality": user.get("nat"),
        "street_number": street.get("number"),
        "street_name": street.get("name"),
        "city": location.get("city"),
        "state": location.get("state"),
        "country": location.get("country"),
        "postcode": str(location.get("postcode")),
        "latitude": coordinates.get("latitude"),
        "longitude": coordinates.get("longitude"),
        "timezone_offset": timezone.get("offset"),
        "timezone_desc": timezone.get("description"),
        "dob_date": dob.get("date"),
        "dob_age": dob.get("age"),
        "registered_date": registered.get("date"),
        "registered_age": registered.get("age"),
    }


def fetch_users_data(url: str = API_URL) -> list:
    """Fetch users from API with error handling."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"API error: {e}")
        return []


def process_and_save_parquet(execution_date=None, **context) -> str:
    """Main pipeline: fetch -> flatten -> save Parquet with execution date."""
    os.makedirs(DATA_DIR, exist_ok=True)

    # Get execution date from context if not provided directly
    if execution_date is None and context:
        execution_date = context.get("ds", "")

    if not execution_date:
        raise ValueError("execution_date is required to build the parquet file path")

    output_file = parquet_file_path(execution_date)

    users = fetch_users_data()
    if not users:
        raise ValueError("No API data retrieved")

    df = pd.DataFrame([flatten_user(u) for u in users])

    # Clean date formats
    df["dob_date"] = pd.to_datetime(df["dob_date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    df["registered_date"] = pd.to_datetime(
        df["registered_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # Save to Parquet
    df.to_parquet(output_file, index=False, compression="snappy")
    return output_file


if __name__ == "__main__":
    process_and_save_parquet()
