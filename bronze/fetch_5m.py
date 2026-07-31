import requests
import json
import io
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

# API endpoints
url_5m = "https://prices.runescape.wiki/api/v1/osrs/5m"

# Headers for the API requests
HEADERS = {"User-Agent": "Flipping recommendations - Data Engineering Practice @xfprodigy on Discord"}

#Directory to store the data
VOLUME_PATH = "/Volumes/osrs_pipeline/bronze/data_raw"




def fetch_endpoint(url, headers, params=None, timeout=10):
    try:
        response = requests.get(url=url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()  # Raise an exception for HTTP errors
        response_json = response.json()
        print(f"OK: {url} — {len(response_json.get('data', {}))} items")
        return response_json

    except requests.RequestException as e:
        print(f"Error fetching endpoint {url}: {e}")
        return None


def write_bronze(response):
    if response is None:
        raise ValueError("No response to write — fetch likely failed")
    # read DATABRICKS_HOST and DATABRICKS_TOKEN from env
    w = WorkspaceClient()

    filename = response.get('timestamp')
    full_path = f"{VOLUME_PATH}/{filename}.json"
    file_contents = io.BytesIO(json.dumps(response).encode('utf-8'))
    w.files.upload(full_path, file_contents, overwrite=True)
    print(f"Data saved to {full_path}")
    return full_path

if __name__ == "__main__":
    write_bronze(fetch_endpoint(url_5m, headers=HEADERS))