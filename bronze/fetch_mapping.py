import requests
import os
import json

# API endpoints
url_mapping = "https://prices.runescape.wiki/api/v1/osrs/mapping"

# Headers for the API requests
HEADERS = {"User-Agent": "Flipping recommendations - Data Engineering Practice @xfprodigy on Discord"}

#Directory to store the data
MAPPING_DIR = "data/mapping/"




def fetch_endpoint(url, headers, params=None, timeout=10):
    try:
        response = requests.get(url=url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()  # Raise an exception for HTTP errors
        response_json = response.json()
        print(f"OK: {url} — {len(response_json)} items")
        return response_json

    except requests.RequestException as e:
        print(f"Error fetching endpoint {url}: {e}")
        return None


def write_bronze(response):
    if response is None:
        raise ValueError("No response to write — fetch likely failed")
    os.makedirs(MAPPING_DIR, exist_ok=True)  # Create the mapping directory if it doesn't exist
    full_path = os.path.join(MAPPING_DIR, "mapping.json")
    with open(full_path, "w") as f:
        json.dump(response, f, indent=2)
    print(f"Data saved to {full_path}")
    return full_path





if __name__ == "__main__":
    write_bronze(fetch_endpoint(url_mapping, headers=HEADERS))