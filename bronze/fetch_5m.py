"""Fetch a single /5m price window and land it in Bronze."""

from bronze.common import fetch_endpoint, write_json

url_5m = "https://prices.runescape.wiki/api/v1/osrs/5m"
VOLUME_PATH = "/Volumes/osrs_pipeline/bronze/data_raw"


def write_bronze(response):
    if response is None:
        raise ValueError("No response to write — fetch likely failed")
    if not response.get("data"):
        raise ValueError(
            f"Empty window: API returned no items for timestamp {response.get('timestamp')}"
        )
    # One file per window, named for the window's own timestamp.
    return write_json(response, VOLUME_PATH, response["timestamp"])


if __name__ == "__main__":
    write_bronze(fetch_endpoint(url_5m))