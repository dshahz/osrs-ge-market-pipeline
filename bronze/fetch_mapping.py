"""Fetch the /mapping item reference snapshot and land it in Bronze."""

from bronze.common import fetch_endpoint, write_json

url_mapping = "https://prices.runescape.wiki/api/v1/osrs/mapping"
VOLUME_PATH = "/Volumes/osrs_pipeline/bronze/item_mapping_raw"


def write_mapping(response):
    if response is None:
        raise ValueError("No response to write — fetch likely failed")
    # Single overwritten snapshot: /mapping returns the full item list every time.
    return write_json(response, VOLUME_PATH, "mapping")


if __name__ == "__main__":
    write_mapping(fetch_endpoint(url_mapping))