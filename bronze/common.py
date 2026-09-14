"""Shared HTTP fetch and Bronze write helpers.

Both ingestion scripts hit the same API host and land JSON in the same kind of
Databricks volume, so request handling and upload live here rather than being
duplicated per endpoint. Endpoint-specific validation stays with the caller.
"""

import io
import json

import requests
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

# Reads DATABRICKS_HOST and DATABRICKS_TOKEN for WorkspaceClient.
load_dotenv()

# The RuneScape Wiki API asks callers to identify themselves in the User-Agent.
HEADERS = {
    "User-Agent": "Flipping recommendations - Data Engineering Practice @xfprodigy on Discord"
}


def _payload_size(payload):
    """Item count for logging. /5m wraps items in `data`; /mapping is a bare list."""
    if isinstance(payload, dict):
        return len(payload.get("data", payload))
    return len(payload)


def fetch_endpoint(url, headers=HEADERS, params=None, timeout=10):
    try:
        response = requests.get(url=url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        print(f"OK: {url} — {_payload_size(payload)} items")
        return payload

    except requests.RequestException as e:
        print(f"Error fetching endpoint {url}: {e}")
        return None


def write_json(payload, volume_path, filename):
    """Upload a JSON payload to a Databricks volume."""
    if payload is None:
        raise ValueError("No payload to write — fetch likely failed")

    w = WorkspaceClient()
    full_path = f"{volume_path}/{filename}.json"
    file_contents = io.BytesIO(json.dumps(payload).encode("utf-8"))
    w.files.upload(full_path, file_contents, overwrite=True)
    print(f"Data saved to {full_path}")
    return full_path