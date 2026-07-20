import requests

HEADERS = {"User-Agent": "Flipping recommendations - Data Engineering Practice @xfprodigy on Discord"}

# API endpoints
url_latest = "https://prices.runescape.wiki/api/v1/osrs/latest"
url_5m = "https://prices.runescape.wiki/api/v1/osrs/5m"




def fetch_endpoint(url, headers, timeout=10):
    try:
        response = requests.get(url=url, headers=headers, timeout=timeout)
        response.raise_for_status()  # Raise an exception for HTTP errors
        response_json = response.json()
        print(f"OK: {url} — {len(response_json.get('data', {}))} items")
        return response_json

    except requests.RequestException as e:
        print(f"Error fetching endpoint {url}: {e}")
        return None



if __name__ == "__main__":
    latest = fetch_endpoint(url_latest, HEADERS)
    five_minute = fetch_endpoint(url_5m, HEADERS)
