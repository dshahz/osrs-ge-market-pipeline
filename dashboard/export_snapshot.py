"""Export the dashboard's results to a parquet snapshot it can fall back on.

When the Databricks warehouse is unreachable (quota, outage, expired token), the
dashboard serves this file instead of crashing. Runs daily from
.github/workflows/dashboard-snapshot.yml, or locally with credentials in .env:

    python dashboard/export_snapshot.py
"""

import os
import sys

import pandas as pd
from dotenv import load_dotenv
from market_data import SNAPSHOT_PATH, TIME_RANGES, LiveDataUnavailable, aggregate, query_gold


def main():
    load_dotenv()
    credentials = {
        "host": os.getenv("DATABRICKS_HOST"),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
        "token": os.getenv("DATABRICKS_TOKEN"),
    }
    snapshot_at = pd.Timestamp.now(tz="UTC")

    frames = []
    try:
        for hours in TIME_RANGES:
            result = aggregate(query_gold(hours, **credentials), hours)
            if not result.empty:
                frames.append(result.reset_index().assign(hours=hours))
    except LiveDataUnavailable as e:
        print(f"Couldn't reach Databricks, keeping the existing snapshot: {e}", file=sys.stderr)
        return 1

    # If the pipeline hasn't run in the last day, an empty export would replace
    # a useful snapshot with nothing.
    if not frames:
        print("No market data in the last 24h, keeping the existing snapshot.")
        return 0

    snapshot = pd.concat(frames, ignore_index=True).assign(snapshot_at=snapshot_at)
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    snapshot.to_parquet(SNAPSHOT_PATH, index=False)
    print(f"Wrote {len(snapshot):,} rows across {len(frames)} time ranges to {SNAPSHOT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
