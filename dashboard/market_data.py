"""Query and aggregation logic shared by the dashboard and the snapshot exporter.

Kept free of Streamlit so export_snapshot.py can run headless in GitHub Actions.
"""

from pathlib import Path

import pandas as pd
from databricks import sql

TIME_RANGES = [1, 3, 6, 12, 24]
WINDOWS_PER_HOUR = 12
WINDOWS_PER_CYCLE = 48  # a buy limit resets every 4 hours

SNAPSHOT_PATH = Path(__file__).parent / "snapshot" / "flip_opportunities.parquet"

# The connector's defaults retry for up to 15 minutes with a 15-minute socket
# timeout, so an unavailable warehouse left visitors on a spinner before the
# page crashed. Failing within ~30 seconds lets the dashboard fall back to the
# snapshot while someone is still looking at it.
CONNECTION_LIMITS = {
    "_retry_stop_after_attempts_count": 4,
    "_retry_stop_after_attempts_duration": 30,
    "_socket_timeout": 30,
}


class LiveDataUnavailable(Exception):
    """The Gold layer couldn't be reached: quota, auth, network or config."""


def query_gold(hours, host, http_path, token):
    """Return raw flip_opportunities rows for the last `hours` hours."""
    try:
        with sql.connect(
            # The CLI and the SQL connector disagree on whether the host includes
            # the scheme, so accept either.
            server_hostname=(host or "").removeprefix("https://").rstrip("/"),
            http_path=http_path,
            access_token=token,
            **CONNECTION_LIMITS,
        ) as connection:
            return pd.read_sql(
                "SELECT * FROM osrs_pipeline.gold.flip_opportunities "
                f"WHERE time_window >= (current_timestamp() - INTERVAL {int(hours)} HOURS)",
                connection,
            )
    except Exception as e:
        # Everything that stops us reaching the warehouse is one condition as far
        # as callers are concerned. The original error stays chained for the logs.
        raise LiveDataUnavailable(str(e)) from e


def aggregate(df, hours):
    """Roll raw 5-minute windows up to one ranked row per item."""
    if df.empty:
        return pd.DataFrame()

    all_agg = df.groupby("item_id").agg({
        "item_name": "first",
        "high_price_volume": "sum",
        "low_price_volume": "sum",
        "avg_high_price": "mean",
        "avg_low_price": "mean",
        "buy_limit": "first",
    })

    complete = df[df["is_complete"]]
    complete_agg = complete.groupby("item_id").agg({
        "profit_per_item": "mean",
        "profit_per_item_pct": "mean",
    })

    result = all_agg.join(complete_agg)
    result["windows_complete"] = complete.groupby("item_id").size()
    result["windows_total"] = df.groupby("item_id").size()
    result["windows_complete"] = result["windows_complete"].fillna(0)

    # Nullable float so a missing buy limit renders blank rather than "None".
    result["buy_limit"] = result["buy_limit"].astype("Float64")

    # Theoretical ceiling: the full buy limit at the average margin.
    result["total_profit_per_cycle"] = result["profit_per_item"] * result["buy_limit"]

    # Realistic quantity is capped by whichever binds first: the buy limit,
    # the units available to buy, or the units someone will buy from you.
    windows_in_range = hours * WINDOWS_PER_HOUR
    high_vol_per_cycle = result["high_price_volume"] / windows_in_range * WINDOWS_PER_CYCLE
    low_vol_per_cycle = result["low_price_volume"] / windows_in_range * WINDOWS_PER_CYCLE

    result["realistic_qty"] = pd.concat(
        [result["buy_limit"], high_vol_per_cycle, low_vol_per_cycle], axis=1
    ).min(axis=1)
    result["realistic_profit_per_cycle"] = result["profit_per_item"] * result["realistic_qty"]

    return result.sort_values("realistic_profit_per_cycle", ascending=False)


def load_snapshot():
    """Return the committed snapshot, or None if one hasn't been exported yet."""
    if not SNAPSHOT_PATH.exists():
        return None
    return pd.read_parquet(SNAPSHOT_PATH)
