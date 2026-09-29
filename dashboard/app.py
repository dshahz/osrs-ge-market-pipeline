import logging
import os
import time

import streamlit as st
from dotenv import load_dotenv
from market_data import TIME_RANGES, LiveDataUnavailable, aggregate, load_snapshot, query_gold

load_dotenv()

logger = logging.getLogger("osrs_dashboard")

REPO_URL = "https://github.com/dshahz/osrs-ge-market-pipeline"

# After a failed connection, skip live queries for this long and serve the
# snapshot. Each time range is its own cache entry, so without this every click
# on the selector would sit through another failed connection attempt.
BREAKER_COOLDOWN_SECONDS = 300


def get_secret(key):
    try:  # this try except allows the app to run locally without streamlit secrets, but still use them in deployment
        return st.secrets[key]
    except Exception:
        return os.getenv(key)

DISPLAY_COLUMNS = [
    "item_name",
    "realistic_profit_per_cycle",
    "profit_per_item",
    "profit_per_item_pct",
    "avg_low_price",
    "avg_high_price",
    "realistic_qty",
    "buy_limit",
    "low_price_volume",
    "high_price_volume",
    "windows_complete",
    "windows_total",
]

COLUMN_CONFIG = {
    "item_name": st.column_config.TextColumn("Item"),
    "realistic_profit_per_cycle": st.column_config.NumberColumn(
        "Profit / cycle",
        format="%,.0f",
        help="Average margin multiplied by the realistic quantity you could actually trade.",
    ),
    "profit_per_item": st.column_config.NumberColumn(
        "Margin / item",
        format="%,.0f",
        help="Average net margin per item after the 2% GE sell tax, over windows where both "
             "a buy and a sell price were observed.",
    ),
    "profit_per_item_pct": st.column_config.NumberColumn("Return %", format="%.2f"),
    "avg_low_price": st.column_config.NumberColumn("Buy at", format="%,.0f"),
    "avg_high_price": st.column_config.NumberColumn("Sell at", format="%,.0f"),
    "realistic_qty": st.column_config.NumberColumn(
        "Realistic qty",
        format="%,.0f",
        help="The lesser of the buy limit, the units available to buy, and the units "
             "someone will buy from you — scaled to one 4-hour cycle.",
    ),
    "buy_limit": st.column_config.NumberColumn(
        "Buy limit",
        format="%,.0f",
        help="Blank means the API doesn't publish a limit for this item — undocumented, "
             "not unlimited.",
    ),
    "low_price_volume": st.column_config.NumberColumn("Sell volume", format="%,.0f"),
    "high_price_volume": st.column_config.NumberColumn("Buy volume", format="%,.0f"),
    "windows_complete": st.column_config.NumberColumn(
        "Windows w/ both",
        format="%d",
        help="Number of 5-minute windows where both a buy and a sell price were observed. "
             "A low count means the margin rests on very few observations.",
    ),
    "windows_total": st.column_config.NumberColumn("Windows traded", format="%d"),
}


@st.cache_data(ttl=300, show_spinner="Loading live market data…")
def load_live(hours):
    rows = query_gold(
        hours,
        host=get_secret("DATABRICKS_HOST"),
        http_path=get_secret("DATABRICKS_HTTP_PATH"),
        token=get_secret("DATABRICKS_TOKEN"),
    )
    return aggregate(rows, hours)


@st.cache_data(show_spinner=False)
def cached_snapshot():
    # The file only changes on a new commit, which redeploys the app anyway.
    return load_snapshot()


@st.cache_resource
def live_breaker():
    # Shared across every visitor's session, unlike st.session_state.
    return {"open_until": 0.0}


def get_data(hours):
    """Return (data, snapshot_at). snapshot_at is None when the data is live.

    Falls back to the committed snapshot when the warehouse is unreachable, and
    returns (None, None) if there is no snapshot to fall back to.
    """
    breaker = live_breaker()
    if time.monotonic() >= breaker["open_until"]:
        try:
            return load_live(hours), None
        except LiveDataUnavailable:
            logger.exception("Live data unavailable; serving snapshot")
            breaker["open_until"] = time.monotonic() + BREAKER_COOLDOWN_SECONDS

    snapshot = cached_snapshot()
    if snapshot is None:
        return None, None
    rows = snapshot[snapshot["hours"] == hours]
    return rows, snapshot["snapshot_at"].iloc[0]


st.set_page_config(page_title="OSRS Flip Opportunities", layout="wide")
st.title("OSRS Flip Opportunities")

hours = st.selectbox("Time range", TIME_RANGES, index=2)
data, snapshot_at = get_data(hours)

if data is None:
    st.info(
        "Live market data is temporarily unavailable. Please check back soon. In the "
        f"meantime, the pipeline's architecture and design decisions are on [GitHub]({REPO_URL})."
    )
    st.stop()

if snapshot_at is None:
    period = f"the last {hours}h"
else:
    ts = snapshot_at
    st.info(
        "Live data from Databricks is temporarily unavailable, so you're viewing a snapshot "
        f"taken {ts:%b} {ts.day}, {ts:%Y} at {ts:%H:%M} UTC."
    )
    period = f"the {hours}h before the snapshot"

if data.empty:
    st.warning(
        f"No market data in {period}. The pipeline may not have run recently — "
        "try a longer range."
    )
else:
    st.caption(
        f"{len(data):,} items traded in {period}. "
        "Sorted by realistic profit per 4-hour buy cycle."
    )
    st.dataframe(
        data[DISPLAY_COLUMNS],
        column_config=COLUMN_CONFIG,
        width="stretch",
        hide_index=True,
    )
