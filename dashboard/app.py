import os

import pandas as pd
import streamlit as st
from databricks import sql
from dotenv import load_dotenv

load_dotenv()


def get_secret(key):
    try:  # this try except allows the app to run locally without streamlit secrets, but still use them in deployment
        return st.secrets[key]
    except Exception:
        return os.getenv(key)

WINDOWS_PER_HOUR = 12
WINDOWS_PER_CYCLE = 48  # a buy limit resets every 4 hours

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


@st.cache_data(ttl=300)
def load_data(hours):
    with sql.connect(
        server_hostname=get_secret("DATABRICKS_HOST"),
        http_path=get_secret("DATABRICKS_HTTP_PATH"),
        access_token=get_secret("DATABRICKS_TOKEN"),
    ) as connection:
        df = pd.read_sql(
            "SELECT * FROM osrs_pipeline.gold.flip_opportunities "
            f"WHERE time_window >= (current_timestamp() - INTERVAL {hours} HOURS)",
            connection,
        )

    if df.empty:
        return pd.DataFrame(columns=DISPLAY_COLUMNS)

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


st.set_page_config(page_title="OSRS Flip Opportunities", layout="wide")
st.title("OSRS Flip Opportunities")

hours = st.selectbox("Time range", [1, 3, 6, 12, 24], index=2)
data = load_data(hours)

if data.empty:
    st.warning(
        f"No market data in the last {hours}h. The pipeline may not have run recently — "
        "try a longer range."
    )
else:
    st.caption(
        f"{len(data):,} items traded in the last {hours}h. "
        "Sorted by realistic profit per 4-hour buy cycle."
    )
    st.dataframe(
        data[DISPLAY_COLUMNS],
        column_config=COLUMN_CONFIG,
        use_container_width=True,
        hide_index=True,
    )