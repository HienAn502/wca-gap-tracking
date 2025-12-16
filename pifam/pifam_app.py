import threading

import streamlit as st
import pandas as pd
import sqlite3
import time
from pifam_gap_tracker import PiFamGapTracker, SPECIFIC_AWARD, SPECIFIC_NOMINEE

# ----------------------------------
# CONFIG
# ----------------------------------
DB_PATH = "wca_votes.db"

st.set_page_config(
    page_title="PiFam Gap Tracker",
    layout="wide"
)


# -------------------------------------------------
# Init gap_tracker (singleton)
# -------------------------------------------------
@st.cache_resource
def start_gap_tracker():
    gap_tracker = PiFamGapTracker()
    t = threading.Thread(target=gap_tracker.run, daemon=True)
    t.start()
    return gap_tracker


gap_tracker = start_gap_tracker()


# ----------------------------------
# HELPERS
# ----------------------------------
def load_gap_history(db_path, limit=300):
    """Load from pifam_gap_history table instead of pifam_gap_tracking"""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        """
        SELECT award_id,
               nominee_id,
               actual_rank,
               gap_above,
               nominee_above_id,
               gap_below,
               nominee_below_id,
               gap_to_top,
               nominee_top_id,
               fetched_at
        FROM pifam_gap_history
        WHERE award_id = ?
          AND nominee_id = ?
        ORDER BY fetched_at ASC LIMIT ?
        """,
        conn,
        params=(SPECIFIC_AWARD, SPECIFIC_NOMINEE, limit)
    )
    conn.close()
    return df


def load_latest_gap(db_path):
    """Load latest snapshot from pifam_gap_tracking"""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        """
        SELECT award_id,
               nominee_id,
               actual_rank,
               gap_above,
               nominee_above_id,
               gap_below,
               nominee_below_id,
               gap_to_top,
               nominee_top_id,
               fetched_at
        FROM pifam_gap_tracking
        WHERE award_id = ?
          AND nominee_id = ?
        """,
        conn,
        params=(SPECIFIC_AWARD, SPECIFIC_NOMINEE)
    )
    conn.close()
    return df


def load_nominee_name_map(db_path):
    """
    Optional helper if you later want to map nominee_id -> name
    Currently IDs are shown directly in tooltip
    """
    return {}


# ----------------------------------
# HEADER
# ----------------------------------
st.title("📊 PiFam Gap Tracker")
st.caption("Tracking ranking gaps over time based on stored WCA vote snapshots")

# ----------------------------------
# SIDEBAR
# ----------------------------------
st.sidebar.header("🎯 Tracking Settings")

award_id = st.sidebar.text_input(
    "Award",
    value="Best Fandom Forever"
)

nominee_id = st.sidebar.text_input(
    "Nominee",
    value="Pifam"
)

history_limit = st.sidebar.slider(
    "History records to display",
    50, 1000, 300,
    step=50
)

refresh = st.sidebar.slider(
    "Refresh interval (seconds)",
    5, 1000, 10
)

# ----------------------------------
# LOAD DATA
# ----------------------------------
latest_df = load_latest_gap(DB_PATH)
gap_df = load_gap_history(DB_PATH, limit=history_limit)

if latest_df.empty:
    st.warning("No gap data found for this award / nominee.")
    st.stop()

latest = latest_df.iloc[0]

# Show total records count
if not gap_df.empty:
    st.sidebar.info(f"📊 Total historical records: {len(gap_df)}")

# ----------------------------------
# STATUS SECTION
# ----------------------------------
st.subheader("🚨 Current Status")

c1, c2, c3, c4 = st.columns(4)

# Rank
c1.metric(
    "Current Rank",
    f"#{int(latest.actual_rank)}"
)

# Gap to above
if pd.isna(latest.gap_above):
    c2.metric("Gap to Above", "🏆 LEADING")
else:
    c2.metric(
        "Gap to Above",
        f"-{int(latest.gap_above):,}",
        help=f"Above nominee ID: {latest.nominee_above_id}"
    )

# Gap to below
if pd.isna(latest.gap_below):
    c3.metric("Gap to Below", "LAST")
else:
    c3.metric(
        "Gap to Below",
        f"+{int(latest.gap_below):,}",
        help=f"Below nominee ID: {latest.nominee_below_id}"
    )

# Gap to top
c4.metric(
    "Gap to Top",
    f"-{int(latest.gap_to_top):,}",
    help=f"Leader nominee ID: {latest.nominee_top_id}"
)

st.caption(f"Last updated: {latest.fetched_at}")

st.divider()

# ----------------------------------
# GAP HISTORY TABLE
# ----------------------------------
st.subheader("📋 Gap History")

if gap_df.empty:
    st.info("No historical data yet. Data will appear as the tracker collects snapshots.")
else:
    # Show most recent first in the table
    table_df = gap_df[[
        "fetched_at",
        "actual_rank",
        "gap_above",
        "gap_below",
        "gap_to_top"
    ]].sort_values("fetched_at", ascending=False).rename(columns={
        "fetched_at": "Time",
        "actual_rank": "Rank",
        "gap_above": "Gap ↑",
        "gap_below": "Gap ↓",
        "gap_to_top": "Gap to Top"
    })

    st.dataframe(
        table_df,
        use_container_width=True,
        height=350
    )

st.divider()

# ----------------------------------
# GAP TREND CHART
# ----------------------------------
st.subheader("📈 Gap Trend Over Time")

if not gap_df.empty:
    chart_df = gap_df.copy()
    chart_df["Time"] = pd.to_datetime(chart_df["fetched_at"])
    chart_df = chart_df.set_index("Time")

    st.line_chart(
        chart_df[[
            "gap_above",
            "gap_below",
        ]],
        height=350
    )

    st.divider()

    # Additional stats
    st.subheader("📊 Historical Statistics")
    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            "Best Rank Achieved",
            f"#{int(gap_df['actual_rank'].min())}"
        )

    with col2:
        st.metric(
            "Smallest Gap to Top",
            f"{int(gap_df['gap_to_top'].min()):,}"
        )
else:
    st.info("Waiting for historical data to generate charts...")

st.divider()
# ----------------------------------
# EXPLANATION
# ----------------------------------
with st.expander("ℹ️ How to read this dashboard", expanded=False):
    st.markdown("""
    **Gap definitions:**
    - **Gap to Above**: Votes needed to overtake the next higher rank  
    - **Gap to Below**: Votes lead over the next lower rank  
    - **Gap to Top**: Votes behind the current leader  

    **Data sources:**
    - Current Status: Latest snapshot from `pifam_gap_tracking` table
    - History & Charts: Historical records from `pifam_gap_history` table

    The tracker fetches fresh data from the API every 10 seconds and saves:
    - A snapshot to the latest table (updated)
    - A new record to the history table (appended)
    """)

# ----------------------------------
# AUTO REFRESH
# ----------------------------------
time.sleep(refresh)
st.rerun()