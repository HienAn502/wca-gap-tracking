import streamlit as st
import threading
import time
import pandas as pd

from wca_vote_crawler import WCAVoteCrawler

# -------------------------------------------------
# Page config
# -------------------------------------------------
st.set_page_config(
    page_title="WCA Vote Tracker",
    layout="wide"
)

st.title("🏆 WCA Vote Tracker")

# -------------------------------------------------
# Init crawler (singleton)
# -------------------------------------------------
@st.cache_resource
def start_crawler():
    crawler = WCAVoteCrawler()
    t = threading.Thread(target=crawler.run, daemon=True)
    t.start()
    return crawler

crawler = start_crawler()

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def flatten_awards(raw_categories: dict) -> dict:
    awards = {}

    for group_key, group_val in raw_categories.items():
        if "subcategories" in group_val:
            for award_id, award in group_val["subcategories"].items():
                awards[award_id] = {
                    "award_name": award["award_name"],
                    "nominees": award["nominees"],
                    "group": group_key
                }
        else:
            for award_id, award in group_val.items():
                awards[award_id] = {
                    "award_name": award["award_name"],
                    "nominees": award["nominees"],
                    "group": group_key
                }
    return awards

# -------------------------------------------------
# Sidebar
# -------------------------------------------------
st.sidebar.header("🎯 Tracking Settings")

raw_categories = crawler.wca_nominees
awards = flatten_awards(raw_categories)

award_id = st.sidebar.selectbox(
    "Award",
    options=list(awards.keys()),
    format_func=lambda k: awards[k]["award_name"]
)

nominees = awards[award_id]["nominees"]

nominee_id = st.sidebar.selectbox(
    "Nominee",
    options=[n["data_member"] for n in nominees],
    format_func=lambda i: next(
        n["nominee_name"] for n in nominees if n["data_member"] == i
    )
)

refresh = st.sidebar.slider(
    "Refresh interval (seconds)",
    5, 60, 10
)

# -------------------------------------------------
# Data
# -------------------------------------------------
votes = crawler.get_latest_votes(award_id)

sorted_votes = sorted(
    votes.items(),
    key=lambda x: x[1],
    reverse=True
)

selected_key = f"{award_id}-{nominee_id}"

current_pos = None
for i, (nid, _) in enumerate(sorted_votes):
    if nid == selected_key:
        current_pos = i
        break

current_votes = votes[selected_key]

# -------------------------------------------------
# GAP SECTION (TOP)
# -------------------------------------------------
st.markdown("## 🚨 Vote Gap Status")

col1, col2, col3 = st.columns(3)

# Position
col1.metric(
    "Current Rank",
    f"#{current_pos + 1}",
)

# Gap above
if current_pos > 0:
    above_id, above_votes = sorted_votes[current_pos - 1]
    gap_above = above_votes - current_votes
    col2.metric(
        "Gap to Above",
        f"-{gap_above:,}",
        help="Votes needed to overtake"
    )
else:
    col2.metric("Gap to Above", "🏆 LEADING")

# Gap below
if current_pos < len(sorted_votes) - 1:
    below_id, below_votes = sorted_votes[current_pos + 1]
    gap_below = current_votes - below_votes
    col3.metric(
        "Gap to Below",
        f"+{gap_below:,}",
        help="Votes ahead"
    )
else:
    col3.metric("Gap to Below", "LAST")

st.divider()

# -------------------------------------------------
# Build table data
# -------------------------------------------------
table_data = []

for rank, (nid, v) in enumerate(sorted_votes, start=1):
    nominee_name = next(
        n["nominee_name"]
        for n in nominees
        if f"{award_id}-{n['data_member']}" == nid
    )

    table_data.append({
        "Rank": rank,
        "Nominee": nominee_name,
        "Votes": v,
        "Gap vs Top": sorted_votes[0][1] - v
    })

df = pd.DataFrame(table_data)

# -------------------------------------------------
# Ranking Table (BOTTOM)
# -------------------------------------------------
st.subheader("📋 Full Ranking")

def highlight_selected(row):
    selected_name = next(
        n["nominee_name"] for n in nominees if n["data_member"] == nominee_id
    )
    return [
        "background-color: #ffeaa7" if row["Nominee"] == selected_name else ""
    ] * len(row)

st.dataframe(
    df.style.apply(highlight_selected, axis=1),
    use_container_width=True,
    height=450
)

# -------------------------------------------------
# Chart
# -------------------------------------------------
st.subheader("📊 Vote Comparison")

top_n = st.slider("Show Top N", 3, len(df), 10)

chart_df = (
    df.head(top_n)
      .set_index("Nominee")[["Votes"]]
)

st.bar_chart(chart_df)

# -------------------------------------------------
# Auto refresh
# -------------------------------------------------
time.sleep(refresh)
st.rerun()
