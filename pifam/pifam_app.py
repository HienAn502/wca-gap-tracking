import json
import threading
from datetime import datetime

import altair as alt
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
               current_votes,
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
               fetched_at,
               current_votes
        FROM pifam_gap_tracking
        WHERE award_id = ?
          AND nominee_id = ?
        """,
        conn,
        params=(SPECIFIC_AWARD, SPECIFIC_NOMINEE)
    )
    conn.close()
    return df


def load_nominee_name_map():
    """
    Load nominee ID to name mapping from wca_nominees.json
    Returns a dict: {nominee_id: nominee_name}
    """
    try:
        with open('wca_nominees.json', 'r', encoding='utf-8') as f:
            wca_data = json.load(f)

        name_map = {}
        for category_data in wca_data.values():
            awards_source = category_data.get('subcategories', category_data)
            for award_id, award_data in awards_source.items():
                if isinstance(award_data, dict) and 'nominees' in award_data:
                    for nominee in award_data.get('nominees', []):
                        nominee_id = nominee.get('data_member')
                        nominee_name = nominee.get('nominee_name')
                        if nominee_id and nominee_name:
                            name_map[nominee_id] = nominee_name

        return name_map
    except FileNotFoundError:
        print("Warning: wca_nominees.json not found")
        return {}
    except Exception as e:
        print(f"Error loading nominee names: {e}")
        return {}


def calculate_gap_trends(latest, previous):
    """
    Calculate trends for gap_above and gap_below
    Returns: (gap_above_trend, gap_below_trend, gap_to_top_trend)
    Each trend is a dict with: {delta, color, arrow}
    """
    trends = {}

    # Gap Above Trend
    # If gap_above increases = bad (we're falling behind) = red up arrow
    # If gap_above decreases = good (we're catching up) = green down arrow
    if pd.notna(latest['gap_above']) and pd.notna(previous['gap_above']):
        delta = latest['gap_above'] - previous['gap_above']
        if delta > 0:
            trends['gap_above'] = {'delta': delta, 'color': 'red', 'arrow': '↑'}
        elif delta < 0:
            trends['gap_above'] = {'delta': delta, 'color': 'green', 'arrow': '↓'}
        else:
            trends['gap_above'] = {'delta': 0, 'color': 'gray', 'arrow': '→'}
    else:
        trends['gap_above'] = None

    # Gap Below Trend
    # If gap_below decreases = bad (they're catching up) = red down arrow
    # If gap_below increases = good (we're pulling away) = green up arrow
    if pd.notna(latest['gap_below']) and pd.notna(previous['gap_below']):
        delta = latest['gap_below'] - previous['gap_below']
        if delta < 0:
            trends['gap_below'] = {'delta': delta, 'color': 'red', 'arrow': '↓'}
        elif delta > 0:
            trends['gap_below'] = {'delta': delta, 'color': 'green', 'arrow': '↑'}
        else:
            trends['gap_below'] = {'delta': 0, 'color': 'gray', 'arrow': '→'}
    else:
        trends['gap_below'] = None

    # Gap To Top Trend
    # If gap_to_top increases = bad (leader pulling away) = red up arrow
    # If gap_to_top decreases = good (we're catching up) = green down arrow
    if pd.notna(latest['gap_to_top']) and pd.notna(previous['gap_to_top']):
        delta = latest['gap_to_top'] - previous['gap_to_top']
        if delta > 0:
            trends['gap_to_top'] = {'delta': delta, 'color': 'red', 'arrow': '↑'}
        elif delta < 0:
            trends['gap_to_top'] = {'delta': delta, 'color': 'green', 'arrow': '↓'}
        else:
            trends['gap_to_top'] = {'delta': 0, 'color': 'gray', 'arrow': '→'}
    else:
        trends['gap_to_top'] = None

    # Vote Trend
    if pd.notna(latest['current_votes']) and pd.notna(previous['current_votes']):
        delta = latest['current_votes'] - previous['current_votes']
        if delta > 0:
            trends['votes'] = {'delta': delta, 'color': 'green', 'arrow': '↑'}
        elif delta < 0:
            trends['votes'] = {'delta': delta, 'color': 'red', 'arrow': '↓'}
        else:
            trends['votes'] = {'delta': 0, 'color': 'gray', 'arrow': '→'}
    else:
        trends['votes'] = None

    return trends


def get_trend_markdown(st, trend):
    color = "#46aa46" if trend['color'] == "green" else "#e45f5e"
    background_color = "#143829" if trend['color'] == "green" else "#3e2428"
    arrow = trend['arrow']
    value = abs(int(trend['delta']))

    st.markdown(
        f"""
        <div style="
            display: inline-block;
            transform:translateY(-16px);
            padding:0 10px;
            font-size:0.9rem;
            color:{color};
            font-weight:600;
            background-color: {background_color};
            border-radius:10px;
        ">
            {arrow} {value:,}
        </div>
        """,
        unsafe_allow_html=True
    )


def format_ts(ts: str) -> str:
    """
    2025-12-16T14:55:01+07:00
    → 16/12/2025 • 14:55:01
    """
    try:
        dt = datetime.fromisoformat(ts)
        return dt.strftime("%d/%m/%Y • %H:%M:%S")
    except Exception:
        return ts


# ----------------------------------
# HEADER
# ----------------------------------
st.title("📊 PiFam Gap Tracker")
st.caption("Theo dõi gap theo thời gian dựa trên số liệu từ weyoung.vn")

# ----------------------------------
# SIDEBAR
# ----------------------------------
st.sidebar.header("🎯 Cài Đặt")

award_id = st.sidebar.text_input(
    "Award",
    value="Best Fandom Forever"
)

nominee_id = st.sidebar.text_input(
    "Nominee",
    value="Pifam"
)

history_limit = st.sidebar.slider(
    "Số lượng bản ghi muốn hiển thị",
    50, 1000, 300,
    step=50
)

refresh = st.sidebar.slider(
    "Chu kỳ làm mới (giây)",
    5, 1000, 10
)

# ----------------------------------
# LOAD DATA
# ----------------------------------
# Load nominee name mapping
nominee_names = load_nominee_name_map()

latest_df = load_latest_gap(DB_PATH)
gap_df = load_gap_history(DB_PATH, limit=history_limit)

if latest_df.empty:
    st.warning("Không tìm thấy dữ liệu cho giải thưởng / đề cử này.")
    st.stop()

latest = latest_df.iloc[0]

# Get previous record from history for comparison
previous = None
trends = None
if not gap_df.empty and len(gap_df) >= 1:
    previous = gap_df.iloc[-1]  # Most recent history record
    trends = calculate_gap_trends(latest, previous)

# Show total records count
if not gap_df.empty:
    st.sidebar.info(f"📊 Tổng số bản ghi lịch sử: {len(gap_df)}")

# ----------------------------------
# STATUS SECTION
# ----------------------------------
st.subheader("🚨 Trạng thái hiện tại (Cập nhật mỗi 10 giây)")

c1, c2, c3, c4, c5 = st.columns(5)

# Rank
c1.metric(
    "Xếp hạng hiện tại",
    f"#{int(latest.actual_rank)}"
)

# Votes with trend
vote_delta = None
if trends and trends.get('votes'):
    vote_trend = trends['votes']
    vote_delta = f"{vote_trend['arrow']} {abs(int(vote_trend['delta'])):,}"

c2.metric(
    "Vote hiện tại",
    f"{int(latest.current_votes):,}"
)

# Gap to above with trend
if pd.isna(latest.gap_above):
    c3.metric("Khoảng cách so với hạng trên", "🏆 LEADING")
else:
    # Get nominee name for tooltip
    nominee_above_name = nominee_names.get(str(latest.nominee_above_id), f"ID: {latest.nominee_above_id}")

    with c3:
        st.metric(
            "Khoảng cách so với hạng trên",
            f"-{int(latest.gap_above):,}",
            help=f"Đề cử trên: {nominee_above_name}"
        )

        if trends and trends.get('gap_above'):
            get_trend_markdown(st, trends['gap_above'])

# Gap to below with trend
if pd.isna(latest.gap_below):
    c4.metric("Khoảng cách so với hạng dưới", "LAST")
else:
    # Get nominee name for tooltip
    nominee_below_name = nominee_names.get(str(latest.nominee_below_id), f"ID: {latest.nominee_below_id}")


    with c4:
        st.metric(
            "Khoảng cách so với hạng dưới",
            f"+{int(latest.gap_below):,}",
            help=f"Đề cử dưới: {nominee_below_name}"
        )

        if trends and trends.get('gap_below'):
            get_trend_markdown(st, trends['gap_below'])

# Get nominee name for tooltip
nominee_top_name = nominee_names.get(str(latest.nominee_top_id), f"ID: {latest.nominee_top_id}")

with c5:
    st.metric(
        "Khoảng cách so với vị trí dẫn đầu",
        f"-{int(latest.gap_to_top):,}",
        help=f"Đề cử dẫn đầu: {nominee_top_name}"
    )

    if trends and trends.get('gap_to_top'):
        get_trend_markdown(st, trends['gap_to_top'])

st.caption(f"Cập nhật lần cuối: {format_ts(latest.fetched_at)}")
if previous is not None:
    st.caption(f"So sánh với: {format_ts(previous.fetched_at)}")


st.divider()

# ----------------------------------
# GAP TREND CHART
# ----------------------------------
st.subheader("📈 Xu hướng khoảng cách theo thời gian")

if not gap_df.empty:
    chart_df = gap_df.copy()
    chart_df["Time"] = pd.to_datetime(
        chart_df["fetched_at"],
        errors="coerce"
    )

    min_y = chart_df[["gap_above", "gap_below"]].min().min()
    max_y = chart_df[["gap_above", "gap_below"]].max().max()

    chart = (
        alt.Chart(chart_df)
        .transform_fold(
            ["gap_above", "gap_below"],
            as_=["type", "value"]
        )
        .mark_line(strokeWidth=3)
        .encode(
            x=alt.X("Time:T", title="Thời gian"),
            y=alt.Y(
                "value:Q",
                scale=alt.Scale(domain=[min_y * 0.98, max_y * 1.02]),
                title="Khoảng cách (votes)"
            ),
            color=alt.Color(
                "type:N",
                scale=alt.Scale(
                    domain=["gap_above", "gap_below"],
                    range=["#7aa4fa", "#98c501"]
                ),
                legend=alt.Legend(
                    title="Loại khoảng cách",
                    labelExpr="""
                        datum.label == 'gap_above' ? 'Khoảng cách lên hạng trên'
                        : 'Khoảng cách với hạng dưới'
                    """
                )
            ),
            strokeDash=alt.StrokeDash(
                "type:N",
                scale=alt.Scale(
                    domain=["gap_above", "gap_below"],
                    range=[[6, 4], [1, 0]]
                )
            ),
            tooltip=[
                alt.Tooltip("Time:T", title="Thời gian"),
                alt.Tooltip("type:N", title="Loại"),
                alt.Tooltip("value:Q", title="Khoảng cách", format=",")
            ]
        )
        .properties(height=350)
    )

    st.altair_chart(chart, use_container_width=True)

else:
    st.info("Đang chờ dữ liệu lịch sử để tạo biểu đồ…")

st.divider()

# ----------------------------------
# GAP HISTORY TABLE
# ----------------------------------
st.subheader("📋 Lịch sử khoảng cách (Cập nhật mỗi 10 phút)")

if gap_df.empty:
    st.info("Chưa có dữ liệu lịch sử. Dữ liệu sẽ xuất hiện khi hệ thống theo dõi thu thập các snapshot.")
else:
    # Show most recent first in the table
    table_df = gap_df[[
        "fetched_at",
        "actual_rank",
        "current_votes",
        "gap_above",
        "gap_below",
        "gap_to_top"
    ]].sort_values("fetched_at", ascending=False).rename(columns={
        "fetched_at": "Thời gian",
        "actual_rank": "Xếp Hạng",
        "current_votes": "Votes",
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

# Additional stats
if not gap_df.empty:
    st.subheader("📊 Thống kê lịch sử")
    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            "Thứ hạng cao nhất từng đạt được",
            f"#{int(gap_df['actual_rank'].min())}"
        )

    with col2:
        st.metric(
            "Khoảng cách nhỏ nhất tới vị trí dẫn đầu",
            f"{int(gap_df['gap_to_top'].min()):,}"
        )

st.divider()

# ----------------------------------
# EXPLANATION
# ----------------------------------
with st.expander("ℹ️ Cách đọc trang này", expanded=False):
    st.markdown("""
    **Định nghĩa "khoảng cách":**
    - **Khoảng cách so với hạng trên**: Số phiếu cần để vượt qua hạng cao hơn liền kề  
    - **Khoảng cách so với hạng dưới**: Số phiếu đang dẫn trước so với hạng thấp hơn liền kề  
    - **Khoảng cách so với vị trí dẫn đầu**: Số phiếu còn kém so với người đang dẫn đầu  

    **Ý nghĩa các mũi tên:**
    - **Gap Above**: 
      - ↓ Xanh = Tốt (gap giảm, đang bắt kịp)
      - ↑ Đỏ = Xấu (gap tăng, đang bị bỏ xa)
    - **Gap Below**: 
      - ↑ Xanh = Tốt (gap tăng, đang bỏ xa đối thủ dưới)
      - ↓ Đỏ = Xấu (gap giảm, đối thủ dưới đang bắt kịp)
    - **Gap To Top**: 
      - ↓ Xanh = Tốt (gap giảm, đang bắt kịp top 1)
      - ↑ Đỏ = Xấu (gap tăng, top 1 đang bỏ xa)

    **Nguồn dữ liệu:**
    - Trạng thái hiện tại: Snapshot mới nhất từ bảng `pifam_gap_tracking`
    - Bảng lịch sử và đồ thị: Các bản ghi lịch sử từ bảng `pifam_gap_history`
    - Xu hướng: So sánh snapshot hiện tại với bản ghi lịch sử gần nhất

    Hệ thống theo dõi lấy dữ liệu mới từ API theo hai tần suất khác nhau:
    - Mỗi 10 giây: Cập nhật một snapshot vào bảng trạng thái hiện tại `pifam_gap_tracking`
    - Mỗi 10 phút: Lưu một bản ghi mới vào bảng lịch sử `pifam_gap_history` (dùng cho thống kê và biểu đồ)
    """)

# ----------------------------------
# AUTO REFRESH
# ----------------------------------
time.sleep(refresh)
st.rerun()