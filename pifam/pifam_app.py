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
latest_df = load_latest_gap(DB_PATH)
gap_df = load_gap_history(DB_PATH, limit=history_limit)

if latest_df.empty:
    st.warning("Không tìm thấy dữ liệu cho giải thưởng / đề cử này.")
    st.stop()

latest = latest_df.iloc[0]

# Show total records count
if not gap_df.empty:
    st.sidebar.info(f"📊 Tổng số bản ghi lịch sử: {len(gap_df)}")

# ----------------------------------
# STATUS SECTION
# ----------------------------------
st.subheader("🚨 Trạng thái hiện tại (Cập nhật mỗi 10 giây)")

c1, c2, c3, c4 = st.columns(4)

# Rank
c1.metric(
    "Xếp hạng hiện tại",
    f"#{int(latest.actual_rank)}"
)

# Gap to above
if pd.isna(latest.gap_above):
    c2.metric("Khoảng cách so với hạng trên", "🏆 LEADING")
else:
    c2.metric(
        "Khoảng cách so với hạng trên",
        f"-{int(latest.gap_above):,}",
        help=f"ID đề cử trên: {latest.nominee_above_id}"
    )

# Gap to below
if pd.isna(latest.gap_below):
    c3.metric("Khoảng cách so với hạng dưới", "LAST")
else:
    c3.metric(
        "Khoảng cách so với hạng dưới",
        f"+{int(latest.gap_below):,}",
        help=f"ID đề cử dưới: {latest.nominee_below_id}"
    )

# Gap to top
c4.metric(
    "Khoảng cách so với vị trí dẫn đầu",
    f"-{int(latest.gap_to_top):,}",
    help=f"ID đề cử dẫn đầu: {latest.nominee_top_id}"
)

st.caption(f"Cập nhật lần cuối: {latest.fetched_at}")

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
st.subheader("📈 Xu hướng khoảng cách theo thời gian")

if not gap_df.empty:
    chart_df = gap_df.copy()
    chart_df["Time"] = pd.to_datetime(
        chart_df["fetched_at"],
        format="ISO8601",
        errors="coerce"
    )
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
else:
    st.info("Đang chờ dữ liệu lịch sử để tạo biểu đồ…")

st.divider()
# ----------------------------------
# EXPLANATION
# ----------------------------------
with st.expander("ℹ️ Cách đọc trang này", expanded=False):
    st.markdown("""
    **Định nghĩa “khoảng cách”:**
    - **Khoảng cách so với hạng trên**: Số phiếu cần để vượt qua hạng cao hơn liền kề  
    - **Khoảng cách so với hạng dưới**: Số phiếu đang dẫn trước so với hạng thấp hơn liền kề  
    - **Gap to Top**: Số phiếu còn kém so với người đang dẫn đầu  

    **Data sources:**
    - Current Status: Snapshot mới nhất từ bảng `pifam_gap_tracking`
    - History & Charts: Các bản ghi lịch sử từ bảng `pifam_gap_history`

    Hệ thống theo dõi lấy dữ liệu mới từ API theo hai tần suất khác nhau:
    - Mỗi 10 giây: Cập nhật một snapshot vào bảng trạng thái hiện tại `pifam_gap_tracking`
    - Mỗi 10 phút: Lưu một bản ghi mới vào bảng lịch sử `pifam_gap_history` (dùng cho thống kê và biểu đồ)
    """)

# ----------------------------------
# AUTO REFRESH
# ----------------------------------
time.sleep(refresh)
st.rerun()