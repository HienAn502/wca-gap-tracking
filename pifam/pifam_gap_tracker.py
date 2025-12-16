import sqlite3
import time
import requests
import json
from datetime import datetime

SPECIFIC_AWARD = "3"
SPECIFIC_NOMINEE = "34"
DB_PATH = "wca_votes.db"

try:
    from zoneinfo import ZoneInfo

    TZ = ZoneInfo("Asia/Ho_Chi_Minh")
except ImportError:
    from pytz import timezone

    TZ = timezone("Asia/Ho_Chi_Minh")


class PiFamGapTracker:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.db_conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db_conn.row_factory = sqlite3.Row
        self._init_db()

        # Track last history save time
        self.last_history_save = None

        # Initialize session for API calls
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
        })

        self.headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://weyoung.vn",
            "Referer": "https://weyoung.vn/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "sec-ch-ua": '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"'
        }

        # Load nominees to get lst_ids
        self.load_wca_nominees()

    def _init_db(self):
        cursor = self.db_conn.cursor()

        # Latest snapshot table
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS pifam_gap_tracking
                       (
                           award_id
                           TEXT
                           NOT
                           NULL,
                           nominee_id
                           TEXT
                           NOT
                           NULL,

                           actual_rank
                           INTEGER,

                           current_votes
                           INTEGER,

                           gap_above
                           INTEGER,
                           nominee_above_id
                           TEXT,

                           gap_below
                           INTEGER,
                           nominee_below_id
                           TEXT,

                           gap_to_top
                           INTEGER,
                           nominee_top_id
                           TEXT,

                           fetched_at
                           TEXT
                           NOT
                           NULL,
                           PRIMARY
                           KEY
                       (
                           award_id,
                           nominee_id
                       )
                           )
                       """)

        # Add current_votes column if it doesn't exist (for existing deployments)
        try:
            cursor.execute("""
                           ALTER TABLE pifam_gap_tracking
                               ADD COLUMN current_votes INTEGER
                           """)
            print("✓ Added current_votes column to pifam_gap_tracking")
        except sqlite3.OperationalError:
            # Column already exists
            pass

        # History table - saves every 10 minutes
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS pifam_gap_history
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           award_id
                           TEXT
                           NOT
                           NULL,
                           nominee_id
                           TEXT
                           NOT
                           NULL,

                           actual_rank
                           INTEGER,

                           current_votes
                           INTEGER,

                           gap_above
                           INTEGER,
                           nominee_above_id
                           TEXT,

                           gap_below
                           INTEGER,
                           nominee_below_id
                           TEXT,

                           gap_to_top
                           INTEGER,
                           nominee_top_id
                           TEXT,

                           fetched_at
                           TEXT
                           NOT
                           NULL
                       )
                       """)

        # Add current_votes column to history table if it doesn't exist
        try:
            cursor.execute("""
                           ALTER TABLE pifam_gap_history
                               ADD COLUMN current_votes INTEGER
                           """)
            print("✓ Added current_votes column to pifam_gap_history")
        except sqlite3.OperationalError:
            # Column already exists
            pass

        # Create indexes for better query performance
        cursor.execute("""
                       CREATE INDEX IF NOT EXISTS idx_pifam_history_award_nominee
                           ON pifam_gap_history(award_id, nominee_id)
                       """)

        cursor.execute("""
                       CREATE INDEX IF NOT EXISTS idx_pifam_history_fetched
                           ON pifam_gap_history(fetched_at)
                       """)

        self.db_conn.commit()

    def load_wca_nominees(self):
        try:
            with open('wca_nominees.json', 'r', encoding='utf-8') as f:
                self.wca_nominees = json.load(f)
        except FileNotFoundError:
            print("Warning: wca_nominees.json not found")
            self.wca_nominees = {}

        self.lst_ids = []

        for category_data in self.wca_nominees.values():
            awards_source = category_data.get('subcategories', category_data)
            for award_id, award_data in awards_source.items():
                if isinstance(award_data, dict) and 'nominees' in award_data:
                    for nominee in award_data.get('nominees', []):
                        nominee_id = nominee.get('data_member')
                        if nominee_id:
                            self.lst_ids.append(f"w{award_id}-{nominee_id}")

    def fetch_votes_from_api(self):
        """Fetch vote data from API like wca_vote_crawler does"""
        api_url = f"https://api.weyoung.vn/vote-token.htm?m=get-vote&lstId={''.join(self.lst_ids)}"

        try:
            response = self.session.get(api_url, headers=self.headers)
            vote_data = response.json()

            if 'Success' not in vote_data or vote_data['Success'] != True:
                print("API response not successful")
                return None

            votes_data = {}

            for item in vote_data.get('Data', []):
                award_id = str(item['a'])
                nominee_id = str(item['m'])
                vote_count = str(item['list'][0]['v']) if item.get('list') else '0'

                if award_id not in votes_data:
                    votes_data[award_id] = {}

                votes_data[award_id][nominee_id] = int(vote_count)

            return votes_data

        except Exception as e:
            print(f"Error fetching votes from API: {e}")
            return None

    def should_save_history(self):
        """Check if 10 minutes have passed since last history save"""
        now = datetime.now(TZ)

        if self.last_history_save is None:
            return True

        time_diff = (now - self.last_history_save).total_seconds()
        return time_diff >= 600  # 600 seconds = 10 minutes

    def calculate_and_save_gap(self):
        # Fetch votes from API instead of database
        votes_data = self.fetch_votes_from_api()

        if not votes_data or SPECIFIC_AWARD not in votes_data:
            print("No vote data for specific award.")
            return

        award_votes = votes_data[SPECIFIC_AWARD]

        # Sort by votes descending
        ranking = sorted(award_votes.items(), key=lambda x: x[1], reverse=True)

        # Find specific nominee
        ids = [r[0] for r in ranking]
        if SPECIFIC_NOMINEE not in ids:
            print("Specific nominee not found.")
            return

        idx = ids.index(SPECIFIC_NOMINEE)
        actual_rank = idx + 1
        current_votes = ranking[idx][1]

        nominee_above_id = None
        gap_above = None
        if idx > 0:
            nominee_above_id, above_votes = ranking[idx - 1]
            gap_above = above_votes - current_votes

        nominee_below_id = None
        gap_below = None
        if idx < len(ranking) - 1:
            nominee_below_id, below_votes = ranking[idx + 1]
            gap_below = current_votes - below_votes

        nominee_top_id, top_votes = ranking[0]
        gap_to_top = top_votes - current_votes

        now = datetime.now(TZ).isoformat(timespec="seconds")

        cursor = self.db_conn.cursor()

        # Always update latest snapshot (every 10s)
        cursor.execute("""
                       INSERT INTO pifam_gap_tracking (award_id,
                                                       nominee_id,
                                                       actual_rank,
                                                       current_votes,
                                                       gap_above,
                                                       nominee_above_id,
                                                       gap_below,
                                                       nominee_below_id,
                                                       gap_to_top,
                                                       nominee_top_id,
                                                       fetched_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(award_id, nominee_id)
            DO
                       UPDATE SET
                           actual_rank = excluded.actual_rank,
                           current_votes = excluded.current_votes,
                           gap_above = excluded.gap_above,
                           nominee_above_id = excluded.nominee_above_id,
                           gap_below = excluded.gap_below,
                           nominee_below_id = excluded.nominee_below_id,
                           gap_to_top = excluded.gap_to_top,
                           nominee_top_id = excluded.nominee_top_id,
                           fetched_at = excluded.fetched_at
                       """, (
                           SPECIFIC_AWARD,
                           SPECIFIC_NOMINEE,
                           actual_rank,
                           current_votes,
                           gap_above,
                           nominee_above_id,
                           gap_below,
                           nominee_below_id,
                           gap_to_top,
                           nominee_top_id,
                           now
                       ))

        # Only insert into history every 10 minutes
        save_to_history = self.should_save_history()

        if save_to_history:
            cursor.execute("""
                           INSERT INTO pifam_gap_history (award_id,
                                                          nominee_id,
                                                          actual_rank,
                                                          current_votes,
                                                          gap_above,
                                                          nominee_above_id,
                                                          gap_below,
                                                          nominee_below_id,
                                                          gap_to_top,
                                                          nominee_top_id,
                                                          fetched_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           """, (
                               SPECIFIC_AWARD,
                               SPECIFIC_NOMINEE,
                               actual_rank,
                               current_votes,
                               gap_above,
                               nominee_above_id,
                               gap_below,
                               nominee_below_id,
                               gap_to_top,
                               nominee_top_id,
                               now
                           ))
            self.last_history_save = datetime.now(TZ)

        self.db_conn.commit()

        print(f"[PiFam Gap] Updated at {now}")
        print(f"Rank: #{actual_rank}")
        print(f"Current votes: {current_votes:,}")
        print(f"Gap above: {gap_above}")
        print(f"Gap below: {gap_below}")
        print(f"Gap to top: {gap_to_top}")
        if save_to_history:
            print("✓ Saved to history")

    def get_gap_history(self, limit=None, start_at=None):
        """Query historical gap data"""
        cursor = self.db_conn.cursor()

        query = """
                SELECT actual_rank, \
                       current_votes, \
                       gap_above, \
                       gap_below, \
                       gap_to_top,
                       nominee_above_id, \
                       nominee_below_id, \
                       nominee_top_id, \
                       fetched_at
                FROM pifam_gap_history
                WHERE award_id = ? \
                  AND nominee_id = ? \
                """
        params = [SPECIFIC_AWARD, SPECIFIC_NOMINEE]

        if start_at:
            query += " AND fetched_at >= ?"
            params.append(start_at)

        query += " ORDER BY fetched_at ASC"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        cursor.execute(query, params)

        return [dict(row) for row in cursor.fetchall()]

    def run(self):
        while True:
            now = datetime.now(TZ)
            if now.second % 10 == 0:
                try:
                    self.calculate_and_save_gap()
                except Exception as e:
                    print(f"Error calculating gap: {e}")
            time.sleep(1)

    def close(self):
        try:
            self.db_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    tracker = PiFamGapTracker()
    try:
        tracker.run()
    finally:
        tracker.close()