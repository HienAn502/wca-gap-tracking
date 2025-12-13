import requests
import json
from datetime import datetime
import wca_nominee_crawler
import sqlite3
import time

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Ho_Chi_Minh")
except ImportError:
    from pytz import timezone
    TZ = timezone("Asia/Ho_Chi_Minh")

class WCAVoteCrawler:
    def __init__(self, db_path='wca_votes.db'):
        self.db_path = db_path
        self.db_conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db_conn.row_factory = sqlite3.Row
        self._init_db()
        self.load_wca_nominees()

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

    def _init_db(self):
        cursor = self.db_conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS votes_latest (
                award_id TEXT NOT NULL,
                nominee_id TEXT NOT NULL,
                vote_count TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (award_id, nominee_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS votes_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                award_id TEXT NOT NULL,
                nominee_id TEXT NOT NULL,
                vote_count TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_votes_latest_award 
            ON votes_latest(award_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_votes_history_award 
            ON votes_history(award_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_votes_history_award_nominee 
            ON votes_history(award_id, nominee_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_votes_history_fetched 
            ON votes_history(fetched_at)
        ''')
        
        self.db_conn.commit()

    def load_wca_nominees(self):
        try:
            with open('wca_nominees.json', 'r', encoding='utf-8') as f:
                self.wca_nominees = json.load(f)
        except FileNotFoundError:
            self.wca_nominees = wca_nominee_crawler.CrawlNominees().crawl_nominees()
        
        self.lst_ids = []
        
        for category_data in self.wca_nominees.values():
            awards_source = category_data.get('subcategories', category_data)
            for award_id, award_data in awards_source.items():
                if isinstance(award_data, dict) and 'nominees' in award_data:
                    for nominee in award_data.get('nominees', []):
                        nominee_id = nominee.get('data_member')
                        if nominee_id:
                            self.lst_ids.append(f"w{award_id}-{nominee_id}")

    def _save_votes(self, votes_data):
        cursor = self.db_conn.cursor()
        now = datetime.now(TZ).isoformat()

        rows_latest = []
        rows_history = []
        
        for award_id, nominees_data in votes_data.items():
            for nominee_id, vote_info in nominees_data.items():
                vote_count = vote_info.get('count', '0')
                rows_latest.append((award_id, nominee_id, vote_count, now))
                rows_history.append((award_id, nominee_id, vote_count, now))

        cursor.executemany("""
            INSERT INTO votes_latest (
                award_id, nominee_id, vote_count, fetched_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(award_id, nominee_id) DO UPDATE SET
                vote_count = excluded.vote_count,
                fetched_at = excluded.fetched_at
        """, rows_latest)

        cursor.executemany("""
            INSERT INTO votes_history (
                award_id, nominee_id, vote_count, fetched_at
            ) VALUES (?, ?, ?, ?)
        """, rows_history)
        
        print(f"Saved votes for {len(rows_latest)} nominees at {now}")
        self.db_conn.commit()

    def crawl_votes(self):        
        api_url = f"https://api.weyoung.vn/vote-token.htm?m=get-vote&lstId={''.join(self.lst_ids)}"

        try:
            response = self.session.get(api_url, headers=self.headers)
            vote_data = response.json()

            if 'Success' not in vote_data or vote_data['Success'] != True:
                return None
            
            votes_data = {}
            
            for item in vote_data.get('Data', []):
                award_id = str(item['a'])
                nominee_id = str(item['m'])
                vote_count = str(item['list'][0]['v']) if item.get('list') else '0'

                if award_id not in votes_data:
                    votes_data[award_id] = {}
                
                votes_data[award_id][nominee_id] = {
                    'count': vote_count
                }

            if votes_data:
                self._save_votes(votes_data)
            
            return votes_data
        except Exception as e:
            print(f"Error crawling votes: {e}")
            return None

    def get_vote_history(self, award_id, limit=5, start_at=None):
        award_data = None
        award_name = None
        
        for category_data in self.wca_nominees.values():
            awards_source = category_data.get('subcategories', category_data)
            if award_id in awards_source:
                award_data = awards_source[award_id]
                award_name = award_data.get('award_name', '')
                break
        
        if not award_data or 'nominees' not in award_data:
            return {"nominees": [], "award_name": award_name or ""}
        
        nominees_list = award_data.get('nominees', [])
        
        cursor = self.db_conn.cursor()
        history_query = """
            SELECT nominee_id, vote_count, fetched_at
            FROM votes_history
            WHERE award_id = ?
        """
        history_params = [award_id]
        
        if start_at:
            try:
                dt = datetime.fromisoformat(start_at.replace(" ", "T"))
                start_at_norm = dt.replace(microsecond=0).isoformat()
            except Exception:
                start_at_norm = start_at
            history_query += " AND fetched_at >= ?"
            history_params.append(start_at_norm)
        
        history_query += " ORDER BY nominee_id, fetched_at ASC"
        
        cursor.execute(history_query, history_params)
        
        vote_history_by_nominee = {}
        vote_counts = {}
        
        for row in cursor.fetchall():
            nominee_id = row["nominee_id"]
            vote_count = row["vote_count"]
            timestamp = datetime.fromisoformat(row["fetched_at"]).strftime("%Y-%m-%d %H:%M:%S")
            
            if nominee_id not in vote_history_by_nominee:
                vote_history_by_nominee[nominee_id] = []
            
            vote_history_by_nominee[nominee_id].append({
                "vote_count": vote_count,
                "timestamp": timestamp
            })
            
            vote_counts[nominee_id] = {
                "vote_count": vote_count,
                "timestamp": timestamp
            }
        
        nominees_with_votes = []
        
        for nominee in nominees_list:
            nominee_id = nominee.get('data_member', '')
            if not nominee_id:
                continue
            
            current_vote = vote_counts.get(nominee_id, {"vote_count": "0", "fetched_at": None})
            
            vote_history = vote_history_by_nominee.get(nominee_id, [])
            
            nominees_with_votes.append({
                **nominee,
                "current_vote_count": current_vote["vote_count"],
                "last_update_time": current_vote["timestamp"],
                "vote_history": vote_history
            })
        
        nominees_with_votes.sort(
            key=lambda x: int(x.get("current_vote_count", "0")), 
            reverse=True
        )
        
        if limit and limit > 0:
            nominees_with_votes = nominees_with_votes[:limit]
        
        return {
            "award_id": award_id,
            "award_name": award_name or "",
            "nominees": nominees_with_votes
        }

    def run(self):
        while True:
            now = datetime.now(TZ)
            if now.second % 10 == 0:
                try:
                    self.crawl_votes()
                except Exception as e:
                    print(f"Error crawling votes: {e}")
            time.sleep(1)

    def close(self):
        try:
            self.db_conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    crawler = WCAVoteCrawler()
    try:
        crawler.run()
    except KeyboardInterrupt:
        print("Stopping crawler...")
    finally:
        crawler.close()