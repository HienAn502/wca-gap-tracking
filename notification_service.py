import sqlite3
import json
import time
import os
from dotenv import load_dotenv
from pywebpush import webpush, WebPushException
import wca_nominee_crawler

load_dotenv()

VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY")
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY")
VAPID_CLAIMS = {
    "sub": "mailto:lzhoang2302@gmail.com"
}

class NotificationService:
    def __init__(self, vote_crawler, nominee_filter=None):
        self.wca_vote_crawler = vote_crawler
        self.nominee_filter = nominee_filter
        self.init_db()
        self.load_nominee_metadata()

    def load_nominee_metadata(self):
        try:
            with open('wca_nominees.json', 'r', encoding='utf-8') as f:
                self.wca_nominees = json.load(f)
        except FileNotFoundError:
            self.wca_nominees = wca_nominee_crawler.CrawlNominees().crawl_nominees()
        
        self.nominee_metadata = {}
        
        for category_data in self.wca_nominees.values():
            awards_source = category_data.get('subcategories', category_data)
            for award_id, award_data in awards_source.items():
                if isinstance(award_data, dict) and 'nominees' in award_data:
                    award_name = award_data.get('award_name', '')
                    for nominee in award_data.get('nominees', []):
                        nominee_id = nominee.get('data_member')
                        if nominee_id:
                            key = f"{award_id}-{nominee_id}"
                            self.nominee_metadata[key] = {
                                "name": nominee.get('nominee_name', ''),
                                "image": nominee.get('ava_link', ''),
                                "award_id": award_id,
                                "nominee_id": nominee_id,
                                "award_name": award_name
                            }

    def init_db(self):
        conn = sqlite3.connect('subscribers.db', check_same_thread=False)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscribers
                     (endpoint TEXT PRIMARY KEY, p256dh TEXT, auth TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_preferences
                     (endpoint TEXT PRIMARY KEY,
                      nominee_filter TEXT,
                      summary_interval INTEGER DEFAULT 900,
                      updated_at TEXT,
                      FOREIGN KEY(endpoint) REFERENCES subscribers(endpoint))''')
        conn.commit()
        conn.close()

    def add_subscriber(self, sub_info):
        try:
            if not sub_info or 'endpoint' not in sub_info:
                return False, "Invalid subscription"

            endpoint = sub_info['endpoint']
            keys = sub_info.get('keys', {})
            p256dh = keys.get('p256dh')
            auth = keys.get('auth')

            conn = sqlite3.connect('subscribers.db', check_same_thread=False)
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO subscribers (endpoint, p256dh, auth) VALUES (?, ?, ?)", (endpoint, p256dh, auth))
            conn.commit()
            conn.close()
            print(f"New subscriber saved: {endpoint[:20]}...")
            return True, "Success"
        except Exception as e:
            return False, str(e)

    def get_vapid_public_key(self):
        return VAPID_PUBLIC_KEY

    def get_all_subscriptions(self):
        conn = sqlite3.connect('subscribers.db', check_same_thread=False)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM subscribers")
        rows = c.fetchall()
        conn.close()
        
        subs = []
        for row in rows:
            subs.append({
                "endpoint": row['endpoint'],
                "keys": {
                    "p256dh": row['p256dh'],
                    "auth": row['auth']
                }
            })
        return subs

    def remove_subscription(self, endpoint):
        conn = sqlite3.connect('subscribers.db', check_same_thread=False)
        c = conn.cursor()
        c.execute("DELETE FROM subscribers WHERE endpoint = ?", (endpoint,))
        c.execute("DELETE FROM user_preferences WHERE endpoint = ?", (endpoint,))
        conn.commit()
        conn.close()

    def get_user_preferences(self, endpoint):
        """Get user preferences for a specific endpoint."""
        conn = sqlite3.connect('subscribers.db', check_same_thread=False)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT nominee_filter, summary_interval FROM user_preferences WHERE endpoint = ?", (endpoint,))
        row = c.fetchone()
        conn.close()
        
        if row:
            nominee_filter = json.loads(row['nominee_filter']) if row['nominee_filter'] else []
            return {
                'nominee_filter': nominee_filter,
                'summary_interval': row['summary_interval'] or 900
            }
        return {
            'nominee_filter': [],
            'summary_interval': 900
        }

    def set_user_preferences(self, endpoint, nominee_filter, summary_interval):
        """Save or update user preferences."""
        from datetime import datetime
        try:
            conn = sqlite3.connect('subscribers.db', check_same_thread=False)
            c = conn.cursor()
            nominee_filter_json = json.dumps(nominee_filter) if nominee_filter else '[]'
            updated_at = datetime.now().isoformat()
            
            c.execute("""
                INSERT OR REPLACE INTO user_preferences 
                (endpoint, nominee_filter, summary_interval, updated_at)
                VALUES (?, ?, ?, ?)
            """, (endpoint, nominee_filter_json, summary_interval, updated_at))
            conn.commit()
            conn.close()
            return True, "Success"
        except Exception as e:
            return False, str(e)

    def get_all_user_preferences(self):
        """Get all active user preferences for notification processing."""
        conn = sqlite3.connect('subscribers.db', check_same_thread=False)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT up.endpoint, up.nominee_filter, up.summary_interval
            FROM user_preferences up
            INNER JOIN subscribers s ON up.endpoint = s.endpoint
        """)
        rows = c.fetchall()
        conn.close()
        
        preferences = {}
        for row in rows:
            nominee_filter = json.loads(row['nominee_filter']) if row['nominee_filter'] else []
            preferences[row['endpoint']] = {
                'nominee_filter': nominee_filter,
                'summary_interval': row['summary_interval'] or 900
            }
        return preferences

    def send_push_notification(self, message_body, target_endpoints=None):
        subscribers = self.get_all_subscriptions()
        if not subscribers:
            return

        # Filter subscribers if target_endpoints is specified
        if target_endpoints:
            subscribers = [sub for sub in subscribers if sub['endpoint'] in target_endpoints]
            if not subscribers:
                return

        print(f"Sending push to {len(subscribers)} subscribers...")
        
        for sub in subscribers:
            try:
                now = int(time.time())
                claims = VAPID_CLAIMS.copy()
                claims.update({
                    "iat": now - 10,
                    "exp": now + 12 * 3600
                })

                webpush(
                    subscription_info=sub,
                    data=json.dumps(message_body),
                    vapid_private_key=VAPID_PRIVATE_KEY,
                    vapid_claims=claims,
                    ttl=60
                )
            except WebPushException as ex:
                if ex.response and ex.response.status_code in [400, 410]:
                    print(f"Removing invalid subscription (status {ex.response.status_code}): {sub['endpoint'][:30]}...")
                    self.remove_subscription(sub['endpoint'])
                elif "400 Bad Request" in str(ex) or "410 Gone" in str(ex):
                     print(f"Removing invalid subscription (error message): {sub['endpoint'][:30]}...")
                     self.remove_subscription(sub['endpoint'])
                else:
                    print(f"Push failed: {ex}")
            except Exception as e:
                print(f"Error sending push: {e}")

    def get_milestone_step(self, vote_count):
        if vote_count <= 10000:
            return 1000
        elif vote_count <= 50000:
            return 5000
        elif vote_count <= 200000:
            return 10000
        elif vote_count <= 1000000:
            return 50000
        else:
            return 100000

    def check_milestone_crossed(self, old_vote, new_vote):
        if old_vote >= new_vote:
            return None
        
        step = self.get_milestone_step(new_vote)
        old_milestone = (old_vote // step) * step
        new_milestone = (new_vote // step) * step
        
        if new_milestone > old_milestone:
            return new_milestone
        
        return None

    def format_milestone(self, milestone):
        if milestone >= 1000000:
            return f"{milestone / 1000000:.1f}M"
        elif milestone >= 1000:
            return f"{milestone / 1000}K"
        else:
            return str(milestone)

    def run(self):
        user_states = {}

        while True:
            try:
                all_preferences = self.get_all_user_preferences()
                
                if not all_preferences:
                    time.sleep(10)
                    continue
                
                current_votes = self.wca_vote_crawler.get_latest_votes()
                
                if not current_votes:
                    time.sleep(10)
                    continue
                
                now = time.time()
                
                for endpoint, prefs in all_preferences.items():
                    nominee_filter = prefs.get('nominee_filter', [])
                    summary_interval = prefs.get('summary_interval', 900)
                    
                    if not nominee_filter:
                        continue
                    
                    if endpoint not in user_states:
                        user_states[endpoint] = {
                            'prev_votes': {},
                            'last_summary_time': now
                        }
                    
                    user_state = user_states[endpoint]
                    prev_votes = user_state['prev_votes']
                    
                    nominee_ids = [f"{award_id}-{nominee_id}" for award_id, nominee_id in nominee_filter]
                    nominee_ids = [nid for nid in nominee_ids if nid in self.nominee_metadata and nid in current_votes]
                    
                    if not nominee_ids:
                        continue
                    
                    nominees_by_award = {}
                    for nid in nominee_ids:
                        meta = self.nominee_metadata.get(nid, {})
                        award_id = meta.get("award_id")
                        if award_id:
                            if award_id not in nominees_by_award:
                                nominees_by_award[award_id] = []
                            nominees_by_award[award_id].append(nid)
                    
                    if not prev_votes:
                        prev_votes = {nid: current_votes.get(nid, 0) for nid in nominee_ids}
                        user_state['prev_votes'] = prev_votes
                        time.sleep(1)
                        continue
                    
                    for award_id, award_nominee_ids in nominees_by_award.items():
                        if not award_nominee_ids:
                            continue
                        
                        curr_ranking = sorted(award_nominee_ids, key=lambda x: current_votes.get(x, 0), reverse=True)
                        prev_ranking = sorted(award_nominee_ids, key=lambda x: prev_votes.get(x, 0), reverse=True)
                        
                        award_name = ""
                        if award_nominee_ids:
                            first_meta = self.nominee_metadata.get(award_nominee_ids[0], {})
                            award_name = first_meta.get("award_name", "")
                        
                        if now - user_state['last_summary_time'] >= summary_interval:
                            if len(curr_ranking) >= 2:
                                top1_id = curr_ranking[0]
                                top2_id = curr_ranking[1]
                                
                                top1_votes = current_votes.get(top1_id, 0)
                                top2_votes = current_votes.get(top2_id, 0)
                                gap = top1_votes - top2_votes
                                
                                top1_meta = self.nominee_metadata.get(top1_id, {})
                                top1_name = top1_meta.get("name", top1_id)
                                top1_image = top1_meta.get("image", "")
                                
                                award_text = f" - {award_name}" if award_name else ""
                                self.send_push_notification({
                                    "title": f"Cập nhật đường đua WeYoung 2025{award_text}",
                                    "body": f"🏆 Dẫn đầu: {top1_name}\n📈 Tổng vote: {top1_votes:,}\n⚡ Tạo khoảng cách: {gap:,} vote",
                                    "icon": top1_image,
                                    "image": top1_image
                                }, target_endpoints=[endpoint])
                                print(f"Push Sent: Race Summary for {award_name} to {endpoint[:20]}...")
                                user_state['last_summary_time'] = now

                        for rank_index, nid in enumerate(curr_ranking):
                            prev_rank_index = -1
                            if nid in prev_ranking:
                                prev_rank_index = prev_ranking.index(nid)
                            
                            if prev_rank_index != -1 and rank_index != prev_rank_index:
                                if rank_index < prev_rank_index:
                                    meta = self.nominee_metadata.get(nid, {})
                                    name = meta.get("name", nid)
                                    image = meta.get("image", "")
                                    
                                    award_text = f" - {award_name}" if award_name else ""
                                    title = f"Biến động BXH WeYoung 2025{award_text}"
                                    body = f"🔥 {name} vừa vươn lên vị trí #{rank_index + 1} tại {award_name}. Vote ngay!"
                                    
                                    if rank_index == 0:
                                        title = f"👑 Ngôi vương WeYoung 2025 đã đổi chủ{award_text}"
                                        body = f"🏆 {name} đã xuất sắc vươn lên Top 1 tại {award_name}. Cuộc đua đang cực kỳ gay cấn!"
                                    
                                    self.send_push_notification({
                                        "title": title,
                                        "body": body,
                                        "icon": image,
                                        "image": image
                                    }, target_endpoints=[endpoint])
                                    print(f"Push Sent: {name} rank up to #{rank_index + 1} in {award_name} to {endpoint[:20]}...")

                        for nid in award_nominee_ids:
                            curr_v = current_votes.get(nid, 0)
                            prev_v = prev_votes.get(nid, 0)
                            
                            milestone = self.check_milestone_crossed(prev_v, curr_v)
                            if milestone is not None:
                                meta = self.nominee_metadata.get(nid, {})
                                name = meta.get("name", nid)
                                image = meta.get("image", "")
                                
                                milestone_formatted = self.format_milestone(milestone)
                                
                                award_text = f" tại {award_name}" if award_name else ""
                                self.send_push_notification({
                                    "title": f"🎉 Chúc mừng {name}{award_text}",
                                    "body": f"🌟 Đã cán mốc {milestone_formatted} vote tại {award_name}. Tiếp tục ủng hộ nào!",
                                    "icon": image,
                                    "image": image
                                }, target_endpoints=[endpoint])
                                print(f"Push Sent: {name} milestone {milestone_formatted} in {award_name} to {endpoint[:20]}...")
                    
                    user_state['prev_votes'] = {nid: current_votes.get(nid, 0) for nid in nominee_ids}
                
                active_endpoints = set(all_preferences.keys())
                user_states = {ep: state for ep, state in user_states.items() if ep in active_endpoints}

            except Exception as e:
                print(f"Notification Service Error: {e}")
            
            time.sleep(10)