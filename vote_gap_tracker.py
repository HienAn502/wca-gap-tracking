import requests
import time
from datetime import datetime
from typing import Optional, Dict, List


class VoteGapTracker:
    def __init__(self, api_url: str = "http://127.0.0.1:5000"):
        self.api_url = api_url
        self.categories = {}

    def load_categories(self) -> bool:
        """Load available categories from API"""
        try:
            response = requests.get(f"{self.api_url}/categories")
            if response.status_code == 200:
                self.categories = response.json()
                return True
            print(f"Failed to load categories: {response.status_code}")
            return False
        except Exception as e:
            print(f"Error loading categories: {e}")
            return False

    def display_categories(self):
        """Display all available categories"""
        if not self.categories:
            print("No categories loaded. Call load_categories() first.")
            return

        print("\n=== Available Categories ===")
        for category_id, data in self.categories.items():
            if category_id == 'idol14':
                subcategories = data['subcategories']
                for award_id, award_data in subcategories.items():
                    print(f"\nAward ID: {award_id}")
                    print(f"Name: {award_data['award_name']}")
                    print("Nominees:")
                    for nominee in award_data['nominees']:
                        print(f"{nominee['nominee_name']} - {nominee['nominee_des']}")
            else:
                print(f"\nAward ID: {category_id}")
                print(f"Name: {data['award_name']}")
                print("Nominees:")
                for nominee in data['nominees']:
                    print(f"{nominee['nominee_name']} - {nominee['nominee_des']}")


    def get_current_votes(self, award_id: str) -> Optional[Dict]:
        """Get current vote counts for an award"""
        try:
            response = requests.get(
                f"{self.api_url}/get_votes",
                params={"award_id": award_id, "limit": 1}
            )
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    return data[0]
            return None
        except Exception as e:
            print(f"Error getting votes: {e}")
            return None

    def find_nominee_position(self, votes: Dict, nominee_id: str) -> Optional[int]:
        """Find the position of a nominee in the sorted vote list"""
        sorted_nominees = sorted(
            votes['votes'].items(),
            key=lambda x: x[1],
            reverse=True
        )
        for idx, (nid, _) in enumerate(sorted_nominees):
            if nid == nominee_id:
                return idx
        return None

    def get_gaps(self, award_id: str, nominee_id: str) -> Optional[Dict]:
        """Get vote gaps for a specific nominee"""
        votes_data = self.get_current_votes(award_id)
        if not votes_data:
            return None

        votes = votes_data['votes']
        if nominee_id not in votes:
            print(f"Nominee {nominee_id} not found in votes")
            return None

        # Sort nominees by votes (descending)
        sorted_nominees = sorted(
            votes.items(),
            key=lambda x: x[1],
            reverse=True
        )

        # Find position
        position = None
        for idx, (nid, _) in enumerate(sorted_nominees):
            if nid == nominee_id:
                position = idx
                break

        if position is None:
            return None

        current_votes = votes[nominee_id]

        # Get nominee names
        nominee_names = {n['id']: n['name'] for n in self.categories[award_id]['nominees']}

        result = {
            'timestamp': votes_data['timestamp'],
            'position': position + 1,
            'total_nominees': len(sorted_nominees),
            'current_votes': current_votes,
            'nominee_name': nominee_names.get(nominee_id, nominee_id),
            'gap_above': None,
            'gap_below': None
        }

        # Gap to nominee above (if exists)
        if position > 0:
            above_id, above_votes = sorted_nominees[position - 1]
            result['gap_above'] = {
                'nominee_id': above_id,
                'nominee_name': nominee_names.get(above_id, above_id),
                'votes': above_votes,
                'gap': above_votes - current_votes
            }

        # Gap to nominee below (if exists)
        if position < len(sorted_nominees) - 1:
            below_id, below_votes = sorted_nominees[position + 1]
            result['gap_below'] = {
                'nominee_id': below_id,
                'nominee_name': nominee_names.get(below_id, below_id),
                'votes': below_votes,
                'gap': current_votes - below_votes
            }

        return result

    def display_gaps(self, gap_data: Dict):
        """Display gap information in a formatted way"""
        print(f"\n{'=' * 60}")
        print(f"Timestamp: {gap_data['timestamp']}")
        print(f"{'=' * 60}")
        print(f"Tracking: {gap_data['nominee_name']}")
        print(f"Position: #{gap_data['position']} of {gap_data['total_nominees']}")
        print(f"Current Votes: {gap_data['current_votes']:,}")
        print(f"{'-' * 60}")

        if gap_data['gap_above']:
            above = gap_data['gap_above']
            print(f"\n↑ ABOVE (#{gap_data['position'] - 1})")
            print(f"  {above['nominee_name']}")
            print(f"  Votes: {above['votes']:,}")
            print(f"  Gap: -{above['gap']:,} votes (need to gain)")
        else:
            print(f"\n🏆 LEADING - No one above!")

        if gap_data['gap_below']:
            below = gap_data['gap_below']
            print(f"\n↓ BELOW (#{gap_data['position'] + 1})")
            print(f"  {below['nominee_name']}")
            print(f"  Votes: {below['votes']:,}")
            print(f"  Gap: +{below['gap']:,} votes (lead)")
        else:
            print(f"\n📍 LAST PLACE - No one below")

        print(f"{'=' * 60}\n")

    def track_continuously(self, award_id: str, nominee_id: str, interval: int = 60):
        """Continuously track gaps at specified interval (seconds)"""
        print(f"\nStarting continuous tracking...")
        print(f"Award ID: {award_id}")
        print(f"Nominee ID: {nominee_id}")
        print(f"Update interval: {interval} seconds")
        print(f"Press Ctrl+C to stop\n")

        try:
            while True:
                gap_data = self.get_gaps(award_id, nominee_id)
                if gap_data:
                    self.display_gaps(gap_data)
                else:
                    print("Failed to get gap data")

                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\nTracking stopped by user")


# Example usage
if __name__ == "__main__":
    tracker = VoteGapTracker()

    # Load categories
    if not tracker.load_categories():
        print("Failed to load categories. Make sure the API is running.")
        exit(1)

    # Display available categories
    tracker.display_categories()

    # Example: Track a specific nominee
    # Replace with actual award_id and nominee_id
    print("\n" + "=" * 60)
    print("Enter tracking details:")
    award_id = input("Award ID: ").strip()
    nominee_id = input("Nominee ID: ").strip()
    interval = input("Update interval in seconds (default 60): ").strip()
    interval = int(interval) if interval else 60

    # Start tracking
    tracker.track_continuously(award_id, nominee_id, interval)