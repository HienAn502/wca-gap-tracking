from flask import Flask, jsonify, request
import wca_vote_crawler
from flask_cors import CORS
import threading

class WCAVotesAPI:
    def __init__(self):
        self.app = Flask(__name__)
        
        self._register_routes()
        CORS(self.app, resources={r"/*": {"origins": ["https://lzhoang2801.github.io", "http://127.0.0.1:3000"]}})

        self.wca_vote_crawler = wca_vote_crawler.WCAVoteCrawler()
        
        crawler_thread = threading.Thread(target=self.wca_vote_crawler.run, daemon=True)
        crawler_thread.start()

    def _register_routes(self):
        self.app.route('/categories', methods=['GET'])(self.api_get_categories)
        self.app.route('/get_votes', methods=['GET'])(self.api_get_votes)

    def api_get_votes(self):
        try:
            last_updated = request.args.get('last_updated')
            award_id = request.args.get('award_id')
            data_member = request.args.get('data_member')
            return jsonify(self.wca_vote_crawler.get_vote_history(
                award_id=award_id,
                data_member=data_member,
                start_at=last_updated
            )), 200
        except Exception as e:
            return jsonify({"error": "An unexpected error occurred"}), 500

    def api_get_categories(self):
        try:
            return jsonify(self.wca_vote_crawler.wca_nominees), 200
        except Exception:
            return jsonify({"error": "Failed to load categories"}), 500

    def run(self, debug=True):
        self.app.run(port=3000)

if __name__ == '__main__':
    wca_votes = WCAVotesAPI()
    wca_votes.run()