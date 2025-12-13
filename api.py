from flask import Flask, jsonify, request
import wca_vote_crawler
import notification_service
from flask_cors import CORS
import threading

class WCAVotesAPI:
    def __init__(self):
        self.app = Flask(__name__)
        
        self._register_routes()
        CORS(self.app, resources={r"/*": {"origins": ["https://lzhoang2801.github.io", "http://127.0.0.1:3000"]}})

        self.wca_vote_crawler = wca_vote_crawler.WCAVoteCrawler()
        self.notification_service = notification_service.NotificationService(self.wca_vote_crawler)
        
        crawler_thread = threading.Thread(target=self.wca_vote_crawler.run, daemon=True)
        crawler_thread.start()

        notification_thread = threading.Thread(target=self.notification_service.run, daemon=True)
        notification_thread.start()

    def _register_routes(self):
        self.app.route('/categories', methods=['GET'])(self.api_get_categories)
        self.app.route('/get_votes', methods=['GET'])(self.api_get_votes)
        self.app.route('/subscribe', methods=['POST'])(self.api_subscribe)
        self.app.route('/unsubscribe', methods=['POST'])(self.api_unsubscribe)
        self.app.route('/vapid_public_key', methods=['GET'])(self.api_get_vapid_key)
        self.app.route('/preferences', methods=['GET', 'POST'])(self.api_preferences)

    def api_get_votes(self):
        try:
            last_updated = request.args.get('last_updated')
            award_id = request.args.get('award_id')
            limit = int(request.args.get('limit', '5'))
            
            return jsonify(self.wca_vote_crawler.get_vote_history(
                award_id=award_id,
                limit=limit,
                start_at=last_updated
            )), 200
        except Exception as e:
            return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500

    def api_get_categories(self):
        try:
            return jsonify(self.wca_vote_crawler.wca_nominees), 200
        except Exception:
            return jsonify({"error": "Failed to load categories"}), 500

    def api_subscribe(self):
        success, msg = self.notification_service.add_subscriber(request.json)
        if success:
            return jsonify({"success": True}), 201
        return jsonify({"error": msg}), 400

    def api_unsubscribe(self):
        try:
            data = request.json
            if data and 'endpoint' in data:
                self.notification_service.remove_subscription(data['endpoint'])
                return jsonify({"success": True}), 200
            return jsonify({"error": "Missing endpoint"}), 400
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def api_get_vapid_key(self):
        return jsonify({"publicKey": self.notification_service.get_vapid_public_key()})

    def api_preferences(self):
        if request.method == 'GET':
            try:
                endpoint = request.args.get('endpoint')
                if not endpoint:
                    return jsonify({"error": "Missing endpoint parameter"}), 400
                
                prefs = self.notification_service.get_user_preferences(endpoint)
                return jsonify(prefs), 200
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        else:
            try:
                data = request.json
                if not data:
                    return jsonify({"error": "Missing request body"}), 400
                
                endpoint = data.get('endpoint')
                nominee_filter = data.get('nominee_filter', [])
                summary_interval = data.get('summary_interval', 900)
                
                if not endpoint:
                    return jsonify({"error": "Missing endpoint"}), 400
                
                if not isinstance(nominee_filter, list):
                    return jsonify({"error": "nominee_filter must be an array"}), 400
                
                for item in nominee_filter:
                    if not isinstance(item, list) or len(item) != 2:
                        return jsonify({"error": "Each item in nominee_filter must be [award_id, nominee_id]"}), 400
                
                if not isinstance(summary_interval, int) or summary_interval < 15 * 60:
                    return jsonify({"error": "Summary interval must be an integer >= 15 minutes"}), 400
                
                success, msg = self.notification_service.set_user_preferences(
                    endpoint, nominee_filter, summary_interval
                )
                
                if success:
                    return jsonify({"success": True, "message": msg}), 200
                return jsonify({"error": msg}), 400
            except Exception as e:
                return jsonify({"error": str(e)}), 500

    def run(self, debug=True):
        self.app.run(port=5000)

if __name__ == '__main__':
    wca_votes = WCAVotesAPI()
    wca_votes.run()