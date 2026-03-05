
from flask import jsonify, request
from scheduler import get_hot_videos, run_scheduler


def register_schedule_routes(app, db):

    @app.route('/schedule/run', methods=['POST'])
    def trigger_scheduler():

        top_n = request.args.get('top_n', 5, type=int)
        threshold = request.args.get('threshold', 1.0, type=float)
        result = run_scheduler(top_n=top_n, threshold=threshold)
        return jsonify(result), 200

    @app.route('/schedule/status', methods=['GET'])
    def scheduler_status():

        top_n = request.args.get('top_n', 10, type=int)
        threshold = request.args.get('threshold', 0.0, type=float)
        hot = get_hot_videos(db.session, top_n=top_n, threshold=threshold)
        return jsonify([{
            "video_id": p.video_id,
            "predicted_requests_next_1h": round(p.predicted_requests, 2),
            "confidence": round(p.confidence, 3) if p.confidence else None,
            "predicted_at": p.predicted_at.isoformat(),
        } for p in hot]), 200
