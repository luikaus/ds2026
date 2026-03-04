"""
Schedule Routes — Increment 6 (Resource Scheduling)
Exposes the cache-warming scheduler via REST endpoints.

To use: in app.py, add at the bottom:
    from schedule_routes import register_schedule_routes
    register_schedule_routes(app, db)
"""
from flask import jsonify, request
from scheduler import get_hot_videos, run_scheduler


def register_schedule_routes(app, db):

    @app.route('/schedule/run', methods=['POST'])
    def trigger_scheduler():
        """
        Triggers a cache pre-warming run for hot videos.
        POST /schedule/run?top_n=5&threshold=1.0
        Reads ML predictions, identifies high-demand videos, and pre-fetches
        their HLS segments to both edge nodes so they are served from cache.
        """
        top_n = request.args.get('top_n', 5, type=int)
        threshold = request.args.get('threshold', 1.0, type=float)
        result = run_scheduler(top_n=top_n, threshold=threshold)
        return jsonify(result), 200

    @app.route('/schedule/status', methods=['GET'])
    def scheduler_status():
        """
        Returns current list of "hot" videos and their predicted demand.
        GET /schedule/status?top_n=10&threshold=0.0
        Use threshold=0.0 to see all videos with predictions.
        """
        top_n = request.args.get('top_n', 10, type=int)
        threshold = request.args.get('threshold', 0.0, type=float)
        hot = get_hot_videos(db.session, top_n=top_n, threshold=threshold)
        return jsonify([{
            "video_id": p.video_id,
            "predicted_requests_next_1h": round(p.predicted_requests, 2),
            "confidence": round(p.confidence, 3) if p.confidence else None,
            "predicted_at": p.predicted_at.isoformat(),
        } for p in hot]), 200
