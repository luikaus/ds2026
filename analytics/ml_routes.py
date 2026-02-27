"""
ML Routes extension for app.py
Add these routes to app.py to expose predictions via REST.

To use: in app.py, add at the bottom:
    from ml_routes import register_ml_routes
    register_ml_routes(app, db)
"""
from flask import jsonify, request
from ml import run_predictions, get_latest_predictions


def register_ml_routes(app, db):

    @app.route('/predictions/run', methods=['POST'])
    def trigger_predictions():
        """
        Triggers a prediction run manually.
        POST /predictions/run
        In production this would be called by a cron job or Celery beat.
        """
        result = run_predictions()
        return jsonify(result), 200

    @app.route('/predictions', methods=['GET'])
    def get_predictions():
        """
        Returns the latest demand spike predictions per video.
        GET /predictions?limit=10
        """
        limit = request.args.get('limit', 10, type=int)
        preds = get_latest_predictions(db.session, limit=limit)
        return jsonify([{
            "video_id": p.video_id,
            "predicted_requests_next_1h": round(p.predicted_requests, 2),
            "confidence": round(p.confidence, 3) if p.confidence else None,
            "predicted_at": p.predicted_at.isoformat()
        } for p in preds]), 200
