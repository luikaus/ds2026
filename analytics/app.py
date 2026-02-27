"""
Logging and analytics middleware to track popular content, QoS metrics, and user sessions.
"""
import os
from datetime import datetime, UTC
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from shared.models import Base, VideoEvent, VideoStats

# Config
postgres_endpoint = os.getenv('POSTGRES_URL')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = postgres_endpoint
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app, model_class=Base)

#with app.app_context():
    #db.create_all()


#  Routes

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "service": "analytics-server"}), 200


@app.route('/event', methods=['POST'])
def receive_event():

    data = request.get_json(silent=True)
    if not data or 'video_id' not in data or 'event_type' not in data:
        return jsonify({"error": "Missing required fields: video_id, event_type"}), 400

    event = VideoEvent(
        video_id=data['video_id'],
        event_type=data['event_type'],
        edge_id=data.get('edge_id'),
        user_ip=data.get('user_ip'),
        file_type=data.get('file_type'),
        duration_ms=data.get('duration_ms'),
        timestamp=datetime.now(UTC)
    )
    db.session.add(event)

    # Update running stats table (denormalized for fast reads)
    _update_stats(data['video_id'], data['event_type'])

    db.session.commit()
    return jsonify({"status": "recorded"}), 201


def _update_stats(video_id: str, event_type: str):
    """Maintain a running aggregated stats row per video."""
    stats = db.session.query(VideoStats).filter_by(video_id=video_id).first()
    if not stats:
        stats = VideoStats(video_id=video_id)
        db.session.add(stats)

    if event_type == 'cache_hit':
        stats.cache_hits += 1
        stats.total_requests += 1
    elif event_type == 'cache_miss':
        stats.cache_misses += 1
        stats.total_requests += 1

    stats.last_accessed = datetime.now(UTC)


@app.route('/metrics', methods=['GET'])
def get_metrics():

    stats = db.session.query(VideoStats).all()
    total_hits = sum(s.cache_hits for s in stats)
    total_misses = sum(s.cache_misses for s in stats)
    total = total_hits + total_misses

    return jsonify({
        "total_requests": total,
        "cache_hits": total_hits,
        "cache_misses": total_misses,
        "cache_hit_rate": round(total_hits / total, 4) if total > 0 else 0,
        "unique_videos_accessed": len(stats)
    }), 200


@app.route('/popular', methods=['GET'])
def get_popular():

    limit = request.args.get('limit', 10, type=int)
    stats = (db.session.query(VideoStats)
             .order_by(VideoStats.total_requests.desc())
             .limit(limit)
             .all())

    return jsonify([{
        "video_id": s.video_id,
        "total_requests": s.total_requests,
        "cache_hits": s.cache_hits,
        "cache_misses": s.cache_misses,
        "cache_hit_rate": round(s.cache_hits / s.total_requests, 4) if s.total_requests > 0 else 0,
        "last_accessed": s.last_accessed.isoformat() if s.last_accessed else None
    } for s in stats]), 200


@app.route('/event/nginx', methods=['GET', 'POST'])
def receive_nginx_mirror():

    uri = request.headers.get('X-Original-URI', '')
    cache_status = request.headers.get('X-Cache-Status', '').upper()
    edge_id = request.headers.get('X-Edge-ID', 'unknown')
    client_ip = request.headers.get('X-Client-IP')

    print(f"Received: \nURI: {uri} \nCache Status: {cache_status} \nEdge ID: {edge_id} \nClient IP: {client_ip}")
    # Extract video_id and file type from URI
    # URI format: /video/<video_id>/seg_001.ts  OR  /video/<video_id>/master.m3u8
    parts = [p for p in uri.split('/') if p]
    if len(parts) < 2 or parts[0] not in ('video', 'thumbnail'):
        return '', 204   # Ignore non-video requests silently

    video_id = parts[1]
    file_type = parts[2].split('.')[-1] if len(parts) > 2 else 'unknown'

    # Map NGINX cache status to event type
    event_type_map = {
        'HIT':     'cache_hit',
        'MISS':    'cache_miss',
        'EXPIRED': 'cache_miss',
        'BYPASS':  'cache_miss',
        'STALE':   'cache_hit',
    }
    event_type = event_type_map.get(cache_status, 'cache_miss')

    event = VideoEvent(
        video_id=video_id,
        event_type=event_type,
        edge_id=edge_id,
        user_ip=client_ip,
        file_type=file_type,
        timestamp=__import__('datetime').datetime.now(__import__('datetime').UTC)
    )
    db.session.add(event)
    _update_stats(video_id, event_type)
    db.session.commit()

    return '', 204   # No content — NGINX ignores this response


@app.route('/events', methods=['GET'])
def get_events():

    video_id = request.args.get('video_id')
    event_type = request.args.get('event_type')
    limit = request.args.get('limit', 100, type=int)

    query = db.session.query(VideoEvent)
    if video_id:
        query = query.filter_by(video_id=video_id)
    if event_type:
        query = query.filter_by(event_type=event_type)

    events = query.order_by(VideoEvent.timestamp.desc()).limit(limit).all()

    return jsonify([{
        "id": e.id,
        "video_id": e.video_id,
        "event_type": e.event_type,
        "edge_id": e.edge_id,
        "user_ip": e.user_ip,
        "file_type": e.file_type,
        "duration_ms": e.duration_ms,
        "timestamp": e.timestamp.isoformat()
    } for e in events]), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)
