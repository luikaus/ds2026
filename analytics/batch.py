"""
Batch Processor - Increment 10
Processes event logs in a batch job to extract content popularity patterns.
Run this as a scheduled job (cron or Celery beat) every N minutes.

Usage:
    python batch.py                    # process all unprocessed events
    python batch.py --window-hours 24  # analyze last 24 hours
"""
import os
import argparse
from datetime import datetime, timedelta, UTC
from collections import defaultdict

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from models import VideoEvent, VideoStats, MLPrediction

postgres_endpoint = os.getenv('POSTGRES_URL')
engine = create_engine(postgres_endpoint)
Session = sessionmaker(bind=engine)


def run_batch(window_hours: int = 1):
    """
    Main batch job. Reads events from the last window_hours,
    computes popularity patterns, and stores them in analytics_stats.
    """
    session = Session()
    try:
        since = datetime.now(UTC) - timedelta(hours=window_hours)
        print(f"[BATCH] Running batch for window: last {window_hours}h (since {since.isoformat()})")

        # ── 1. Aggregate request counts per video ──────────────────────────
        rows = (session.query(
                    VideoEvent.video_id,
                    func.count(VideoEvent.id).label('total'),
                    func.sum(
                        (VideoEvent.event_type == 'cache_hit').cast('int')
                    ).label('hits'),
                    func.sum(
                        (VideoEvent.event_type == 'cache_miss').cast('int')
                    ).label('misses')
                )
                .filter(VideoEvent.timestamp >= since)
                .group_by(VideoEvent.video_id)
                .all())

        print(f"[BATCH] Found {len(rows)} unique videos with activity in window")

        # ── 2. Upsert into VideoStats ──────────────────────────────────────
        for row in rows:
            stats = session.query(VideoStats).filter_by(video_id=row.video_id).first()
            if not stats:
                stats = VideoStats(video_id=row.video_id, total_requests=0,
                                   cache_hits=0, cache_misses=0)
                session.add(stats)

            # Accumulate (don't replace — stats are running totals)
            stats.total_requests += row.total
            stats.cache_hits += (row.hits or 0)
            stats.cache_misses += (row.misses or 0)
            stats.last_accessed = datetime.now(UTC)

        session.commit()
        print("[BATCH] Stats updated successfully")

        # ── 3. Print popularity ranking ────────────────────────────────────
        top = (session.query(VideoStats)
               .order_by(VideoStats.total_requests.desc())
               .limit(5)
               .all())

        print("\n[BATCH] Top 5 most requested videos:")
        print(f"  {'video_id':<20} {'requests':>10} {'hit_rate':>10}")
        print(f"  {'-'*20} {'-'*10} {'-'*10}")
        for s in top:
            hit_rate = s.cache_hits / s.total_requests if s.total_requests > 0 else 0
            print(f"  {s.video_id[:20]:<20} {s.total_requests:>10} {hit_rate:>9.1%}")

        return {"processed_videos": len(rows), "window_hours": window_hours}

    except Exception as e:
        session.rollback()
        print(f"[BATCH ERROR] {e}")
        raise
    finally:
        session.close()


def compute_popularity_patterns():
    """
    Extended analysis: computes hourly request patterns per video.
    Returns a dict usable as features for the ML model in batch_ml.py
    """
    session = Session()
    try:
        # Get hourly buckets for the last 24h
        since = datetime.now(UTC) - timedelta(hours=24)
        events = (session.query(VideoEvent)
                  .filter(VideoEvent.timestamp >= since)
                  .all())

        patterns = defaultdict(lambda: defaultdict(int))
        for e in events:
            hour_bucket = e.timestamp.replace(minute=0, second=0, microsecond=0)
            patterns[e.video_id][hour_bucket.isoformat()] += 1

        return dict(patterns)
    finally:
        session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analytics batch processor')
    parser.add_argument('--window-hours', type=int, default=1,
                        help='Time window to process (default: 1h)')
    args = parser.parse_args()
    run_batch(window_hours=args.window_hours)
