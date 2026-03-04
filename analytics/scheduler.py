"""
Resource Scheduler — Increment 6
Reads ML predictions and popularity stats, then pre-warms edge node caches
for high-demand ("hot") videos so popular content is always served from cache.
"""
import os
import requests
from datetime import datetime, UTC
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from shared.models import MLPrediction, VideoStats

postgres_endpoint = os.getenv('POSTGRES_URL')
engine = create_engine(postgres_endpoint)
Session = sessionmaker(bind=engine)

EDGE_NODES = [
    "http://edge-node-1:80",
    "http://edge-node-2:80",
]

HOT_THRESHOLD = 1.0  # predicted requests/hour to qualify as "hot"
TOP_N = 5            # max videos to pre-warm per run


def get_hot_videos(session, top_n=TOP_N, threshold=HOT_THRESHOLD):
    """
    Returns the top-N videos whose latest ML prediction >= threshold,
    ordered by predicted demand (highest first).
    """
    subq = (session.query(
                MLPrediction.video_id,
                func.max(MLPrediction.predicted_at).label('latest')
            ).group_by(MLPrediction.video_id).subquery())

    return (session.query(MLPrediction)
            .join(subq, (MLPrediction.video_id == subq.c.video_id) &
                        (MLPrediction.predicted_at == subq.c.latest))
            .filter(MLPrediction.predicted_requests >= threshold)
            .order_by(MLPrediction.predicted_requests.desc())
            .limit(top_n)
            .all())


def warm_edge_cache(video_id: str, edge_url: str) -> list:
    """
    Pre-fetches a video's HLS content from an edge node to warm its cache.
    Fetches: master.m3u8, thumbnail, each variant's index.m3u8 and seg_000.ts.
    Returns a list of {url, status, cache_status} dicts.
    """
    results = []

    def fetch(url):
        try:
            r = requests.get(url, timeout=10, stream=True)
            r.close()
            return {
                "url": url,
                "status": r.status_code,
                "cache_status": r.headers.get("X-Cache-Status", "unknown"),
            }
        except Exception as e:
            return {"url": url, "status": None, "error": str(e)}

    # Always warm master playlist and thumbnail
    master_url = f"{edge_url}/video/{video_id}/master.m3u8"
    results.append(fetch(f"{edge_url}/thumbnail/{video_id}"))
    master_result = fetch(master_url)
    results.append(master_result)

    # Parse master.m3u8 to discover which resolution variants exist
    variants = []
    try:
        r = requests.get(master_url, timeout=5)
        if r.status_code == 200:
            for line in r.text.splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    # Lines look like: "360p/index.m3u8"
                    resolution = line.split('/')[0]
                    variants.append(resolution)
    except Exception:
        pass

    # Warm index.m3u8 and first segment for each variant
    for resolution in variants:
        results.append(fetch(f"{edge_url}/video/{video_id}/{resolution}/index.m3u8"))
        results.append(fetch(f"{edge_url}/video/{video_id}/{resolution}/seg_000.ts"))

    return results


def run_scheduler(top_n=TOP_N, threshold=HOT_THRESHOLD) -> dict:
    """
    Main entry point: identifies hot videos and pre-warms both edge node caches.
    Returns a summary with per-video, per-edge warming results.
    """
    session = Session()
    try:
        hot_videos = get_hot_videos(session, top_n=top_n, threshold=threshold)

        if not hot_videos:
            return {
                "scheduled": 0,
                "threshold": threshold,
                "message": "No videos above demand threshold — nothing to pre-warm.",
            }

        results = []
        for pred in hot_videos:
            video_result = {
                "video_id": pred.video_id,
                "predicted_requests_next_1h": round(pred.predicted_requests, 2),
                "confidence": round(pred.confidence, 3) if pred.confidence else None,
                "edges": {},
            }
            for edge_url in EDGE_NODES:
                edge_name = edge_url.split("//")[1].split(":")[0]
                warmed = warm_edge_cache(pred.video_id, edge_url)
                hits = sum(1 for w in warmed if w.get("cache_status") == "HIT")
                misses = sum(1 for w in warmed if w.get("cache_status") in ("MISS", "EXPIRED"))
                video_result["edges"][edge_name] = {
                    "warmed_urls": len(warmed),
                    "cache_hits": hits,
                    "cache_misses": misses,
                    "detail": warmed,
                }
            results.append(video_result)

        print(f"[SCHEDULER] Pre-warmed {len(results)} hot videos across {len(EDGE_NODES)} edge nodes "
              f"(threshold={threshold} req/h)")
        return {
            "scheduled": len(results),
            "threshold": threshold,
            "edge_nodes": [e.split("//")[1].split(":")[0] for e in EDGE_NODES],
            "results": results,
        }
    finally:
        session.close()
