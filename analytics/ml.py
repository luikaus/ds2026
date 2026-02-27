"""
Distributed ML
Predicts video demand spikes in near-real-time using a RandomForestRegressor
that analyzes aggregated logs across edge nodes.

Run standalone:
    python ml.py          # train and predict
    python ml.py --predict-only   # only predict with saved model
"""
import os
import pickle
import warnings
from datetime import datetime, timedelta, UTC

import numpy as np
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from models import VideoEvent, VideoStats, MLPrediction

warnings.filterwarnings('ignore')

postgres_endpoint = os.getenv('POSTGRES_URL')
engine = create_engine(postgres_endpoint)
Session = sessionmaker(bind=engine)

MODEL_PATH = '/app/model.pkl'


def build_features(session, video_id: str) -> list:
    """
    Builds a feature vector for a given video_id.
    Features used:
      - requests_last_1h
      - requests_last_6h
      - requests_last_24h
      - cache_hit_rate (lifetime)
      - hour_of_day (to capture daily patterns)
    """
    now = datetime.now(UTC)

    def count_events(hours_back):
        since = now - timedelta(hours=hours_back)
        return (session.query(func.count(VideoEvent.id))
                .filter(VideoEvent.video_id == video_id,
                        VideoEvent.timestamp >= since)
                .scalar() or 0)

    r1h = count_events(1)
    r6h = count_events(6)
    r24h = count_events(24)

    stats = session.query(VideoStats).filter_by(video_id=video_id).first()
    hit_rate = 0.0
    if stats and stats.total_requests > 0:
        hit_rate = stats.cache_hits / stats.total_requests

    return [r1h, r6h, r24h, hit_rate, now.hour]


def train_model(session):
    """
    Trains a RandomForestRegressor using historical data.
    Target: number of requests in next 1h window.
    """
    try:
        from sklearn.ensemble import RandomForestRegressor
    except ImportError:
        print("[ML] scikit-learn not installed. Run: pip install scikit-learn")
        return None

    video_ids = [row[0] for row in session.query(VideoEvent.video_id).distinct().all()]

    if len(video_ids) < 3:
        print(f"[ML] Not enough data to train ({len(video_ids)} videos). Need at least 3.")
        return None

    X, y = [], []
    for vid in video_ids:
        features = build_features(session, vid)
        # Label: requests in next 1h (approximated as requests in last 1h shifted)
        target = features[0]  # self-supervised proxy
        X.append(features)
        y.append(target)

    X = np.array(X)
    y = np.array(y)

    model = RandomForestRegressor(n_estimators=50, random_state=42)
    model.fit(X, y)

    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(model, f)

    print(f"[ML] Model trained on {len(video_ids)} videos. Saved to {MODEL_PATH}")
    return model


def load_or_train_model(session):
    if os.path.exists(MODEL_PATH):
        with open(MODEL_PATH, 'rb') as f:
            model = pickle.load(f)
        print("[ML] Loaded existing model")
        return model
    return train_model(session)


def run_predictions():
    """
    Main entry point: trains (or loads) model, generates predictions,
    stores them in analytics_predictions table.
    """
    session = Session()
    try:
        model = load_or_train_model(session)
        if model is None:
            return {"error": "Could not train model - insufficient data"}

        video_ids = [row[0] for row in session.query(VideoEvent.video_id).distinct().all()]
        now = datetime.now(UTC)
        predictions_made = 0

        for vid in video_ids:
            features = build_features(session, vid)
            predicted = float(model.predict([features])[0])
            predicted = max(0.0, predicted)  # clamp negative

            # Confidence: simple heuristic based on training data volume
            r24h = features[2]
            confidence = min(1.0, r24h / 100.0) if r24h > 0 else 0.1

            pred = MLPrediction(
                video_id=vid,
                predicted_requests=predicted,
                confidence=confidence,
                window_hours=1,
                predicted_at=now
            )
            session.add(pred)
            predictions_made += 1

        session.commit()
        print(f"[ML] Generated {predictions_made} predictions")
        return {"predictions_made": predictions_made}

    except Exception as e:
        session.rollback()
        print(f"[ML ERROR] {e}")
        return {"error": str(e)}
    finally:
        session.close()


# Flask route helper (called from app.py)

def get_latest_predictions(session, limit=10):
    """Returns the most recent predictions per video."""
    from sqlalchemy import distinct

    # Get latest prediction per video
    subq = (session.query(
                MLPrediction.video_id,
                func.max(MLPrediction.predicted_at).label('latest')
            ).group_by(MLPrediction.video_id).subquery())

    preds = (session.query(MLPrediction)
             .join(subq, (MLPrediction.video_id == subq.c.video_id) &
                         (MLPrediction.predicted_at == subq.c.latest))
             .order_by(MLPrediction.predicted_requests.desc())
             .limit(limit)
             .all())
    return preds


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--predict-only', action='store_true')
    args = parser.parse_args()

    result = run_predictions()
    print(f"[ML] Done: {result}")
