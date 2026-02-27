"""
Analytics database models.
These tables live in the same PostgreSQL instance as the core-server
but use a distinct prefix 'analytics_' to avoid collisions.
"""
from sqlalchemy import Column, String, Integer, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, UTC

# Uses the same Base from shared/ so create_all() works with db.create_all()
from shared import Base


class VideoEvent(Base):
    """
    Raw event log — one row per request arriving at an edge node.
    This is the append-only source of truth for Increments 8 and 10.
    """
    __tablename__ = 'analytics_events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    video_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(32), nullable=False)   # cache_hit, cache_miss, upload, transcode_done
    edge_id = Column(String(64), nullable=True)        # edge-node-1 / edge-node-2
    user_ip = Column(String(45), nullable=True)        # IPv4 or IPv6
    file_type = Column(String(10), nullable=True)      # ts, m3u8, jpg
    duration_ms = Column(Integer, nullable=True)       # Request duration in ms
    timestamp = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))


class VideoStats(Base):
    """
    Denormalized running aggregates — updated on every event.
    Fast reads for /metrics and /popular endpoints.
    """
    __tablename__ = 'analytics_stats'

    video_id = Column(String(64), primary_key=True)
    total_requests = Column(Integer, nullable=False, default=0)
    cache_hits = Column(Integer, nullable=False, default=0)
    cache_misses = Column(Integer, nullable=False, default=0)
    last_accessed = Column(DateTime, nullable=True)


class MLPrediction(Base):
    """
    Stores predictions made by the distributed ML model .
    Predictions are written by the batch job and read by the scheduler .
    """
    __tablename__ = 'analytics_predictions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    video_id = Column(String(64), nullable=False, index=True)
    predicted_requests = Column(Float, nullable=False)   # Expected requests in next window
    confidence = Column(Float, nullable=True)             # Model confidence 0-1
    window_hours = Column(Integer, nullable=False, default=1)
    predicted_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
