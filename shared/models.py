from sqlalchemy import Column, String, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, UTC

Base = declarative_base()

# Postgres video table model
class VideoModel(Base):
    """
    Video Model. SQL table model for video data.
    """
    __tablename__ = 'videos'

    # TODO: Create users table and link to it with users_id
    id = Column(String(64), primary_key=True)
    user_id = Column(Integer, nullable=False, default=0)
    title = Column(String(255), nullable=False)
    status = Column(String(20), nullable=False, default='pending')
    created_at = Column(DateTime, nullable=False, default=datetime.now(UTC))
