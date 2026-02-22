import os
import subprocess
import json
from celery import Celery
from minio import Minio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from shared import VideoModel


# Bucket paths
TMP_BUCKET = 'temp-uploads'
VIDEO_BUCKET = 'videos'

# Database endpoints
minio_endpoint = os.getenv('MINIO_ENDPOINT')
postgres_endpoint = os.getenv('POSTGRES_URL')

# Database user
access_key = os.getenv('MINIO_ROOT_USER')
secret_key = os.getenv('MINIO_ROOT_PASSWORD')

# RabbitMQ user
rabbitmq_user = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
rabbitmq_pass = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')
rabbitmq_host = os.getenv('RABBITMQ_DEFAULT_HOST', 'rabbitmq')

# RabbitMQ format: amqp://user:password@hostname:port//
broker_url = f"amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:5672//"
result_backend = "redis://redis_backend:6379/0"
engine = create_engine(postgres_endpoint)

app = Celery('tasks', broker=broker_url, backend=result_backend)
storage = Minio(
    endpoint=minio_endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)

session_factory = sessionmaker(bind=engine)


def set_video_status(file_hash, status):
    session = session_factory()
    try:
        video = session.query(VideoModel).filter_by(video_hash=file_hash).first()
        if video:
            video.status = status
            session.commit()
        session.close()
    except Exception as e:
        print(f"[POSTGRES ERROR] Could not set video status to \"{status}\" for {file_hash}: {e}")
        session.rollback()
    finally:
        session.close()


def get_video_dimensions(path):
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "json", path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)
    width = int(data['streams'][0]['width'])
    height = int(data['streams'][0]['height'])
    return width, height


@app.task(name='tasks.transcode_video')
def transcode_video(file_hash):
    set_video_status(file_hash, status='processing')
    local_source = None
    try:

        obj_info = storage.stat_object(bucket_name=TMP_BUCKET, object_name=file_hash)

        # (MinIO prefixes custom metadata with x-amz-meta-)
        original_name = obj_info.metadata.get('x-amz-meta-file-name')

        local_source = f"/tmp/{original_name}"
        storage.fget_object('temp-uploads', file_hash, local_source)

        orig_w, orig_h = get_video_dimensions(local_source)

        variants = {"1080p": [1920, 1080],
               "720p": [1280, 720],
               "360p": [640, 360]}

        if not storage.bucket_exists(VIDEO_BUCKET):
            storage.make_bucket(VIDEO_BUCKET)

        if orig_w < variants["360p"][0] or orig_h < variants["360p"][1]:
            raise ValueError("Invalid video dimensions!")

        for key, [w, h] in variants.items():

            if orig_w < w or orig_h < h:
                continue

            output_file = f"/tmp/{file_hash}_{key}.mp4"

            vf_args = (
                f"scale={w}x{h}:force_original_aspect_ratio=decrease,"
                f"pad=x=(ow-iw)/2:y=(oh-ih)/2:aspect=16/9"
            )

            cmd = ["ffmpeg", "-y", "-i", local_source,
                   "-vf", vf_args,
                   "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                   "-c:a", "aac", "-strict", "-2",
                   output_file]

            subprocess.run(cmd, check=True)

            # Upload to permanent database
            storage.fput_object(bucket_name=VIDEO_BUCKET,
                                object_name=f"{file_hash}/{key}.mp4",
                                file_path=output_file)
            os.remove(output_file)

        thumbnail = f"/tmp/{file_hash}_thumbnail.jpg"
        subprocess.run(["ffmpeg", "-y",
                        "-ss", "00:00:01",
                        "-i", local_source,
                        "-vframes", "1",
                        "-q:v", "2",
                        "-update", "1"
                        ,thumbnail])

        storage.fput_object(VIDEO_BUCKET, f"{file_hash}/thumbnail.jpg", thumbnail)

        os.remove(local_source)
        storage.remove_object("temp-uploads", file_hash)

        set_video_status(file_hash, status='ready')
        print(f"Transcoding successful for {original_name}!")

    except Exception as e:
        set_video_status(file_hash, status='error')
        print(f"[TRANSCODING ERROR] transcoding failed for {file_hash}: {e}")
        return

    finally:
        if local_source and os.path.exists(local_source):
            os.remove(local_source)
