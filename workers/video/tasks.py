import os
import subprocess
import json
import shutil
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
result_backend = "redis://redis-backend:6379/0"
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
        video = session.query(VideoModel).filter_by(id=file_hash).first()
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


def generate_master_playlist(coded_variants, local_path):
    hls_metadata = {"1080p": {"bandwidth": 6000 * 10**3, "resolution": "1920x1080"},
                    "720p": {"bandwidth": 3500 * 10**3, "resolution": "1280x720"},
                    "360p": {"bandwidth": 1000 * 10**3, "resolution": "640x360"}}
    master_path = f"{local_path}/master.m3u8"
    with open(master_path, 'w') as master_playlist:
        master_playlist.write("#EXTM3U\n")
        for var in coded_variants:
            variant_metadata = hls_metadata[var]
            line = (f"#EXT-X-STREAM-INF:BANDWIDTH={variant_metadata['bandwidth']},"
                    f"RESOLUTION={variant_metadata['resolution']}\n")
            master_playlist.write(line)
            master_playlist.write(f"{var}/index.m3u8\n")
    return master_path


def upload_hls_files(local_path, file_hash):
    for root, dirs, files in os.walk(local_path):
        for file in files:
            file_path = str(os.path.join(root, file))
            rel_path = os.path.relpath(file_path, local_path)
            remote_object = f"{file_hash}/{rel_path}"

            storage.fput_object(bucket_name=VIDEO_BUCKET,
                         object_name=remote_object,
                         file_path=file_path,
                         content_type='application/vnd.apple.mpegurl' if file.endswith('.m3u8') else 'video/mp2t')


@app.task(name='tasks.transcode_video')
def transcode_video(file_hash):
    set_video_status(file_hash, status='processing')
    local_path = None
    try:

        obj_info = storage.stat_object(bucket_name=TMP_BUCKET, object_name=file_hash)

        # MinIO prefixes custom metadata with x-amz-meta-
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

        coded_variants = []
        local_path = f"/tmp/{file_hash}"
        os.makedirs(local_path, exist_ok=True)

        for resolution, [w, h] in reversed(variants.items()):

            if orig_w < w or orig_h < h:
                continue

            output_dir = f"/tmp/{file_hash}/{resolution}"
            os.makedirs(output_dir, exist_ok=True)

            vf_args = (
                f"scale={w}x{h}:force_original_aspect_ratio=decrease,"
                f"pad=x=(ow-iw)/2:y=(oh-ih)/2:aspect=16/9"
            )

            cmd = ["ffmpeg", "-y", "-i", local_source,
                   "-vf", vf_args,
                   "-c:v", "libx264",
                   "-preset", "veryfast",
                   "-g", "48", "-sc_threshold", "0",
                   "-c:a", "aac", "-b:a", "128k",
                   "-hls_time", "4",
                   "-hls_playlist_type", "vod",
                   "-hls_segment_filename", f"{output_dir}/seg_%03d.ts",
                   f"{output_dir}/index.m3u8"]

            subprocess.run(cmd, check=True)
            coded_variants.append(resolution)

        # Upload files to blob storage
        upload_hls_files(local_path, file_hash)

        # Generate and upload thumbnail
        thumbnail = f"/tmp/{file_hash}_thumbnail.jpg"
        subprocess.run(["ffmpeg", "-y",
                        "-ss", "00:00:01",
                        "-i", local_source,
                        "-vframes", "1",
                        "-q:v", "2",
                        "-update", "1"
                        ,thumbnail])
        storage.fput_object(VIDEO_BUCKET, f"{file_hash}/thumbnail.jpg", thumbnail)

        # Generate and upload master playlist
        master_path = generate_master_playlist(coded_variants, local_path)
        storage.fput_object(VIDEO_BUCKET, f"{file_hash}/master.m3u8", master_path)

        set_video_status(file_hash, status='ready')
        print(f"Transcoding successful for {original_name}!")

    except Exception as e:
        set_video_status(file_hash, status='error')
        data_to_clean = storage.list_objects(VIDEO_BUCKET, prefix=f"{file_hash}/", recursive=True)
        for data in data_to_clean:
            storage.remove_object(VIDEO_BUCKET, data.object_name)
        print(f"[TRANSCODING ERROR] transcoding failed for {file_hash}: {e}")
        return

    finally:
        if local_path and os.path.exists(local_path):
            shutil.rmtree(local_path)
            storage.remove_object(TMP_BUCKET, file_hash)
