import os
import subprocess
from celery import Celery
from minio import Minio

TMP_BUCKET = 'temp-uploads'
VIDEO_BUCKET = 'videos'

# Database endpoint
minio_endpoint = os.getenv('MINIO_ENDPOINT')

# Database user
access_key = os.getenv('MINIO_ROOT_USER')
secret_key = os.getenv('MINIO_ROOT_PASSWORD')

# RabbitMQ user
rabbitmq_user = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
rabbitmq_pass = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')
rabbitmq_host = os.getenv('RABBITMQ_DEFAULT_HOST', 'rabbitmq')

# The format: amqp://user:password@hostname:port//
broker_url = f"amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:5672//"
result_backend = "redis://redis_backend:6379/0"

app = Celery('tasks', broker=broker_url, backend=result_backend)
storage = Minio(
    endpoint=minio_endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)

@app.task
def transcode_video(file_hash):
    obj_info = storage.stat_object(bucket_name=TMP_BUCKET, object_name=file_hash)

    # (MinIO prefixes custom metadata with x-amz-meta-)
    original_name = obj_info.metadata.get('x-amz-meta-file-name')

    local_source = f"/tmp/{original_name}"
    storage.fget_object('temp-uploads', file_hash, local_source)

    # TODO: FIX POSSIBLE ERROR WITH VIDEOS (RES < 1920x1080)!

    variants = {"1080p": ["1920", "1080"],
               "720p": ["1280", "720"],
               "360p": ["640", "360"]}

    if not storage.bucket_exists(VIDEO_BUCKET):
        storage.make_bucket(VIDEO_BUCKET)

    try:
        for key, [w, h] in variants.items():
            output_file = f"/tmp/{file_hash}_{key}.mp4"
            cmd = ["ffmpeg", "-y", "-i", local_source,
                   "-vf", f"scale={w}x{h},pad=x=(ow-iw)/2:y=(oh-ih)/2:aspect=16/9",
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

    except subprocess.CalledProcessError as e:
        print(f"Transcoding failed for {file_hash}: {e}")

    print(f"Transcoding successful for {original_name}!")

    # TODO: Some message queue to store/inform that video has been uploaded successfully
