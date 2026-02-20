import hashlib
import os
from celery import Celery
from flask import Flask, request, jsonify, render_template
from minio import Minio
from werkzeug.utils import secure_filename

TMP_BUCKET = 'temp-uploads'

# Database endpoint
minio_endpoint = os.getenv('MINIO_ENDPOINT')

# Database user
access_key = os.getenv('MINIO_ROOT_USER')
secret_key = os.getenv('MINIO_ROOT_PASSWORD')

# RabbitMQ user
rabbitmq_user = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
rabbitmq_pass = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')
rabbitmq_host = os.getenv('RABBITMQ_DEFAULT_HOST', 'rabbitmq')

templates_path = os.path.join(os.path.dirname(__file__), 'templates')

broker_url = f"amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:5672"
result_backend = "redis://redis_backend:6379/0"

app = Flask(__name__, template_folder=templates_path)
celery_app = Celery('tasks',
                    broker=broker_url,
                    backend=result_backend)

# Allow uploads up to 500MB
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

# Get connection to database
storage = Minio(minio_endpoint, access_key, secret_key , secure=False)

def get_file_hash(file_stream):
    hasher = hashlib.sha256()
    while chunk := file_stream.read(8192):
        hasher.update(chunk)
    file_stream.seek(0)
    return hasher.hexdigest()

@app.route('/upload', methods=['GET'])
def upload_page():
    return render_template('upload.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return "No file part", 400

    file = request.files['file']
    if file.filename == '':
        return "No selected file", 400

    # Get all necessary data from file
    # TODO: some file extension validation checks?
    file_name = secure_filename(file.filename)
    file_hash = get_file_hash(file.stream)
    _, extension = os.path.splitext(file_name)
    file_metadata = {"file-name": file_name,
                     "ext": extension,
                     "user-id'": "default_user"}
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)

    if not storage.bucket_exists(TMP_BUCKET):
        storage.make_bucket(TMP_BUCKET)

    # Upload temp raw video
    storage.put_object(bucket_name=TMP_BUCKET,
                       object_name=file_hash,
                       data=file.stream,
                       length=file_size,
                       part_size=10*1024*1024,
                       content_type=file.content_type,
                       metadata=file_metadata)

    # Start video transcoding process
    celery_app.send_task('tasks.transcode_video', args=[file_hash])

    return jsonify({"status": "success", "filename": file_name})
