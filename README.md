# Distributed Systems 2026

Stub repo for Distributed Systems 2026 project.

Contains a placeholder core and edge server and a Docker Compose project for running them in containers.


## Demo video
[Demo video](https://www.youtube.com/watch?v=FngewfpcUWM)

## Running

To run the entire project, run the following command with Docker installed

```sh
docker compose up --build -d
```

Or to start it in the foreground

```sh
docker compose up --build
```

The client will be available at http://localhost/

##### Backend:
- Data of all available videos: http://localhost/videos
- Upload a new video: http://localhost/upload
- Get thumbnail of video: `http://localhost/thumbnail/{video_id}`
- Get video HLS master playlist: `http://localhost/video/{video_id}`
