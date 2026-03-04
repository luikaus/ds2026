import { useEffect, useState } from 'react';
import { client, type Video } from './api';
import HLSPlayer from './HLSPlayer';

function App() {
  const [video, setVideo] = useState<Video | null>(null);
  const [videos, setVideos] = useState<Video[]>([]);

  useEffect(() => {
    client.getVideos().then(setVideos);
    const interval = setInterval(() => {
      client.getVideos().then(setVideos);
    }, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="App">
      <HLSPlayer src={video?.url} controls width="640" height="360" />
      {/* <video src={video?.url} controls width="640" height="360" /> */}

      <ul>
        {videos.map(v => <li><VideoButton key={v.id} title={v.title} value={v} onClick={setVideo} /></li>)}
      </ul>
    </div>
  );
}

function VideoButton({ title, value, onClick }: { title: string, value: Video, onClick: (video: Video) => void }) {
  return <button onClick={() => onClick(value)}>{title}</button>
}

export default App;
