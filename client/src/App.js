import { useEffect, useState } from 'react';
import { client } from './api';

function App() {
  const [video, setVideo] = useState(null);
  const [videos, setVideos] = useState([]);

  useEffect(() => {
    client.getVideos().then(setVideos);
  }, []);

  return (
    <div className="App">
      <video src={video?.url} controls width="640" height="360" />

      <ul>
        {videos.map(v => <li><VideoButton key={v.id} title={v.title} value={v} onClick={setVideo} /></li>)}
      </ul>
    </div>
  );
}

function VideoButton({ title, value, onClick }) {
  return <button onClick={() => onClick(value)}>{title}</button>
}

export default App;
