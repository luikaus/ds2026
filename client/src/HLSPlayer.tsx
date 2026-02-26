import { useEffect, useRef, type VideoHTMLAttributes } from 'react';
import Hls from 'hls.js';

interface HLSPlayerProps extends VideoHTMLAttributes<HTMLVideoElement> {
  src: string | undefined;
}

function HLSPlayer({ src, ...videoProps }: HLSPlayerProps) {
  const videoRef = useRef<HTMLVideoElement>(null);

  useEffect(() => {
    if (!src) return;

    if (!Hls.isSupported()) {
      console.error('HLS not supported');
      return;
    }

    const hls = new Hls();
    hls.loadSource(src);
    hls.attachMedia(videoRef.current!);

    return () => {
      hls.destroy();
    };
  }, [src]);

  return <video ref={videoRef} {...videoProps} />;
}

export default HLSPlayer;
