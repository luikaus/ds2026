export interface Video {
  id: string;
  title: string;
  url: string;
}

// API client with mocked responses before API is finalized
export class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
  }

  async getVideos(): Promise<Video[]> {
    const res = await fetch(`${this.baseUrl}/videos`);
    if (!res.ok) {
      console.error('Failed to get video list', res)
      return [];
    }

    const data = await res.json() as { id: string, title: string }[];

    return data.map(v => ({
      id: v.id,
      title: v.title,
      url: `${this.baseUrl}/video/${v.id}/master.m3u8`,
    }));
  }
}

export const client = new ApiClient(import.meta.env.VITE_API_BASE_URL);
