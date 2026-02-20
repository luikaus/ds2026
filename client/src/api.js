// API client with mocked responses before API is finalized
export class ApiClient {
    constructor(baseUrl) {
        this.baseUrl = baseUrl;
    }

    async getVideos() {
        // Something like this
        // const res = await fetch(`${baseUrl}/videoIds`);

        // For now, we just have a placeholder
        await new Promise(res => setTimeout(res, 1000));
        const videoIds = ['video-1', 'video-2', 'video-3'];

        return videoIds.map(id => ({
            id,
            title: id,
            url: `${this.baseUrl}/video/${id}`,
        }));
    }
}

export const client = new ApiClient(process.env.REACT_APP_API_BASE_URL);
