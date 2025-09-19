# Video Transcoding Service

A robust video transcoding service built with Node.js that automatically converts videos to multiple resolutions and generates HLS (HTTP Live Streaming) playlists. Perfect for video streaming platforms and web applications.

## Features

- **Multi-resolution transcoding** (1080p, 720p, 480p, 360p, 240p)
- **HLS output** with adaptive bitrate streaming
- **BackBlaze B2 integration** for storage
- **Redis-backed job queue** with persistence
- **Real-time progress tracking**
- **Web dashboard** for monitoring
- **RESTful API** for integration
- **Automatic retry** on failures
- **Concurrent job processing**
- **PM2 production ready**

## Architecture

```
Web App → API Call → Transcoding Service
                           ↓
                   Queue Job (Redis/SQLite)
                           ↓
                   Worker Process (FFmpeg)
                           ↓
           Download B2 → Transcode → Upload B2 → Callback
```

## Prerequisites

### System Requirements

- **Node.js** 18+
- **Redis** server
- **FFmpeg** 4.0+
- **4 vCPUs, 8GB RAM** (recommended)

### BackBlaze B2 Setup

1. Create a BackBlaze B2 account
2. Create a bucket for your videos
3. Generate application keys with read/write permissions
4. Note down: Key ID, Application Key, Bucket ID, Bucket Name

## Installation

### 1. Clone and Setup

```bash
git clone <your-repo>
cd video-transcoding-service
npm install
```

### 2. Install System Dependencies

**Ubuntu/Debian:**

```bash
# Redis
sudo apt update
sudo apt install redis-server

# FFmpeg
sudo apt install ffmpeg

# PM2 (optional, for production)
sudo npm install -g pm2
```

### 3. Configure Environment

```bash
# Run setup script
npm run setup

# Edit configuration
nano .env
```

**Required .env configuration:**

```env
# Server
PORT=3000
NODE_ENV=production

# Redis
REDIS_URL=redis://localhost:6379

# BackBlaze B2
B2_APPLICATION_KEY_ID=your_key_id_here
B2_APPLICATION_KEY=your_application_key_here
B2_BUCKET_ID=your_bucket_id_here
B2_BUCKET_NAME=your_bucket_name_here

# Your Web App
WEBAPP_CALLBACK_URL=https://your-webapp.com/api/transcoding-complete
WEBAPP_API_KEY=your_webapp_api_key_here

# Transcoding
MAX_CONCURRENT_JOBS=2
TEMP_UPLOAD_DIR=./uploads
OUTPUT_RESOLUTIONS=1080p,720p,480p,360p

# Security (optional)
API_KEY=your_secure_api_key_here
```

### 4. Start Services

```bash
# Start Redis
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Development mode
npm run dev

# Production mode with PM2
pm2 start ecosystem.config.js
pm2 save
pm2 startup
```

## Dashboard

Visit `http://your-server:3000` to access the web dashboard:

- **Real-time job monitoring**
- **Queue statistics**
- **Job history and details**
- **System health checks**
- **Auto-refresh every 5 seconds**

## API Usage

### Start Transcoding Job

```bash
curl -X POST http://localhost:3000/api/transcode \
  -H "Content-Type: application/json" \
  -H "x-api-key: your_api_key" \
  -d '{
    "key": "videos/input-video.mp4",
    "resolutions": ["720p", "480p", "360p"],
    "priority": 0
  }'
```

**Response:**

```json
{
  "success": true,
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "originalKey": "videos/input-video.mp4",
  "resolutions": ["720p", "480p", "360p"],
  "status": "queued",
  "message": "Transcoding job created successfully"
}
```

### Check Job Status

```bash
curl http://localhost:3000/api/job/550e8400-e29b-41d4-a716-446655440000
```

**Response:**

```json
{
  "success": true,
  "job": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "originalKey": "videos/input-video.mp4",
    "outputKey": "transcoded/2025-01-15/input-video/master.m3u8",
    "status": "completed",
    "progress": 100,
    "resolutions": ["720p", "480p", "360p"],
    "createdAt": "2025-01-15T10:30:00.000Z",
    "completedAt": "2025-01-15T10:35:00.000Z",
    "duration": 120.5,
    "fileSize": 15728640
  }
}
```

### List All Jobs

```bash
curl "http://localhost:3000/api/jobs?page=1&limit=20&status=completed"
```

### Health Check

```bash
curl http://localhost:3000/api/health
```

## Web App Integration

Your web app should implement a callback endpoint to receive transcoding completion notifications:

### Callback Endpoint (Your Web App)

```javascript
// POST /api/transcoding-complete
app.post('/api/transcoding-complete', (req, res) => {
  const { jobId, originalKey, outputKey, status, metadata } = req.body;

  if (status === 'completed') {
    // Update database: replace originalKey with outputKey
    await updateVideoRecord(originalKey, outputKey);
    console.log(`Video transcoded: ${originalKey} → ${outputKey}`);
  } else if (status === 'failed') {
    console.error(`Transcoding failed: ${originalKey}`);
  }

  res.json({ success: true });
});
```

### Triggering Transcoding (Your Web App)

```javascript
// After uploading video to B2
async function startTranscoding(videoKey) {
  const response = await fetch(
    "http://your-transcoding-service:3000/api/transcode",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": "your_api_key",
      },
      body: JSON.stringify({
        key: videoKey,
        resolutions: ["720p", "480p", "360p"],
      }),
    },
  );

  const result = await response.json();
  console.log("Transcoding started:", result.jobId);
}
```

## Output Structure

The service organizes transcoded files in B2 with this structure:

```
transcoded/
├── 2025-01-15/
│   └── video-name/
│       ├── master.m3u8           # Master playlist
│       ├── 720p/
│       │   ├── playlist.m3u8     # 720p playlist
│       │   ├── segment_000.ts    # Video segments
│       │   ├── segment_001.ts
│       │   └── ...
│       ├── 480p/
│       │   ├── playlist.m3u8
│       │   └── segments...
│       └── 360p/
│           ├── playlist.m3u8
│           └── segments...
```

## Video Player Integration

Use the master playlist URL in your video player:

```html
<!-- Video.js -->
<video-js id="player" controls preload="auto" data-setup="{}">
  <source
    src="https://f002.backblazeb2.com/file/your-bucket/transcoded/2025-01-15/video-name/master.m3u8"
    type="application/x-mpegURL"
  />
</video-js>

<!-- HLS.js -->
<video id="video" controls></video>
<script>
  const video = document.getElementById("video");
  const hls = new Hls();
  hls.loadSource(
    "https://f002.backblazeb2.com/file/your-bucket/transcoded/2025-01-15/video-name/master.m3u8",
  );
  hls.attachMedia(video);
</script>
```

## Production Configuration

### PM2 Management

```bash
# Start
pm2 start ecosystem.config.js

# Monitor
pm2 monit

# Logs
pm2 logs transcoding-service

# Restart
pm2 restart transcoding-service

# Stop
pm2 stop transcoding-service

# Auto-start on boot
pm2 startup
pm2 save
```

### Nginx Reverse Proxy

```nginx
server {
    listen 80;
    server_name your-transcoding-domain.com;

    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
    }
}
```

### Monitoring & Logs

```bash
# View logs
tail -f logs/combined.log

# Monitor Redis
redis-cli monitor

# System resources
htop
iostat -x 1

# PM2 monitoring
pm2 monit
```

## Troubleshooting

### Common Issues

**Redis Connection Failed:**

```bash
sudo systemctl status redis-server
sudo systemctl start redis-server
```

**FFmpeg Not Found:**

```bash
which ffmpeg
sudo apt install ffmpeg
```

**Permission Errors:**

```bash
sudo chown -R $USER:$USER ./uploads
chmod 755 ./uploads
```

**High Memory Usage:**

- Reduce `MAX_CONCURRENT_JOBS` in .env
- Monitor with `pm2 monit`
- Check video file sizes

**B2 Upload Failures:**

- Verify B2 credentials
- Check bucket permissions
- Monitor network connectivity

### Debug Mode

```bash
NODE_ENV=development DEBUG=* npm run dev
```

## Performance Tuning

### EC2 Instance Optimization

- **c5.xlarge** (4 vCPU, 8GB) for moderate load
- **c5.2xlarge** (8 vCPU, 16GB) for high load
- Use **GP3 SSD** storage for temp files
- Configure swap if needed

### Concurrency Settings

```env
# Conservative (4 vCPU)
MAX_CONCURRENT_JOBS=2

# Aggressive (8+ vCPU)
MAX_CONCURRENT_JOBS=4
```

### Redis Optimization

```bash
# /etc/redis/redis.conf
maxmemory 2gb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
```

## API Reference

| Endpoint             | Method | Description           |
| -------------------- | ------ | --------------------- |
| `/api/transcode`     | POST   | Start transcoding job |
| `/api/job/:id`       | GET    | Get job status        |
| `/api/jobs`          | GET    | List all jobs         |
| `/api/job/:id`       | DELETE | Cancel job            |
| `/api/job/:id/retry` | POST   | Retry failed job      |
| `/api/queue/stats`   | GET    | Queue statistics      |
| `/api/queue/pause`   | POST   | Pause queue           |
| `/api/queue/resume`  | POST   | Resume queue          |
| `/api/health`        | GET    | Health check          |

## Support

- Check logs: `pm2 logs transcoding-service`
- Monitor dashboard: `http://localhost:3000`
- Health endpoint: `http://localhost:3000/api/health`
