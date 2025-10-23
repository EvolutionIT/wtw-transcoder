import { Router } from "express";
import { v4 as uuidv4 } from "uuid";
import { JobManager } from "../services/database.js";
import { QueueManager } from "../services/queue.js";
import { getB2Service, BUCKET_TYPES } from "../services/b2.js";
import { basename, extname } from "path";

const router = Router();

function authenticate(req, res, next) {
  const apiKey =
    req.headers["x-api-key"] ||
    req.headers["authorization"]?.replace("Bearer ", "");
  if (process.env.API_KEY && apiKey !== process.env.API_KEY) {
    return res.status(401).json({ error: "Invalid API key" });
  }
  next();
}

router.post("/", authenticate, async (req, res) => {
  try {
    const { key, resolutions, priority, videoName, callback_url } = req.body;

    if (!key) {
      return res.status(400).json({ error: "Missing required parameter: key" });
    }

    const environment = callback_url.includes("stage")
      ? "staging"
      : "production";

    if (callback_url) {
      try {
        const url = new URL(callback_url);
        if (!["http:", "https:"].includes(url.protocol)) {
          return res.status(400).json({
            error: "Callback URL must use HTTP or HTTPS protocol",
          });
        }
      } catch (urlError) {
        return res.status(400).json({
          error: "Invalid callback URL format",
        });
      }
    }

    const targetResolutions = resolutions || [
      "1080p",
      "720p",
      "480p",
      "360p",
      "240p",
    ];

    const validResolutions = ["1080p", "720p", "480p", "360p", "240p"];
    const invalidResolutions = targetResolutions.filter(
      (r) => !validResolutions.includes(r),
    );

    if (invalidResolutions.length > 0) {
      return res.status(400).json({
        error: `Invalid resolutions: ${invalidResolutions.join(", ")}. Valid options: ${validResolutions.join(", ")}`,
      });
    }

    const outputVideoName = videoName || basename(key, extname(key));

    const videoNameRegex = /^[a-zA-Z0-9_-]+$/;
    if (!videoNameRegex.test(outputVideoName)) {
      return res.status(400).json({
        error:
          "videoName must contain only alphanumeric characters, hyphens, and underscores",
      });
    }

    try {
      const b2Service = getB2Service();
      const fileInfo = await b2Service.getFileInfo(
        key,
        BUCKET_TYPES.ORIGINAL_VIDEO,
      );
      if (!fileInfo) {
        return res
          .status(404)
          .json({ error: `File not found in Original Video bucket: ${key}` });
      }
      console.log(
        `Found file: ${key} (${fileInfo.formattedSize}) in OV bucket`,
      );
    } catch (b2Error) {
      console.warn("Could not verify file in B2:", b2Error.message);
      // let the worker handle the error
    }

    const jobId = uuidv4();

    const jobMetadata = {
      videoName: outputVideoName,
      environment: environment,
      callbackUrl: callback_url || null,
      originalFileSize: null,
    };

    await JobManager.createJob(jobId, key, targetResolutions, jobMetadata);

    await QueueManager.addTranscodingJob(
      jobId,
      key,
      targetResolutions,
      priority || 0,
      outputVideoName,
      environment,
      callback_url,
    );

    console.log(
      `New transcoding job created: ${jobId} for ${key} -> ${outputVideoName} (${environment})`,
    );

    res.status(201).json({
      success: true,
      jobId,
      originalKey: key,
      videoName: outputVideoName,
      environment: environment,
      callbackUrl: callback_url,
      resolutions: targetResolutions,
      status: "queued",
      message: "Transcoding job created successfully",
    });
  } catch (error) {
    console.error("Failed to create transcoding job:", error);
    res.status(500).json({
      error: "Failed to create transcoding job",
      message: error.message,
    });
  }
});

router.get("/job/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;
    const job = await JobManager.getJob(jobId);

    if (!job) {
      return res.status(404).json({ error: "Job not found" });
    }

    res.json({
      success: true,
      job: {
        id: job.job_id,
        originalKey: job.original_key,
        outputKey: job.output_key,
        videoName: job.metadata?.videoName,
        status: job.status,
        progress: job.progress,
        error: job.error_message,
        resolutions: job.resolutions,
        createdAt: job.created_at,
        startedAt: job.started_at,
        completedAt: job.completed_at,
        fileSize: job.file_size,
        duration: job.duration,
        metadata: job.metadata,
      },
    });
  } catch (error) {
    console.error("Failed to get job status:", error);
    res
      .status(500)
      .json({ error: "Failed to get job status", message: error.message });
  }
});

router.get("/jobs", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const status = req.query.status;
    const offset = (page - 1) * limit;

    let jobs;
    if (status) {
      jobs = await JobManager.getJobsByStatus(status);
      jobs = jobs.slice(offset, offset + limit);
    } else {
      jobs = await JobManager.getAllJobs(limit, offset);
    }

    const counts = await JobManager.getJobCounts();

    res.json({
      success: true,
      jobs: jobs.map((job) => ({
        id: job.job_id,
        originalKey: job.original_key,
        outputKey: job.output_key,
        videoName: job.metadata?.videoName,
        status: job.status,
        progress: job.progress,
        error: job.error_message,
        resolutions: job.resolutions,
        createdAt: job.created_at,
        startedAt: job.started_at,
        completedAt: job.completed_at,
        fileSize: job.file_size,
        duration: job.duration,
      })),
      pagination: {
        page,
        limit,
        total: counts.total,
      },
      summary: counts,
    });
  } catch (error) {
    console.error("Failed to get jobs:", error);
    res
      .status(500)
      .json({ error: "Failed to get jobs", message: error.message });
  }
});

router.delete("/job/:jobId", authenticate, async (req, res) => {
  try {
    const { jobId } = req.params;
    const job = await JobManager.getJob(jobId);

    if (!job) {
      return res.status(404).json({ error: "Job not found" });
    }

    if (job.status === "queued") {
      try {
        const queue = QueueManager.getQueue();
        const bullJobs = await queue.getJobs(["waiting", "delayed"]);
        const bullJob = bullJobs.find((j) => j.data.jobId === jobId);
        if (bullJob) {
          await bullJob.remove();
          console.log(`Removed job ${jobId} from queue`);
        }
      } catch (queueError) {
        console.warn("Could not remove from queue:", queueError.message);
      }
    }

    await JobManager.setJobError(jobId, "Job cancelled by user");
    res.json({ success: true, message: "Job cancelled successfully" });
  } catch (error) {
    console.error("Failed to cancel job:", error);
    res
      .status(500)
      .json({ error: "Failed to cancel job", message: error.message });
  }
});

router.post("/job/:jobId/retry", authenticate, async (req, res) => {
  try {
    const { jobId } = req.params;
    const job = await JobManager.getJob(jobId);

    if (!job) {
      return res.status(404).json({ error: "Job not found" });
    }

    if (job.status !== "failed") {
      return res.status(400).json({ error: "Only failed jobs can be retried" });
    }

    // Reset job status
    await JobManager.updateJobStatus(jobId, "queued");
    await JobManager.updateJobProgress(jobId, 0);

    // Get videoName, environment, and callback from metadata
    const videoName =
      job.metadata?.videoName ||
      basename(job.original_key, extname(job.original_key));
    const environment = job.metadata?.environment || "production";
    const callbackUrl = job.metadata?.callbackUrl || null;

    await QueueManager.addTranscodingJob(
      jobId,
      job.original_key,
      job.resolutions,
      0,
      videoName,
      environment,
      callbackUrl,
    );

    console.log(`Retrying job: ${jobId}`);
    res.json({ success: true, message: "Job queued for retry" });
  } catch (error) {
    console.error("Failed to retry job:", error);
    res
      .status(500)
      .json({ error: "Failed to retry job", message: error.message });
  }
});

router.get("/queue/stats", async (req, res) => {
  try {
    const queueStats = await QueueManager.getQueueStats();
    const jobCounts = await JobManager.getJobCounts();
    const activeJobs = await QueueManager.getActiveJobs();
    const redisInfo = await QueueManager.getRedisInfo();

    res.json({
      success: true,
      queue: queueStats,
      database: jobCounts,
      activeJobs: activeJobs.map((job) => ({
        id: job.id,
        jobId: job.data.jobId,
        originalKey: job.data.originalKey,
        videoName: job.data.videoName,
        progress: job.progress,
        startedAt: job.processedOn,
      })),
      redis: redisInfo,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Failed to get queue stats:", error);
    res
      .status(500)
      .json({ error: "Failed to get queue stats", message: error.message });
  }
});

router.post("/queue/pause", authenticate, async (req, res) => {
  try {
    await QueueManager.pauseQueue();
    res.json({ success: true, message: "Queue paused" });
  } catch (error) {
    res
      .status(500)
      .json({ error: "Failed to pause queue", message: error.message });
  }
});

router.post("/queue/resume", authenticate, async (req, res) => {
  try {
    await QueueManager.resumeQueue();
    res.json({ success: true, message: "Queue resumed" });
  } catch (error) {
    res
      .status(500)
      .json({ error: "Failed to resume queue", message: error.message });
  }
});

router.get("/queue/status", async (req, res) => {
  try {
    const isPaused = await QueueManager.isQueuePaused();
    res.json({
      success: true,
      paused: isPaused,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res
      .status(500)
      .json({ error: "Failed to get queue status", message: error.message });
  }
});

router.get("/files/:fileName/info", async (req, res) => {
  try {
    const fileName = req.params.fileName;
    const bucketType = req.query.bucket || "hls"; // 'ov' for original video, 'hls' for output

    const b2Service = getB2Service();
    const selectedBucket =
      bucketType === "ov"
        ? BUCKET_TYPES.ORIGINAL_VIDEO
        : BUCKET_TYPES.HLS_OUTPUT;

    const fileInfo = await b2Service.getFileInfo(fileName, selectedBucket);

    if (!fileInfo) {
      return res.status(404).json({ error: "File not found in B2" });
    }

    res.json({
      success: true,
      file: fileInfo,
      bucketType: selectedBucket,
      publicUrl: b2Service.getPublicUrl(fileName, selectedBucket),
    });
  } catch (error) {
    res
      .status(500)
      .json({ error: "Failed to get file info", message: error.message });
  }
});

router.post("/test/callback", (req, res) => {
  console.log("Received test callback:", JSON.stringify(req.body, null, 2));
  res.json({
    success: true,
    message: "Callback received",
    receivedData: req.body,
    timestamp: new Date().toISOString(),
  });
});

router.get("/health", async (req, res) => {
  try {
    const queueStats = await QueueManager.getQueueStats();
    const redisInfo = await QueueManager.getRedisInfo();
    const jobCounts = await JobManager.getJobCounts();

    res.json({
      success: true,
      status: "healthy",
      services: {
        database: "connected",
        redis: redisInfo.connected ? "connected" : "disconnected",
        queue: queueStats.total !== undefined ? "connected" : "error",
      },
      stats: { queue: queueStats, jobs: jobCounts },
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      status: "unhealthy",
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

export default router;
