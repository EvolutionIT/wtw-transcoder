import { Router } from "express";
import { basename, extname } from "path";
import { JobManager, LOG_LEVELS } from "../services/database.js";
import { QueueManager } from "../services/queue.js";

const router = Router();

// Note: Authentication middleware is now applied at the app level, not here

// Dashboard home
router.get("/", async (req, res) => {
  try {
    const [recentJobs, jobCounts, queueStats] = await Promise.all([
      JobManager.getRecentJobs(10),
      JobManager.getJobCounts(),
      QueueManager.getQueueStats(),
    ]);

    res.render("dashboard", {
      title: "Video Transcoding Service",
      recentJobs,
      jobCounts,
      queueStats,
      currentTime: new Date().toISOString(),
    });
  } catch (error) {
    console.error("❌ Dashboard error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
      stack: error.stack,
    });
  }
});

// Job details page
router.get("/job/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;

    const job = await JobManager.getJobWithLogs(jobId);

    if (!job) {
      return res.status(404).render("error", {
        title: "Job Not Found",
        error: "The requested job could not be found.",
        stack: null,
      });
    }

    // Get additional queue info if job is still in queue
    let queueInfo = null;
    if (job.status === "queued" || job.status === "processing") {
      try {
        const queue = QueueManager.getQueue();
        const bullJobs = await queue.getJobs(["waiting", "active", "delayed"]);
        const bullJob = bullJobs.find((j) => j.data.jobId === jobId);
        if (bullJob) {
          queueInfo = {
            id: bullJob.id,
            progress: bullJob._progress || 0,
            processedOn: bullJob.processedOn,
            timestamp: bullJob.timestamp,
            attemptsMade: bullJob.attemptsMade,
            opts: bullJob.opts,
          };
        }
      } catch (queueError) {
        console.warn("Could not get queue info:", queueError.message);
      }
    }

    res.render("job-details", {
      title: `Job ${jobId.substring(0, 8)}`,
      job,
      queueInfo,
      currentTime: new Date().toISOString(),
    });
  } catch (error) {
    console.error("❌ Job details error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
      stack: error.stack,
    });
  }
});

// Jobs list page
router.get("/jobs", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const status = req.query.status;
    const offset = (page - 1) * limit;

    let jobs;
    let totalJobs;

    if (status) {
      jobs = await JobManager.getJobsByStatus(status);
      totalJobs = jobs.length;
      jobs = jobs.slice(offset, offset + limit);
    } else {
      jobs = await JobManager.getAllJobs(limit, offset);
      const counts = await JobManager.getJobCounts();
      totalJobs = counts.total;
    }

    const jobCounts = await JobManager.getJobCounts();

    res.render("jobs-list", {
      title: "All Jobs",
      jobs,
      jobCounts,
      pagination: {
        page,
        limit,
        total: totalJobs,
        totalPages: Math.ceil(totalJobs / limit),
        hasNext: page * limit < totalJobs,
        hasPrev: page > 1,
      },
      filters: {
        status: status || "all",
      },
      currentTime: new Date().toISOString(),
    });
  } catch (error) {
    console.error("❌ Jobs list error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
      stack: error.stack,
    });
  }
});

// Logs page
router.get("/logs", async (req, res) => {
  try {
    const level = req.query.level;
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);

    let logs;
    if (level === "error") {
      logs = await JobManager.getErrorLogs(limit);
    } else {
      logs = await JobManager.getRecentLogs(limit);
    }

    res.render("logs", {
      title: "System Logs",
      logs,
      filters: {
        level: level || "all",
        limit,
      },
      logLevels: Object.values(LOG_LEVELS),
      currentTime: new Date().toISOString(),
    });
  } catch (error) {
    console.error("❌ Logs error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
      stack: error.stack,
    });
  }
});

// API endpoint for job logs (for AJAX updates)
router.get("/api/job/:jobId/logs", async (req, res) => {
  try {
    const { jobId } = req.params;
    const since = req.query.since; // ISO timestamp

    let logs = await JobManager.getJobLogs(jobId);

    if (since) {
      const sinceDate = new Date(since);
      logs = logs.filter((log) => new Date(log.created_at) > sinceDate);
    }

    res.json({
      success: true,
      logs,
      count: logs.length,
    });
  } catch (error) {
    console.error("❌ Job logs API error:", error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// API endpoint for deleting completed/failed jobs
router.delete("/api/job/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;

    const job = await JobManager.getJob(jobId);

    if (!job) {
      return res.status(404).json({
        success: false,
        error: "Job not found",
      });
    }

    // Only allow deletion of completed or failed jobs
    if (!["completed", "failed"].includes(job.status)) {
      return res.status(400).json({
        success: false,
        error: "Only completed or failed jobs can be deleted",
      });
    }

    // Remove from queue if it exists there
    try {
      const queue = QueueManager.getQueue();
      const bullJobs = await queue.getJobs(["completed", "failed"]);
      const bullJob = bullJobs.find((j) => j.data.jobId === jobId);
      if (bullJob) {
        await bullJob.remove();
        console.log(`🗑️ Removed job ${jobId} from queue`);
      }
    } catch (queueError) {
      console.warn("⚠️ Could not remove from queue:", queueError.message);
    }

    // Delete from database
    await JobManager.deleteJob(jobId);

    await JobManager.addJobLog(
      jobId,
      "info",
      "Job deleted by user",
      "deletion",
    );

    res.json({
      success: true,
      message: "Job deleted successfully",
    });
  } catch (error) {
    console.error("❌ Failed to delete job:", error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// API endpoint for retrying failed jobs
router.post("/api/job/:jobId/retry", async (req, res) => {
  try {
    const { jobId } = req.params;

    const job = await JobManager.getJob(jobId);

    if (!job) {
      return res.status(404).json({
        success: false,
        error: "Job not found",
      });
    }

    if (job.status !== "failed") {
      return res.status(400).json({
        success: false,
        error: "Only failed jobs can be retried",
      });
    }

    // Reset job status
    await JobManager.updateJobStatus(jobId, "queued");
    await JobManager.updateJobProgress(jobId, 0);

    // Get job details from metadata
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

    console.log(`🔄 Retrying job: ${jobId}`);

    res.json({
      success: true,
      message: "Job queued for retry",
    });
  } catch (error) {
    console.error("❌ Failed to retry job:", error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// API endpoint for job status (for AJAX updates)
router.get("/api/job/:jobId/status", async (req, res) => {
  try {
    const { jobId } = req.params;

    const job = await JobManager.getJob(jobId);

    if (!job) {
      return res.status(404).json({
        success: false,
        error: "Job not found",
      });
    }

    res.json({
      success: true,
      job: {
        id: job.job_id,
        status: job.status,
        progress: job.progress,
        error: job.error_message,
        completedAt: job.completed_at,
        startedAt: job.started_at,
      },
    });
  } catch (error) {
    console.error("❌ Job status API error:", error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

export default router;
