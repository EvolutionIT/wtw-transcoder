import { Router } from "express";
import { JobManager, LOG_LEVELS } from "../services/database.js";
import { QueueManager } from "../services/queue.js";

const router = Router();

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
    console.error("Dashboard error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
      stack: error.stack,
    });
  }
});

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
    console.error("Job details error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
      stack: error.stack,
    });
  }
});

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
    console.error("Jobs list error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
      stack: error.stack,
    });
  }
});

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
    console.error("Logs error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
      stack: error.stack,
    });
  }
});

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
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

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

    if (!["completed", "failed"].includes(job.status)) {
      return res.status(400).json({
        success: false,
        error: "Only completed or failed jobs can be deleted",
      });
    }

    try {
      const queue = QueueManager.getQueue();
      const bullJobs = await queue.getJobs(["completed", "failed"]);
      const bullJob = bullJobs.find((j) => j.data.jobId === jobId);
      if (bullJob) {
        await bullJob.remove();
        console.log(`Removed job ${jobId} from queue`);
      }
    } catch (queueError) {
      console.warn("Could not remove from queue:", queueError.message);
    }

    await JobManager.deleteJob(jobId);

    res.json({
      success: true,
      message: "Job deleted successfully",
    });
  } catch (error) {
    console.error("Failed to delete job:", error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

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
    console.error("Job status API error:", error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

export default router;
