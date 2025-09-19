import { Router } from "express";
import { JobManager } from "../services/database.js";
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

router.get("/jobs", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = 20;
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

    const totalPages = Math.ceil(totalJobs / limit);

    res.render("jobs", {
      title: "All Jobs",
      jobs,
      pagination: {
        currentPage: page,
        totalPages,
        hasNext: page < totalPages,
        hasPrev: page > 1,
        nextPage: page + 1,
        prevPage: page - 1,
      },
      filters: {
        status: status || "all",
      },
    });
  } catch (error) {
    console.error("Jobs page error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
    });
  }
});

router.get("/job/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;
    const job = await JobManager.getJob(jobId);

    if (!job) {
      return res.status(404).render("error", {
        title: "Job Not Found",
        error: `Job ${jobId} not found`,
      });
    }

    res.render("job-detail", {
      title: `Job ${jobId}`,
      job,
    });
  } catch (error) {
    console.error("Job detail error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
    });
  }
});

router.get("/queue", async (req, res) => {
  try {
    const [queueStats, activeJobs, failedJobs] = await Promise.all([
      QueueManager.getQueueStats(),
      QueueManager.getActiveJobs(),
      QueueManager.getFailedJobs(10),
    ]);

    const isPaused = await QueueManager.isQueuePaused();

    res.render("queue", {
      title: "Queue Management",
      queueStats,
      activeJobs,
      failedJobs,
      isPaused,
    });
  } catch (error) {
    console.error("Queue page error:", error);
    res.render("error", {
      title: "Error",
      error: error.message,
    });
  }
});

router.get("/api/stats", async (req, res) => {
  try {
    const [jobCounts, queueStats, activeJobs] = await Promise.all([
      JobManager.getJobCounts(),
      QueueManager.getQueueStats(),
      QueueManager.getActiveJobs(),
    ]);

    res.json({
      jobCounts,
      queueStats,
      activeJobs: activeJobs.map((job) => ({
        id: job.id,
        jobId: job.data.jobId,
        originalKey: job.data.originalKey,
        progress: job.progress,
      })),
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Stats API error:", error);
    res.status(500).json({ error: error.message });
  }
});

export default router;
