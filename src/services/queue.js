import Queue from "bull";
import { createClient } from "redis";
import { basename, extname } from "path";
import { JobManager, JOB_STATUS } from "./database.js";
import transcodingWorker from "../workers/transcoding.js";

let transcodingQueue = null;
let redisClient = null;

async function initializeQueue() {
  try {
    redisClient = createClient({
      url: process.env.REDIS_URL || "redis://localhost:6379",
    });

    redisClient.on("error", (err) => {
      console.error("Redis Client Error:", err);
    });

    redisClient.on("connect", () => {
      console.log("Connected to Redis");
    });

    await redisClient.connect();

    transcodingQueue = new Queue(
      "transcoding",
      process.env.REDIS_URL || "redis://localhost:6379",
      {
        defaultJobOptions: {
          removeOnComplete: 50,
          removeOnFail: 20,
          attempts: 3,
          backoff: {
            type: "exponential",
            delay: 5000,
          },
        },
      },
    );

    const maxConcurrentJobs = parseInt(process.env.MAX_CONCURRENT_JOBS) || 2;
    transcodingQueue.process(
      "transcode-video",
      maxConcurrentJobs,
      transcodingWorker,
    );

    transcodingQueue.on("active", async (job) => {
      console.log(`Job ${job.id} started processing: ${job.data.originalKey}`);
      await JobManager.updateJobStatus(job.data.jobId, JOB_STATUS.PROCESSING);
    });

    transcodingQueue.on("progress", async (job, progress) => {
      console.log(`Job ${job.id} progress: ${progress}%`);
      await JobManager.updateJobProgress(job.data.jobId, progress);
    });

    transcodingQueue.on("completed", async (job, result) => {
      console.log(`Job ${job.id} completed: ${result.outputKey}`);
      await JobManager.completeJob(
        job.data.jobId,
        result.outputKey,
        result.fileSize,
        result.duration,
        result.metadata,
      );
    });

    transcodingQueue.on("failed", async (job, err) => {
      console.error(`Job ${job.id} failed:`, err.message);
      await JobManager.setJobError(job.data.jobId, err.message);
    });

    transcodingQueue.on("stalled", async (job) => {
      console.warn(`Job ${job.id} stalled, will retry`);
    });

    setInterval(
      async () => {
        try {
          await transcodingQueue.clean(24 * 60 * 60 * 1000, "completed");
          await transcodingQueue.clean(24 * 60 * 60 * 1000, "failed");
        } catch (error) {
          console.error("Error cleaning queue:", error);
        }
      },
      60 * 60 * 1000,
    );

    console.log("Queue system initialized successfully");
  } catch (error) {
    console.error("Queue initialization failed:", error);
    throw error;
  }
}

class QueueManager {
  static async addTranscodingJob(
    jobId,
    originalKey,
    resolutions,
    priority = 0,
    videoName = null,
    environment = "production",
    callbackUrl = null,
  ) {
    try {
      if (!transcodingQueue) {
        throw new Error("Queue not initialized");
      }

      const jobData = {
        jobId,
        originalKey,
        resolutions,
        videoName: videoName || basename(originalKey, extname(originalKey)),
        environment: environment,
        callbackUrl: callbackUrl,
      };

      const job = await transcodingQueue.add("transcode-video", jobData, {
        priority: priority,
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 2000,
        },
        removeOnComplete: 10,
        removeOnFail: 5,
      });

      console.log(`Added transcoding job to queue: ${jobId} (${environment})`);
      return job;
    } catch (error) {
      console.error("Failed to add transcoding job to queue:", error);
      throw error;
    }
  }

  static async getQueueStats() {
    if (!transcodingQueue) {
      throw new Error("Queue not initialized");
    }

    try {
      const [waiting, active, completed, failed, delayed] = await Promise.all([
        transcodingQueue.getWaiting(),
        transcodingQueue.getActive(),
        transcodingQueue.getCompleted(),
        transcodingQueue.getFailed(),
        transcodingQueue.getDelayed(),
      ]);

      return {
        waiting: waiting.length,
        active: active.length,
        completed: completed.length,
        failed: failed.length,
        delayed: delayed.length,
        total:
          waiting.length +
          active.length +
          completed.length +
          failed.length +
          delayed.length,
      };
    } catch (error) {
      console.error("Error getting queue stats:", error);
      return {
        waiting: 0,
        active: 0,
        completed: 0,
        failed: 0,
        delayed: 0,
        total: 0,
        error: error.message,
      };
    }
  }

  static async getActiveJobs() {
    if (!transcodingQueue) {
      throw new Error("Queue not initialized");
    }

    try {
      const activeJobs = await transcodingQueue.getActive();
      return activeJobs.map((job) => ({
        id: job.id,
        data: job.data,
        progress: job._progress || 0,
        processedOn: job.processedOn,
        timestamp: job.timestamp,
      }));
    } catch (error) {
      console.error("Error getting active jobs:", error);
      return [];
    }
  }

  static async getFailedJobs(limit = 10) {
    if (!transcodingQueue) {
      throw new Error("Queue not initialized");
    }

    try {
      const failedJobs = await transcodingQueue.getFailed(0, limit - 1);
      return failedJobs.map((job) => ({
        id: job.id,
        data: job.data,
        failedReason: job.failedReason,
        finishedOn: job.finishedOn,
        timestamp: job.timestamp,
        attemptsMade: job.attemptsMade,
      }));
    } catch (error) {
      console.error("Error getting failed jobs:", error);
      return [];
    }
  }

  static async retryFailedJob(jobId) {
    if (!transcodingQueue) {
      throw new Error("Queue not initialized");
    }

    try {
      const job = await transcodingQueue.getJob(jobId);
      if (job) {
        await job.retry();
        console.log(`Retrying job ${jobId}`);
        return true;
      }
      return false;
    } catch (error) {
      console.error("Error retrying job:", error);
      throw error;
    }
  }

  static async retryAllFailedJobs() {
    if (!transcodingQueue) {
      throw new Error("Queue not initialized");
    }

    try {
      const failedJobs = await transcodingQueue.getFailed();
      let retriedCount = 0;

      for (const job of failedJobs) {
        try {
          await job.retry();
          retriedCount++;
          console.log(`Retried job ${job.id} (jobId: ${job.data.jobId})`);
        } catch (error) {
          console.error(`Failed to retry job ${job.id}:`, error.message);
        }
      }

      console.log(
        `Retried ${retriedCount} out of ${failedJobs.length} failed jobs`,
      );
      return {
        success: true,
        retriedCount,
        totalFailed: failedJobs.length,
        message: `Successfully queued ${retriedCount} jobs for retry`,
      };
    } catch (error) {
      console.error("Error retrying all failed jobs:", error);
      throw error;
    }
  }

  static async removeJob(jobId) {
    if (!transcodingQueue) {
      throw new Error("Queue not initialized");
    }

    try {
      const job = await transcodingQueue.getJob(jobId);
      if (job) {
        await job.remove();
        console.log(`Removed job ${jobId}`);
        return true;
      }
      return false;
    } catch (error) {
      console.error("Error removing job:", error);
      throw error;
    }
  }

  static async pauseQueue() {
    if (!transcodingQueue) {
      throw new Error("Queue not initialized");
    }
    await transcodingQueue.pause();
    console.log("Queue paused");
  }

  static async resumeQueue() {
    if (!transcodingQueue) {
      throw new Error("Queue not initialized");
    }
    await transcodingQueue.resume();
    console.log("Queue resumed");
  }

  static async isQueuePaused() {
    if (!transcodingQueue) {
      throw new Error("Queue not initialized");
    }
    return await transcodingQueue.isPaused();
  }

  static getQueue() {
    return transcodingQueue;
  }

  static async getRedisInfo() {
    if (!redisClient) {
      return { connected: false };
    }

    try {
      const info = await redisClient.ping();
      return {
        connected: true,
        ping: info,
        url: process.env.REDIS_URL || "redis://localhost:6379",
      };
    } catch (error) {
      return {
        connected: false,
        error: error.message,
      };
    }
  }
}

process.on("SIGTERM", async () => {
  console.log("Shutting down queue...");
  if (transcodingQueue) {
    await transcodingQueue.close();
  }
  if (redisClient) {
    await redisClient.disconnect();
  }
});

export { initializeQueue, QueueManager };
