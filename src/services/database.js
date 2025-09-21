import Database from "better-sqlite3";
import { join } from "path";
import { existsSync, mkdirSync } from "fs";

const DB_PATH = join(process.cwd(), "database.sqlite");
let db = null;

const JOB_STATUS = {
  QUEUED: "queued",
  PROCESSING: "processing",
  COMPLETED: "completed",
  FAILED: "failed",
};

const LOG_LEVELS = {
  INFO: "info",
  WARN: "warn",
  ERROR: "error",
  DEBUG: "debug",
};

function initializeDatabase() {
  return new Promise((resolve, reject) => {
    try {
      const uploadsDir = process.env.TEMP_UPLOAD_DIR || "./uploads";
      if (!existsSync(uploadsDir)) {
        mkdirSync(uploadsDir, { recursive: true });
      }

      db = new Database(DB_PATH);
      db.pragma("journal_mode = WAL");

      db.exec(`
        CREATE TABLE IF NOT EXISTS jobs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT UNIQUE NOT NULL,
          original_key TEXT NOT NULL,
          output_key TEXT,
          status TEXT NOT NULL DEFAULT 'queued',
          progress INTEGER DEFAULT 0,
          error_message TEXT,
          resolutions TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          started_at DATETIME,
          completed_at DATETIME,
          file_size INTEGER,
          duration REAL,
          metadata TEXT
        )
      `);

      db.exec(`
        CREATE TABLE IF NOT EXISTS job_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT NOT NULL,
          level TEXT NOT NULL,
          message TEXT NOT NULL,
          stage TEXT,
          details TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        )
      `);

      db.exec(`
        CREATE INDEX IF NOT EXISTS idx_jobs_job_id ON jobs(job_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
        CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);
        CREATE INDEX IF NOT EXISTS idx_job_logs_level ON job_logs(level);
        CREATE INDEX IF NOT EXISTS idx_job_logs_created_at ON job_logs(created_at);
      `);

      makeJobQueries();
      console.log("Database initialized successfully");
      resolve();
    } catch (error) {
      console.error("Database initialization failed:", error);
      reject(error);
    }
  });
}

let jobQueries = {};

const makeJobQueries = () => {
  jobQueries = {
    create: db
      ? db.prepare(`
        INSERT INTO jobs (job_id, original_key, status, resolutions, metadata)
        VALUES (?, ?, ?, ?, ?)
      `)
      : null,

    getById: db
      ? db.prepare(`
        SELECT * FROM jobs WHERE job_id = ?
      `)
      : null,

    updateStatus: db
      ? db.prepare(`
        UPDATE jobs 
        SET status = ?, 
            started_at = CASE WHEN ? = 'processing' THEN CURRENT_TIMESTAMP ELSE started_at END,
            completed_at = CASE WHEN ? IN ('completed', 'failed') THEN CURRENT_TIMESTAMP ELSE completed_at END
        WHERE job_id = ?
      `)
      : null,

    updateProgress: db
      ? db.prepare(`
        UPDATE jobs SET progress = ? WHERE job_id = ?
      `)
      : null,

    setError: db
      ? db.prepare(`
        UPDATE jobs SET status = 'failed', error_message = ?, completed_at = CURRENT_TIMESTAMP WHERE job_id = ?
      `)
      : null,

    complete: db
      ? db.prepare(`
        UPDATE jobs 
        SET status = 'completed', output_key = ?, progress = 100, completed_at = CURRENT_TIMESTAMP,
            file_size = ?, duration = ?, metadata = ?
        WHERE job_id = ?
      `)
      : null,

    getAll: db
      ? db.prepare(`
        SELECT * FROM jobs 
        ORDER BY created_at DESC 
        LIMIT ? OFFSET ?
      `)
      : null,

    getByStatus: db
      ? db.prepare(`
        SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC
      `)
      : null,

    getCounts: db
      ? db.prepare(`
        SELECT 
          status,
          COUNT(*) as count
        FROM jobs 
        GROUP BY status
      `)
      : null,

    getRecent: db
      ? db.prepare(`
        SELECT * FROM jobs 
        ORDER BY created_at DESC 
        LIMIT ?
      `)
      : null,

    addLog: db
      ? db.prepare(`
        INSERT INTO job_logs (job_id, level, message, stage, details)
        VALUES (?, ?, ?, ?, ?)
      `)
      : null,

    getLogsByJobId: db
      ? db.prepare(`
        SELECT * FROM job_logs 
        WHERE job_id = ? 
        ORDER BY created_at ASC
      `)
      : null,

    getRecentLogs: db
      ? db.prepare(`
        SELECT jl.*, j.original_key, j.status as job_status
        FROM job_logs jl
        JOIN jobs j ON jl.job_id = j.job_id
        ORDER BY jl.created_at DESC
        LIMIT ?
      `)
      : null,

    getErrorLogs: db
      ? db.prepare(`
        SELECT jl.*, j.original_key, j.status as job_status
        FROM job_logs jl
        JOIN jobs j ON jl.job_id = j.job_id
        WHERE jl.level = 'error'
        ORDER BY jl.created_at DESC
        LIMIT ?
      `)
      : null,

    deleteJob: db
      ? db.prepare(`
        DELETE FROM jobs WHERE job_id = ?
      `)
      : null,

    deleteJobLogs: db
      ? db.prepare(`
        DELETE FROM job_logs WHERE job_id = ?
      `)
      : null,
  };
};

class JobManager {
  static async createJob(jobId, originalKey, resolutions, metadata = {}) {
    try {
      if (!db) throw new Error("Database not initialized");

      const metadataJson = JSON.stringify(metadata);
      const resolutionsJson = JSON.stringify(resolutions);

      jobQueries.create.run(
        jobId,
        originalKey,
        "queued",
        resolutionsJson,
        metadataJson,
      );

      await this.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        `Job created for ${originalKey}`,
        "initialization",
        {
          resolutions,
          metadata,
        },
      );

      console.log(`Created job record: ${jobId}`);
      return jobId;
    } catch (error) {
      console.error("Failed to create job:", error);
      throw error;
    }
  }

  static async getJob(jobId) {
    if (!db) throw new Error("Database not initialized");

    const job = jobQueries.getById.get(jobId);
    if (!job) return null;

    if (job.resolutions) {
      try {
        job.resolutions = JSON.parse(job.resolutions);
      } catch (e) {
        job.resolutions = [];
      }
    }

    if (job.metadata) {
      try {
        job.metadata = JSON.parse(job.metadata);
      } catch (e) {
        job.metadata = {};
      }
    }

    return job;
  }

  static async getJobWithLogs(jobId) {
    const job = await this.getJob(jobId);
    if (!job) return null;

    const logs = await this.getJobLogs(jobId);
    return {
      ...job,
      logs,
    };
  }

  static async updateJobStatus(jobId, status) {
    if (!db) throw new Error("Database not initialized");

    jobQueries.updateStatus.run(status, status, status, jobId);

    await this.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      `Status changed to ${status}`,
      "status_update",
    );
  }

  static async updateJobProgress(jobId, progress) {
    if (!db) throw new Error("Database not initialized");
    jobQueries.updateProgress.run(Math.round(progress), jobId);

    if (progress % 25 === 0 || progress === 100) {
      await this.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        `Progress: ${Math.round(progress)}%`,
        "progress",
      );
    }
  }

  static async setJobError(jobId, errorMessage) {
    if (!db) throw new Error("Database not initialized");

    jobQueries.setError.run(errorMessage, jobId);

    await this.addJobLog(jobId, LOG_LEVELS.ERROR, errorMessage, "error");
  }

  static async completeJob(
    jobId,
    outputKey,
    fileSize = null,
    duration = null,
    metadata = {},
  ) {
    if (!db) throw new Error("Database not initialized");

    jobQueries.complete.run(
      outputKey,
      fileSize,
      duration,
      JSON.stringify(metadata),
      jobId,
    );

    await this.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      `Job completed successfully`,
      "completion",
      {
        outputKey,
        fileSize,
        duration,
        metadata,
      },
    );
  }

  static async getAllJobs(limit = 50, offset = 0) {
    if (!db) throw new Error("Database not initialized");

    const jobs = jobQueries.getAll.all(limit, offset);
    return jobs.map((job) => {
      if (job.resolutions) {
        try {
          job.resolutions = JSON.parse(job.resolutions);
        } catch (e) {
          job.resolutions = [];
        }
      }
      if (job.metadata) {
        try {
          job.metadata = JSON.parse(job.metadata);
        } catch (e) {
          job.metadata = {};
        }
      }
      return job;
    });
  }

  static async getJobsByStatus(status) {
    if (!db) throw new Error("Database not initialized");

    const jobs = jobQueries.getByStatus.all(status);
    return jobs.map((job) => {
      if (job.resolutions) {
        try {
          job.resolutions = JSON.parse(job.resolutions);
        } catch (e) {
          job.resolutions = [];
        }
      }
      if (job.metadata) {
        try {
          job.metadata = JSON.parse(job.metadata);
        } catch (e) {
          job.metadata = {};
        }
      }
      return job;
    });
  }

  static async getJobCounts() {
    if (!db) throw new Error("Database not initialized");

    const counts = jobQueries.getCounts.all();
    const result = {
      queued: 0,
      processing: 0,
      completed: 0,
      failed: 0,
      total: 0,
    };

    counts.forEach(({ status, count }) => {
      result[status] = count;
      result.total += count;
    });

    return result;
  }

  static async getRecentJobs(limit = 10) {
    if (!db) throw new Error("Database not initialized");

    const jobs = jobQueries.getRecent.all(limit);
    return jobs.map((job) => {
      if (job.resolutions) {
        try {
          job.resolutions = JSON.parse(job.resolutions);
        } catch (e) {
          job.resolutions = [];
        }
      }
      if (job.metadata) {
        try {
          job.metadata = JSON.parse(job.metadata);
        } catch (e) {
          job.metadata = {};
        }
      }
      return job;
    });
  }

  static async addJobLog(jobId, level, message, stage = null, details = null) {
    if (!db) throw new Error("Database not initialized");

    const detailsJson = details ? JSON.stringify(details) : null;

    jobQueries.addLog.run(jobId, level, message, stage, detailsJson);

    // Also log to console with job context
    const logPrefix = `[Job: ${jobId.substring(0, 8)}]`;
    switch (level) {
      case LOG_LEVELS.ERROR:
        console.error(`${logPrefix} ${message}`, details || "");
        break;
      case LOG_LEVELS.WARN:
        console.warn(`${logPrefix} ${message}`, details || "");
        break;
      case LOG_LEVELS.DEBUG:
        console.debug(`${logPrefix} ${message}`, details || "");
        break;
      default:
        console.log(`${logPrefix} ${message}`, details || "");
    }
  }

  static async getJobLogs(jobId) {
    if (!db) throw new Error("Database not initialized");

    const logs = jobQueries.getLogsByJobId.all(jobId);
    return logs.map((log) => {
      if (log.details) {
        try {
          log.details = JSON.parse(log.details);
        } catch (e) {
          // Keep as string if parsing fails
        }
      }
      return log;
    });
  }

  static async getRecentLogs(limit = 50) {
    if (!db) throw new Error("Database not initialized");

    const logs = jobQueries.getRecentLogs.all(limit);
    return logs.map((log) => {
      if (log.details) {
        try {
          log.details = JSON.parse(log.details);
        } catch (e) {
          // Keep as string if parsing fails
        }
      }
      return log;
    });
  }

  static async getErrorLogs(limit = 20) {
    if (!db) throw new Error("Database not initialized");

    const logs = jobQueries.getErrorLogs.all(limit);
    return logs.map((log) => {
      if (log.details) {
        try {
          log.details = JSON.parse(log.details);
        } catch (e) {
          // Keep as string if parsing fails
        }
      }
      return log;
    });
  }

  static async deleteJob(jobId) {
    if (!db) throw new Error("Database not initialized");

    jobQueries.deleteJobLogs.run(jobId);

    const result = jobQueries.deleteJob.run(jobId);

    console.log(`Deleted job record: ${jobId}`);
    return result.changes > 0;
  }
}

const getDatabase = () => db;

export { initializeDatabase, JobManager, JOB_STATUS, LOG_LEVELS, getDatabase };
