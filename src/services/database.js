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
        CREATE INDEX IF NOT EXISTS idx_jobs_job_id ON jobs(job_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
      `);

      makeJobQueries();
      resolve();
    } catch (error) {
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

      return jobId;
    } catch (error) {
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

  static async updateJobStatus(jobId, status) {
    if (!db) throw new Error("Database not initialized");
    jobQueries.updateStatus.run(status, status, status, jobId);
  }

  static async updateJobProgress(jobId, progress) {
    if (!db) throw new Error("Database not initialized");
    jobQueries.updateProgress.run(Math.round(progress), jobId);
  }

  static async setJobError(jobId, errorMessage) {
    if (!db) throw new Error("Database not initialized");
    jobQueries.setError.run(errorMessage, jobId);
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
}

const getDatabase = () => db;

export { initializeDatabase, JobManager, JOB_STATUS, getDatabase };
