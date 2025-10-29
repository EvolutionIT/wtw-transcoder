import ffmpeg from "fluent-ffmpeg";
const { ffprobe } = ffmpeg;
import { join, basename, extname, dirname } from "path";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  rmSync,
  writeFileSync,
  readFileSync,
  statSync,
} from "fs";
import { getB2Service, BUCKET_TYPES } from "../services/b2.js";
import { JobManager, LOG_LEVELS } from "../services/database.js";
import axios from "axios";

// Format validation helpers (inline for simplicity)
const SUPPORTED_VIDEO_FORMATS = [
  ".mp4",
  ".m4v",
  ".mov",
  ".avi",
  ".mkv",
  ".webm",
  ".wmv",
  ".flv",
  ".f4v",
  ".mpg",
  ".mpeg",
  ".m2v",
  ".mxf",
  ".mts",
  ".m2ts",
  ".ts",
  ".3gp",
  ".3g2",
  ".ogv",
  ".vob",
  ".asf",
  ".rm",
  ".rmvb",
  ".divx",
];

const getFormatSpecificOptions = (filename) => {
  const ext = extname(filename).toLowerCase();

  switch (ext) {
    case ".flv":
      return ["-fflags", "+genpts", "-avoid_negative_ts", "make_zero"];
    case ".wmv":
    case ".asf":
      return ["-fflags", "+genpts"];
    case ".mts":
    case ".m2ts":
      return [
        "-fflags",
        "+genpts",
        "-analyzeduration",
        "100M",
        "-probesize",
        "100M",
      ];
    case ".vob":
      return ["-fflags", "+genpts", "-analyzeduration", "100M"];
    default:
      return [];
  }
};

// Job stages for tracking progress
const JOB_STAGES = {
  INITIALIZED: "initialized",
  DOWNLOADED: "downloaded",
  ANALYZED: "analyzed",
  THUMBNAILS_GENERATED: "thumbnails_generated",
  TRANSCODED: "transcoded",
  UPLOADED: "uploaded",
  COMPLETED: "completed",
  FAILED: "failed",
};

// Resolution configurations with codec profiles to match AWS ETS
const RESOLUTION_CONFIGS = {
  "1080p": {
    width: 1920,
    height: 1080,
    bitrate: "6593k",
    audioBitrate: "192k",
    profile: "high",
    level: "4.0",
    codecs: "avc1.640028,mp4a.40.5",
  },
  "720p": {
    width: 1280,
    height: 720,
    bitrate: "2766k",
    audioBitrate: "128k",
    profile: "high",
    level: "4.0",
    codecs: "avc1.640028,mp4a.40.5",
  },
  "480p": {
    width: 854,
    height: 480,
    bitrate: "1395k",
    audioBitrate: "128k",
    profile: "main",
    level: "3.1",
    codecs: "avc1.42001f,mp4a.40.5",
  },
  "360p": {
    width: 640,
    height: 360,
    bitrate: "1038k",
    audioBitrate: "96k",
    profile: "main",
    level: "3.1",
    codecs: "avc1.4d001f,mp4a.40.5",
  },
  "240p": {
    width: 426,
    height: 240,
    bitrate: "400k",
    audioBitrate: "64k",
    profile: "baseline",
    level: "3.0",
    codecs: "avc1.42001e,mp4a.40.5",
  },
};

class TranscodingError extends Error {
  constructor(message, stage, originalError = null) {
    super(message);
    this.name = "TranscodingError";
    this.stage = stage;
    this.originalError = originalError;
  }
}

class JobStateManager {
  constructor(jobId, tempDir) {
    this.jobId = jobId;
    this.stateFile = join(tempDir, "job_state.json");
    this.state = this.loadState();
  }

  loadState() {
    try {
      if (existsSync(this.stateFile)) {
        const data = readFileSync(this.stateFile, "utf8");
        return JSON.parse(data);
      }
    } catch (error) {
      console.warn(`⚠️ Could not load job state: ${error.message}`);
    }

    return {
      jobId: this.jobId,
      stage: JOB_STAGES.INITIALIZED,
      completedResolutions: [],
      uploadedFiles: [],
      videoInfo: null,
      validResolutions: [],
      outputVideoName: null,
      thumbnailPaths: [],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
  }

  saveState() {
    try {
      this.state.updatedAt = new Date().toISOString();
      const dir = dirname(this.stateFile);
      if (!existsSync(dir)) {
        mkdirSync(dir, { recursive: true });
      }
      writeFileSync(this.stateFile, JSON.stringify(this.state, null, 2));
    } catch (error) {
      console.warn(`⚠️ Could not save job state: ${error.message}`);
    }
  }

  updateStage(stage, additionalData = {}) {
    this.state.stage = stage;
    Object.assign(this.state, additionalData);
    this.saveState();
  }

  isStageCompleted(stage) {
    const stages = Object.values(JOB_STAGES);
    const currentIndex = stages.indexOf(this.state.stage);
    const targetIndex = stages.indexOf(stage);
    return currentIndex > targetIndex;
  }

  addCompletedResolution(resolution) {
    if (!this.state.completedResolutions.includes(resolution)) {
      this.state.completedResolutions.push(resolution);
      this.saveState();
    }
  }

  addUploadedFile(fileName, fileKey) {
    this.state.uploadedFiles.push({
      fileName,
      fileKey,
      uploadedAt: new Date().toISOString(),
    });
    this.saveState();
  }

  isResolutionCompleted(resolution) {
    return this.state.completedResolutions.includes(resolution);
  }

  isFileUploaded(fileKey) {
    return this.state.uploadedFiles.some((file) => file.fileKey === fileKey);
  }
}

async function transcodingWorker(job) {
  const {
    jobId,
    originalKey,
    resolutions,
    videoName,
    environment,
    callbackUrl,
  } = job.data;

  // Use videoName from API, or fallback to originalKey basename
  const outputVideoName =
    videoName || basename(originalKey, extname(originalKey));

  const tempDir = join(process.env.TEMP_UPLOAD_DIR || "./uploads", jobId);
  const b2Service = getB2Service();

  // Create temporary directory first
  if (!existsSync(tempDir)) {
    mkdirSync(tempDir, { recursive: true });
  }

  const stateManager = new JobStateManager(jobId, tempDir);
  let downloadedFile = null;
  let masterPlaylistPath = null;

  try {
    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      `Starting transcoding job for ${originalKey}`,
      "initialization",
      {
        outputVideoName,
        resolutions,
        tempDir,
        environment: environment || "production",
        callbackUrl: callbackUrl || "default",
      },
    );

    // Validate file format
    const fileExt = extname(originalKey).toLowerCase();
    const isSupported = SUPPORTED_VIDEO_FORMATS.includes(fileExt);

    if (!isSupported) {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.WARN,
        `Unsupported file format: ${fileExt}. Processing may fail.`,
        "validation",
        {
          extension: fileExt,
          supportedFormats: SUPPORTED_VIDEO_FORMATS.join(", "),
        },
      );
    } else {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        `File format ${fileExt} is supported`,
        "validation",
      );
    }

    // Check if job was already completed
    if (stateManager.state.stage === JOB_STAGES.COMPLETED) {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        "Job was already completed successfully",
        "resume",
      );
      job.progress(100);
      return {
        jobId,
        originalKey,
        outputKey: `${outputVideoName}/index.m3u8`,
        videoName: outputVideoName,
        resolutions: stateManager.state.validResolutions,
        resumed: true,
        ...stateManager.state,
      };
    }

    stateManager.updateStage(JOB_STAGES.INITIALIZED, { outputVideoName });

    // Stage 1: Download original file (skip if already downloaded)
    if (!stateManager.isStageCompleted(JOB_STAGES.DOWNLOADED)) {
      job.progress(5);
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        `Downloading original file from OV bucket: ${originalKey}`,
        "download",
      );

      const originalFileName = basename(originalKey);
      downloadedFile = join(tempDir, originalFileName);

      // Check if file already exists from previous run
      if (!existsSync(downloadedFile)) {
        try {
          await b2Service.downloadFile(
            originalKey,
            downloadedFile,
            BUCKET_TYPES.ORIGINAL_VIDEO,
          );
          await JobManager.addJobLog(
            jobId,
            LOG_LEVELS.INFO,
            `Successfully downloaded ${originalKey}`,
            "download",
            {
              localPath: downloadedFile,
              originalFileName,
            },
          );
        } catch (downloadError) {
          await JobManager.addJobLog(
            jobId,
            LOG_LEVELS.ERROR,
            `Failed to download ${originalKey}: ${downloadError.message}`,
            "download",
            {
              error: downloadError.message,
              stack: downloadError.stack,
            },
          );
          throw new TranscodingError(
            `Download failed: ${downloadError.message}`,
            "download",
            downloadError,
          );
        }
      } else {
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Using existing downloaded file: ${downloadedFile}`,
          "download",
        );
      }

      if (!existsSync(downloadedFile)) {
        throw new TranscodingError("Downloaded file not found", "download");
      }

      stateManager.updateStage(JOB_STAGES.DOWNLOADED, { downloadedFile });
    } else {
      downloadedFile = stateManager.state.downloadedFile;
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        "Skipping download stage (already completed)",
        "resume",
      );
    }

    // Stage 2: Get video information (skip if already analyzed)
    if (!stateManager.isStageCompleted(JOB_STAGES.ANALYZED)) {
      job.progress(10);
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        `Analyzing video: ${basename(downloadedFile)}`,
        "analysis",
      );

      try {
        const videoInfo = await getVideoInfo(downloadedFile);
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Video analysis complete`,
          "analysis",
          {
            width: videoInfo.width,
            height: videoInfo.height,
            duration: videoInfo.duration,
            codec: videoInfo.codec,
            bitrate: videoInfo.bitrate,
            size: videoInfo.size,
          },
        );

        // Filter resolutions based on source video
        const validResolutions = filterValidResolutions(resolutions, videoInfo);
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Valid resolutions determined: ${validResolutions.join(", ")}`,
          "validation",
          {
            requestedResolutions: resolutions,
            validResolutions,
            sourceResolution: `${videoInfo.width}x${videoInfo.height}`,
          },
        );

        if (validResolutions.length === 0) {
          await JobManager.addJobLog(
            jobId,
            LOG_LEVELS.ERROR,
            "No valid resolutions for transcoding",
            "validation",
            {
              sourceResolution: `${videoInfo.width}x${videoInfo.height}`,
              requestedResolutions: resolutions,
            },
          );
          throw new TranscodingError(
            "No valid resolutions for transcoding",
            "validation",
          );
        }

        stateManager.updateStage(JOB_STAGES.ANALYZED, {
          videoInfo,
          validResolutions,
        });
      } catch (analysisError) {
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.ERROR,
          `Video analysis failed: ${analysisError.message}`,
          "analysis",
          {
            error: analysisError.message,
            stack: analysisError.stack,
          },
        );
        throw analysisError;
      }
    } else {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        "Skipping analysis stage (already completed)",
        "resume",
      );
    }

    const { videoInfo, validResolutions } = stateManager.state;

    // Stage 3: Generate thumbnails (skip if already generated)
    if (!stateManager.isStageCompleted(JOB_STAGES.THUMBNAILS_GENERATED)) {
      job.progress(12);
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        "Generating thumbnails",
        "thumbnails",
      );

      try {
        const thumbnailPaths = await generateThumbnails(
          downloadedFile,
          tempDir,
          outputVideoName,
        );
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Generated ${thumbnailPaths.length} thumbnails`,
          "thumbnails",
          {
            thumbnailPaths: thumbnailPaths.map((p) => basename(p)),
          },
        );
        stateManager.updateStage(JOB_STAGES.THUMBNAILS_GENERATED, {
          thumbnailPaths,
        });
      } catch (thumbnailError) {
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.WARN,
          `Thumbnail generation failed: ${thumbnailError.message}`,
          "thumbnails",
          {
            error: thumbnailError.message,
          },
        );
        // Continue without thumbnails
        stateManager.updateStage(JOB_STAGES.THUMBNAILS_GENERATED, {
          thumbnailPaths: [],
        });
      }
    } else {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        "Skipping thumbnail generation (already completed)",
        "resume",
      );
    }

    // Stage 4: Transcode to multiple resolutions (resume incomplete resolutions)
    const progressPerResolution = 65 / validResolutions.length;
    let currentProgress = 15;

    const transcodedFiles = [];
    const baseName = basename(originalKey, extname(originalKey));
    const timestamp = new Date().toISOString().slice(0, 10);
    const baseOutputPath = `transcoded/${timestamp}/${baseName}`;

    for (let i = 0; i < validResolutions.length; i++) {
      const resolution = validResolutions[i];

      if (stateManager.isResolutionCompleted(resolution)) {
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Skipping ${resolution} transcoding (already completed)`,
          "transcoding",
        );
        currentProgress += progressPerResolution;
        job.progress(Math.round(currentProgress));
        continue;
      }

      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        `Starting ${resolution} transcoding`,
        "transcoding",
        {
          resolution,
          config: RESOLUTION_CONFIGS[resolution],
        },
      );

      const resolutionDir = join(tempDir, `hls_${resolution}`);
      if (!existsSync(resolutionDir)) {
        mkdirSync(resolutionDir, { recursive: true });
      }

      const playlistPath = join(resolutionDir, "index-.m3u8");

      try {
        // STEP 1: Transcode this resolution
        await transcodeToHLS(
          downloadedFile,
          playlistPath,
          resolution,
          jobId,
          (progress) => {
            const totalProgress =
              currentProgress + (progress * progressPerResolution * 0.5) / 100; // 50% for transcode
            job.progress(Math.round(totalProgress));
          },
        );

        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Transcoding complete for ${resolution}`,
          "transcoding",
          {
            resolution,
            outputPath: playlistPath,
          },
        );

        // STEP 2: Immediately upload all files for this resolution
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Uploading ${resolution} files to B2`,
          "upload",
        );

        // Upload playlist
        const playlistKey = `${outputVideoName}/hls_${resolution}/index-.m3u8`;
        await b2Service.uploadFile(
          playlistPath,
          playlistKey,
          "application/x-mpegURL",
          BUCKET_TYPES.HLS_OUTPUT,
        );
        stateManager.addUploadedFile(basename(playlistPath), playlistKey);
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.DEBUG,
          `Uploaded playlist: ${playlistKey}`,
          "upload",
        );

        // Upload all segment files
        const segmentFiles = readdirSync(resolutionDir).filter((file) =>
          file.endsWith(".ts"),
        );

        let uploadedSegments = 0;
        for (const segmentFile of segmentFiles) {
          const segmentPath = join(resolutionDir, segmentFile);
          const segmentKey = `${outputVideoName}/hls_${resolution}/${segmentFile}`;

          await b2Service.uploadFile(
            segmentPath,
            segmentKey,
            "video/mp2t",
            BUCKET_TYPES.HLS_OUTPUT,
          );
          stateManager.addUploadedFile(segmentFile, segmentKey);
          uploadedSegments++;

          // Update progress for uploads
          const uploadProgress =
            currentProgress +
            progressPerResolution * 0.5 +
            (uploadedSegments / segmentFiles.length) *
              progressPerResolution *
              0.5;
          job.progress(Math.round(uploadProgress));
        }

        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Uploaded ${segmentFiles.length} segments for ${resolution}`,
          "upload",
        );

        // STEP 3: Immediately delete the entire resolution directory
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Deleting local files for ${resolution}`,
          "cleanup",
        );

        const resolutionSize = getDirectorySize(resolutionDir);
        rmSync(resolutionDir, { recursive: true, force: true });

        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Freed ${(resolutionSize / 1024 / 1024).toFixed(2)}MB from ${resolution}`,
          "cleanup",
          {
            resolution,
            sizeBytes: resolutionSize,
            sizeMB: (resolutionSize / 1024 / 1024).toFixed(2),
          },
        );

        console.log(
          `🧹 Deleted ${resolution} files (${(resolutionSize / 1024 / 1024).toFixed(2)}MB freed)`,
        );

        // Mark this resolution as completed
        stateManager.addCompletedResolution(resolution);

        transcodedFiles.push({
          resolution,
          playlistPath: null, // Already deleted
          segmentsDir: null, // Already deleted
        });
      } catch (transcodeError) {
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.ERROR,
          `${resolution} processing failed: ${transcodeError.message}`,
          "transcoding",
          {
            resolution,
            error: transcodeError.message,
            stack: transcodeError.stack,
          },
        );
        throw transcodeError;
      }

      currentProgress += progressPerResolution;
      job.progress(Math.round(currentProgress));
    }

    stateManager.updateStage(JOB_STAGES.TRANSCODED);
    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      `All resolutions transcoded and uploaded successfully`,
      "transcoding",
      {
        completedResolutions: validResolutions,
      },
    );

    // Stage 5: Create master playlist
    job.progress(82);
    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      "Creating master playlist",
      "playlist",
    );

    masterPlaylistPath = join(tempDir, "index.m3u8");
    await createMasterPlaylist(transcodedFiles, masterPlaylistPath, videoInfo);
    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      "Master playlist created successfully",
      "playlist",
      {
        playlistPath: masterPlaylistPath,
        resolutions: validResolutions,
      },
    );

    // Stage 6: Upload master playlist and thumbnails
    job.progress(85);
    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      "Uploading master playlist and thumbnails to HLS bucket",
      "upload",
    );

    // Upload master playlist as index.m3u8
    const masterPlaylistKey = `${outputVideoName}/index.m3u8`;
    await b2Service.uploadFile(
      masterPlaylistPath,
      masterPlaylistKey,
      "application/x-mpegURL",
      BUCKET_TYPES.HLS_OUTPUT,
    );
    stateManager.addUploadedFile(
      basename(masterPlaylistPath),
      masterPlaylistKey,
    );
    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      "Master playlist uploaded",
      "upload",
    );

    // Delete master playlist immediately after upload
    if (existsSync(masterPlaylistPath)) {
      rmSync(masterPlaylistPath, { force: true });
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.DEBUG,
        "Deleted master playlist from local storage",
        "cleanup",
      );
    }

    // Upload and delete thumbnails
    for (const thumbnailPath of stateManager.state.thumbnailPaths) {
      if (existsSync(thumbnailPath)) {
        const thumbnailName = basename(thumbnailPath);
        const thumbnailKey = `${outputVideoName}/${thumbnailName}`;
        const contentType = thumbnailName.endsWith(".jpg")
          ? "image/jpeg"
          : "image/png";

        await b2Service.uploadFile(
          thumbnailPath,
          thumbnailKey,
          contentType,
          BUCKET_TYPES.HLS_OUTPUT,
        );
        stateManager.addUploadedFile(thumbnailName, thumbnailKey);

        // Delete thumbnail immediately after upload
        rmSync(thumbnailPath, { force: true });
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.DEBUG,
          `Uploaded and deleted thumbnail: ${thumbnailName}`,
          "upload",
        );
      }
    }

    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      "All remaining files uploaded and deleted",
      "upload",
    );

    // Delete the original downloaded video file
    if (existsSync(downloadedFile)) {
      const originalSize = statSync(downloadedFile).size;
      rmSync(downloadedFile, { force: true });
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        `Deleted original video file (${(originalSize / 1024 / 1024).toFixed(2)}MB)`,
        "cleanup",
        {
          file: basename(downloadedFile),
          sizeBytes: originalSize,
          sizeMB: (originalSize / 1024 / 1024).toFixed(2),
        },
      );
      console.log(
        `🧹 Deleted original video file (${(originalSize / 1024 / 1024).toFixed(2)}MB freed)`,
      );
    }

    stateManager.updateStage(JOB_STAGES.UPLOADED);

    // Stage 7: Send callback to web app
    job.progress(95);

    try {
      await sendCompletionCallback(
        jobId,
        originalKey,
        masterPlaylistKey,
        videoInfo,
        outputVideoName,
        environment,
        callbackUrl,
      );
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        "Completion callback sent successfully",
        "callback",
      );
    } catch (callbackError) {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.WARN,
        `Callback failed: ${callbackError.message}`,
        "callback",
        {
          error: callbackError.message,
          callbackUrl: callbackUrl || "default",
        },
      );
      // Don't fail the job for callback errors
    }

    // Stage 8: Mark as completed
    const totalSize = stateManager.state.uploadedFiles.reduce(
      (sum, file) => sum + (file.fileSize || 0),
      0,
    );

    stateManager.updateStage(JOB_STAGES.COMPLETED, {
      completedAt: new Date().toISOString(),
      totalSize,
    });

    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.INFO,
      "Transcoding job completed successfully",
      "completion",
      {
        outputKey: masterPlaylistKey,
        totalSize,
        duration: videoInfo.duration,
        resolutions: validResolutions,
        totalFiles: stateManager.state.uploadedFiles.length,
      },
    );

    job.progress(100);

    return {
      jobId,
      originalKey,
      outputKey: masterPlaylistKey,
      videoName: outputVideoName,
      resolutions: validResolutions,
      fileSize: totalSize,
      duration: videoInfo.duration,
      metadata: {
        originalResolution: `${videoInfo.width}x${videoInfo.height}`,
        outputResolutions: validResolutions,
        segmentCount: stateManager.state.uploadedFiles.filter((f) =>
          f.fileKey.endsWith(".ts"),
        ).length,
        totalFiles: stateManager.state.uploadedFiles.length,
        thumbnailCount: stateManager.state.thumbnailPaths.length,
      },
    };
  } catch (error) {
    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.ERROR,
      `Transcoding failed: ${error.message}`,
      "error",
      {
        error: error.message,
        stack: error.stack,
        stage: error.stage || "unknown",
      },
    );

    stateManager.updateStage(JOB_STAGES.FAILED, {
      error: error.message,
      failedAt: new Date().toISOString(),
    });

    // Send failure callback
    try {
      await sendFailureCallback(
        jobId,
        originalKey,
        error.message,
        environment,
        callbackUrl,
      );
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        "Failure callback sent successfully",
        "callback",
      );
    } catch (callbackError) {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.ERROR,
        `Failure callback failed: ${callbackError.message}`,
        "callback",
        {
          error: callbackError.message,
        },
      );
    }

    throw error;
  } finally {
    // ALWAYS cleanup temporary files - critical for storage management
    try {
      if (existsSync(tempDir)) {
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Cleaning up temporary directory: ${tempDir}`,
          "cleanup",
        );

        // Get directory size before cleanup for logging
        const getDirectorySize = (dir) => {
          let size = 0;
          try {
            const files = readdirSync(dir, { withFileTypes: true });
            for (const file of files) {
              const filePath = join(dir, file.name);
              if (file.isDirectory()) {
                size += getDirectorySize(filePath);
              } else {
                const stats = statSync(filePath);
                size += stats.size;
              }
            }
          } catch (err) {
            // Ignore errors during size calculation
          }
          return size;
        };

        const dirSize = getDirectorySize(tempDir);
        const dirSizeMB = (dirSize / 1024 / 1024).toFixed(2);

        rmSync(tempDir, { recursive: true, force: true });

        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `Cleaned up ${dirSizeMB}MB from temporary directory`,
          "cleanup",
          {
            tempDir,
            sizeBytes: dirSize,
            sizeMB: dirSizeMB,
          },
        );

        console.log(
          `🧹 Cleaned up temporary directory: ${tempDir} (${dirSizeMB}MB freed)`,
        );
      }
    } catch (cleanupError) {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.ERROR,
        `Failed to cleanup temporary files: ${cleanupError.message}`,
        "cleanup",
        {
          error: cleanupError.message,
          tempDir,
        },
      );
      console.error(
        `❌ Failed to cleanup temporary files for ${jobId}:`,
        cleanupError.message,
      );
    }
  }
}

// Helper function to calculate directory size
function getDirectorySize(dir) {
  let size = 0;
  try {
    if (!existsSync(dir)) return 0;

    const files = readdirSync(dir, { withFileTypes: true });
    for (const file of files) {
      const filePath = join(dir, file.name);
      try {
        if (file.isDirectory()) {
          size += getDirectorySize(filePath);
        } else {
          const stats = statSync(filePath);
          size += stats.size;
        }
      } catch (err) {
        // Skip files we can't access
      }
    }
  } catch (err) {
    // Directory might not exist
  }
  return size;
}

async function generateThumbnails(inputPath, outputDir, videoName) {
  const thumbnailPaths = [];

  // Generate JPG thumbnail at 1 second
  const jpgPath = join(outputDir, `${videoName}-00001.jpg`);
  if (!existsSync(jpgPath)) {
    await new Promise((resolve, reject) => {
      ffmpeg(inputPath)
        .screenshots({
          timestamps: ["1"],
          filename: `${videoName}-00001.jpg`,
          folder: outputDir,
          size: "320x240",
        })
        .on("end", resolve)
        .on("error", reject);
    });
  }

  if (existsSync(jpgPath)) {
    thumbnailPaths.push(jpgPath);
  }

  // Generate PNG thumbnail at 1 second
  const pngPath = join(outputDir, `${videoName}-00001.png`);
  if (!existsSync(pngPath)) {
    await new Promise((resolve, reject) => {
      ffmpeg(inputPath)
        .screenshots({
          timestamps: ["1"],
          filename: `${videoName}-00001.png`,
          folder: outputDir,
          size: "320x240",
        })
        .on("end", resolve)
        .on("error", reject);
    });
  }

  if (existsSync(pngPath)) {
    thumbnailPaths.push(pngPath);
  }

  return thumbnailPaths;
}

async function getVideoInfo(filePath) {
  return new Promise((resolve, reject) => {
    ffprobe(filePath, (err, metadata) => {
      if (err) {
        reject(
          new TranscodingError(
            `Failed to probe video: ${err.message}`,
            "probe",
            err,
          ),
        );
        return;
      }

      const videoStream = metadata.streams.find(
        (stream) => stream.codec_type === "video",
      );
      if (!videoStream) {
        reject(new TranscodingError("No video stream found", "probe"));
        return;
      }

      resolve({
        duration: parseFloat(metadata.format.duration) || 0,
        width: videoStream.width || 0,
        height: videoStream.height || 0,
        bitrate: parseInt(metadata.format.bit_rate) || 0,
        size: parseInt(metadata.format.size) || 0,
        codec: videoStream.codec_name || "unknown",
      });
    });
  });
}

function filterValidResolutions(requestedResolutions, videoInfo) {
  return requestedResolutions.filter((resolution) => {
    const config = RESOLUTION_CONFIGS[resolution];
    if (!config) {
      return false;
    }

    // Don't upscale - only include resolutions smaller than or equal to source
    if (config.height > videoInfo.height) {
      return false;
    }

    return true;
  });
}

async function transcodeToHLS(
  inputPath,
  outputPath,
  resolution,
  jobId,
  progressCallback,
) {
  const config = RESOLUTION_CONFIGS[resolution];
  const outputDir = dirname(outputPath);

  // Get format-specific input options
  const inputOptions = getFormatSpecificOptions(inputPath);

  return new Promise((resolve, reject) => {
    let command = ffmpeg(inputPath);

    // Apply format-specific input options if any
    if (inputOptions.length > 0) {
      command = command.inputOptions(inputOptions);
    }

    command
      .addOptions([
        "-c:v libx264", // Video codec
        "-c:a aac", // Audio codec
        "-preset fast", // Encoding speed preset
        `-profile:v ${config.profile}`, // H.264 profile
        `-level ${config.level}`, // H.264 level
        "-crf 23", // Quality (lower = better quality)
        `-maxrate ${config.bitrate}`,
        `-bufsize ${parseInt(config.bitrate) * 2}k`,
        `-b:a ${config.audioBitrate}`,
        `-vf scale=${config.width}:${config.height}`, // Scale video
        "-hls_time 10", // 10 second segments (AWS ETS default)
        "-hls_playlist_type vod", // Video on demand
        "-hls_segment_filename",
        join(outputDir, "index-%05d.ts"), // Match AWS ETS naming: index-00000.ts, index-00001.ts, etc.
        "-start_number 0",
        "-hls_base_url",
        "", // Relative URLs in playlist
        "-f hls", // HLS format
      ])
      .output(outputPath);

    command.on("start", async (commandLine) => {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.DEBUG,
        `FFmpeg command started for ${resolution}`,
        "transcoding",
        {
          resolution,
          command: commandLine,
        },
      );
    });

    command.on("progress", async (progress) => {
      if (progressCallback) {
        progressCallback(progress.percent || 0);
      }

      // Log progress every 25%
      const percent = Math.round(progress.percent || 0);
      if (percent % 25 === 0 && percent > 0) {
        await JobManager.addJobLog(
          jobId,
          LOG_LEVELS.INFO,
          `${resolution} transcoding progress: ${percent}%`,
          "transcoding",
          {
            resolution,
            progress: percent,
          },
        );
      }
    });

    command.on("end", async () => {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.INFO,
        `${resolution} transcoding completed`,
        "transcoding",
        {
          resolution,
          outputPath,
        },
      );
      resolve();
    });

    command.on("error", async (err) => {
      await JobManager.addJobLog(
        jobId,
        LOG_LEVELS.ERROR,
        `${resolution} transcoding failed: ${err.message}`,
        "transcoding",
        {
          resolution,
          error: err.message,
          stack: err.stack,
        },
      );
      reject(
        new TranscodingError(
          `Transcoding failed for ${resolution}: ${err.message}`,
          "transcode",
          err,
        ),
      );
    });

    command.run();
  });
}

async function createMasterPlaylist(transcodedFiles, outputPath, videoInfo) {
  const masterPlaylist = ["#EXTM3U"];

  // Sort resolutions by quality (highest first) to match AWS ETS behavior
  const sortedFiles = transcodedFiles.sort((a, b) => {
    const aConfig = RESOLUTION_CONFIGS[a.resolution];
    const bConfig = RESOLUTION_CONFIGS[b.resolution];
    return bConfig.height - aConfig.height;
  });

  sortedFiles.forEach(({ resolution }) => {
    const config = RESOLUTION_CONFIGS[resolution];
    const bandwidth = parseInt(config.bitrate) * 1000;

    masterPlaylist.push(
      `#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=${bandwidth},RESOLUTION=${config.width}x${config.height},CODECS="${config.codecs}"`,
      `hls_${resolution}/index-.m3u8`,
    );
  });

  writeFileSync(outputPath, masterPlaylist.join("\n"));
}

// Helper function to format duration to hh:mm:ss
function formatDuration(seconds) {
  if (!seconds || isNaN(seconds)) return "00:00:00";

  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);

  return [hours, minutes, secs]
    .map((v) => v.toString().padStart(2, "0"))
    .join(":");
}

async function sendCompletionCallback(
  jobId,
  originalKey,
  outputKey,
  videoInfo,
  videoName,
  environment = "production",
  callbackUrl = null,
) {
  // Use custom callback URL if provided, otherwise fall back to environment variable
  const targetUrl = callbackUrl || process.env.WEBAPP_CALLBACK_URL;

  if (!targetUrl) {
    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.WARN,
      "No callback URL configured, skipping callback",
      "callback",
    );
    return;
  }

  const callbackData = {
    jobId,
    originalKey,
    outputKey,
    videoName,
    environment,
    status: "completed",
    timestamp: new Date().toISOString(),
    metadata: {
      duration: videoInfo.duration,
      durationFormatted: formatDuration(videoInfo.duration), // hh:mm:ss format
      originalResolution: `${videoInfo.width}x${videoInfo.height}`,
    },
  };

  const callbackToken =
    process.env.CALLBACK_TOKEN || process.env.WEBAPP_API_KEY || "none";

  await JobManager.addJobLog(
    jobId,
    LOG_LEVELS.DEBUG,
    `Preparing to send callback to ${targetUrl}`,
    "callback",
    {
      url: targetUrl,
      data: callbackData,
      token: callbackToken !== "none" ? callbackToken : "none",
    },
  );

  const response = await axios.post(targetUrl, callbackData, {
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${callbackToken}`,
    },
    timeout: 10000,
  });

  await JobManager.addJobLog(
    jobId,
    LOG_LEVELS.INFO,
    `Callback sent to ${environment} environment`,
    "callback",
    {
      url: targetUrl,
      responseStatus: response.status,
      environment,
      duration: formatDuration(videoInfo.duration),
    },
  );
}

async function sendFailureCallback(
  jobId,
  originalKey,
  errorMessage,
  environment = "production",
  callbackUrl = null,
) {
  // Use custom callback URL if provided, otherwise fall back to environment variable
  const targetUrl = callbackUrl || process.env.WEBAPP_CALLBACK_URL;

  if (!targetUrl) {
    await JobManager.addJobLog(
      jobId,
      LOG_LEVELS.WARN,
      "No callback URL configured, skipping failure callback",
      "callback",
    );
    return;
  }

  const callbackData = {
    jobId,
    originalKey,
    environment,
    status: "failed",
    error: errorMessage,
    timestamp: new Date().toISOString(),
  };

  // Get callback token - same for all environments as requested
  const callbackToken =
    process.env.CALLBACK_TOKEN || process.env.WEBAPP_API_KEY || "none";

  await axios.post(targetUrl, callbackData, {
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${callbackToken}`,
    },
    timeout: 10000,
  });

  await JobManager.addJobLog(
    jobId,
    LOG_LEVELS.INFO,
    `Failure callback sent to ${environment} environment`,
    "callback",
    {
      url: targetUrl,
      environment,
    },
  );
}

// Cleanup function to remove old job directories (call this periodically)
// Modified to be more aggressive for low storage environments
export async function cleanupOldJobs(maxAgeHours = 1) {
  // Changed from 24 to 1 hour
  const uploadsDir = process.env.TEMP_UPLOAD_DIR || "./uploads";

  if (!existsSync(uploadsDir)) {
    return;
  }

  const now = Date.now();
  const maxAge = maxAgeHours * 60 * 60 * 1000; // Convert to milliseconds

  try {
    const dirs = readdirSync(uploadsDir);
    let totalFreed = 0;

    for (const dir of dirs) {
      const dirPath = join(uploadsDir, dir);

      // Skip if not a directory
      try {
        const stats = statSync(dirPath);
        if (!stats.isDirectory()) continue;
      } catch (err) {
        continue;
      }

      const stateFile = join(dirPath, "job_state.json");

      // If no state file, delete immediately (orphaned directory)
      if (!existsSync(stateFile)) {
        const dirSize = getDirectorySize(dirPath);
        rmSync(dirPath, { recursive: true, force: true });
        totalFreed += dirSize;
        console.log(
          `🧹 Cleaned up orphaned directory: ${dir} (${(dirSize / 1024 / 1024).toFixed(2)}MB)`,
        );
        continue;
      }

      try {
        const stateData = JSON.parse(readFileSync(stateFile, "utf8"));
        const updatedAt = new Date(stateData.updatedAt).getTime();

        // Clean up completed jobs older than 1 hour OR failed jobs older than 24 hours
        const shouldCleanup =
          (stateData.stage === JOB_STAGES.COMPLETED &&
            now - updatedAt > maxAge) ||
          (stateData.stage === JOB_STAGES.FAILED &&
            now - updatedAt > 24 * 60 * 60 * 1000);

        if (shouldCleanup) {
          const dirSize = getDirectorySize(dirPath);
          rmSync(dirPath, { recursive: true, force: true });
          totalFreed += dirSize;
          console.log(
            `🧹 Cleaned up old job directory: ${dir} (${stateData.stage}, ${(dirSize / 1024 / 1024).toFixed(2)}MB)`,
          );
        }
      } catch (error) {
        console.warn(
          `⚠️ Could not process job directory ${dir}:`,
          error.message,
        );
      }
    }

    if (totalFreed > 0) {
      console.log(
        `🧹 Total storage freed: ${(totalFreed / 1024 / 1024).toFixed(2)}MB`,
      );
    }
  } catch (error) {
    console.error(`❌ Error during cleanup:`, error.message);
  }
}

export default transcodingWorker;
