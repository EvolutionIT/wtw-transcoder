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
} from "fs";
import { getB2Service, BUCKET_TYPES } from "../services/b2.js";
import axios from "axios";

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
      console.warn(`⚠️Could not load job state: ${error.message}`);
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
      console.warn(`Could not save job state: ${error.message}`);
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
  const { jobId, originalKey, resolutions, videoName } = job.data;

  const outputVideoName =
    videoName || basename(originalKey, extname(originalKey));

  const tempDir = join(process.env.TEMP_UPLOAD_DIR || "./uploads", jobId);
  const b2Service = getB2Service();

  if (!existsSync(tempDir)) {
    mkdirSync(tempDir, { recursive: true });
  }

  const stateManager = new JobStateManager(jobId, tempDir);
  let downloadedFile = null;
  let masterPlaylistPath = null;

  try {
    if (stateManager.state.stage === JOB_STAGES.COMPLETED) {
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

    if (!stateManager.isStageCompleted(JOB_STAGES.DOWNLOADED)) {
      job.progress(5);
      const originalFileName = basename(originalKey);
      downloadedFile = join(tempDir, originalFileName);

      if (!existsSync(downloadedFile)) {
        await b2Service.downloadFile(
          originalKey,
          downloadedFile,
          BUCKET_TYPES.ORIGINAL_VIDEO,
        );
      }

      if (!existsSync(downloadedFile)) {
        throw new TranscodingError("Downloaded file not found", "download");
      }

      stateManager.updateStage(JOB_STAGES.DOWNLOADED, { downloadedFile });
    } else {
      downloadedFile = stateManager.state.downloadedFile;
    }

    // Stage 2: Get video information (skip if already analyzed)
    if (!stateManager.isStageCompleted(JOB_STAGES.ANALYZED)) {
      job.progress(10);

      const videoInfo = await getVideoInfo(downloadedFile);

      const validResolutions = filterValidResolutions(resolutions, videoInfo);

      if (validResolutions.length === 0) {
        throw new TranscodingError(
          "No valid resolutions for transcoding",
          "validation",
        );
      }

      stateManager.updateStage(JOB_STAGES.ANALYZED, {
        videoInfo,
        validResolutions,
      });
    }

    const { videoInfo, validResolutions } = stateManager.state;

    if (!stateManager.isStageCompleted(JOB_STAGES.THUMBNAILS_GENERATED)) {
      job.progress(12);

      const thumbnailPaths = await generateThumbnails(
        downloadedFile,
        tempDir,
        outputVideoName,
      );
      stateManager.updateStage(JOB_STAGES.THUMBNAILS_GENERATED, {
        thumbnailPaths,
      });
    }

    const progressPerResolution = 65 / validResolutions.length;
    let currentProgress = 15;

    const transcodedFiles = [];

    for (let i = 0; i < validResolutions.length; i++) {
      const resolution = validResolutions[i];

      if (stateManager.isResolutionCompleted(resolution)) {
        currentProgress += progressPerResolution;
        job.progress(Math.round(currentProgress));

        const resolutionDir = join(tempDir, `hls_${resolution}`);
        const playlistPath = join(resolutionDir, "index-.m3u8");
        transcodedFiles.push({
          resolution,
          playlistPath,
          segmentsDir: resolutionDir,
        });
        continue;
      }

      const resolutionDir = join(tempDir, `hls_${resolution}`);
      if (!existsSync(resolutionDir)) {
        mkdirSync(resolutionDir, { recursive: true });
      }

      const playlistPath = join(resolutionDir, "index-.m3u8");

      await transcodeToHLS(
        downloadedFile,
        playlistPath,
        resolution,
        (progress) => {
          const totalProgress =
            currentProgress + (progress * progressPerResolution) / 100;
          job.progress(Math.round(totalProgress));
        },
      );

      transcodedFiles.push({
        resolution,
        playlistPath,
        segmentsDir: resolutionDir,
      });

      stateManager.addCompletedResolution(resolution);

      currentProgress += progressPerResolution;
      job.progress(Math.round(currentProgress));
    }

    stateManager.updateStage(JOB_STAGES.TRANSCODED);

    job.progress(82);

    masterPlaylistPath = join(tempDir, "index.m3u8");
    if (!existsSync(masterPlaylistPath)) {
      await createMasterPlaylist(
        transcodedFiles,
        masterPlaylistPath,
        videoInfo,
      );
    }

    if (!stateManager.isStageCompleted(JOB_STAGES.UPLOADED)) {
      job.progress(85);

      const uploadPromises = [];

      const masterPlaylistKey = `${outputVideoName}/index.m3u8`;
      if (!stateManager.isFileUploaded(masterPlaylistKey)) {
        uploadPromises.push(
          uploadFileWithTracking(
            b2Service,
            masterPlaylistPath,
            masterPlaylistKey,
            "application/x-mpegURL",
            BUCKET_TYPES.HLS_OUTPUT,
            stateManager,
          ),
        );
      }

      for (const thumbnailPath of stateManager.state.thumbnailPaths) {
        const thumbnailName = basename(thumbnailPath);
        const thumbnailKey = `${outputVideoName}/${thumbnailName}`;
        const contentType = thumbnailName.endsWith(".jpg")
          ? "image/jpeg"
          : "image/png";

        if (
          !stateManager.isFileUploaded(thumbnailKey) &&
          existsSync(thumbnailPath)
        ) {
          uploadPromises.push(
            uploadFileWithTracking(
              b2Service,
              thumbnailPath,
              thumbnailKey,
              contentType,
              BUCKET_TYPES.HLS_OUTPUT,
              stateManager,
            ),
          );
        }
      }

      for (const transcodedFile of transcodedFiles) {
        const { resolution, playlistPath, segmentsDir } = transcodedFile;

        const playlistKey = `${outputVideoName}/hls_${resolution}/index-.m3u8`;
        if (
          !stateManager.isFileUploaded(playlistKey) &&
          existsSync(playlistPath)
        ) {
          uploadPromises.push(
            uploadFileWithTracking(
              b2Service,
              playlistPath,
              playlistKey,
              "application/x-mpegURL",
              BUCKET_TYPES.HLS_OUTPUT,
              stateManager,
            ),
          );
        }

        if (existsSync(segmentsDir)) {
          const segmentFiles = readdirSync(segmentsDir).filter((file) =>
            file.endsWith(".ts"),
          );

          for (const segmentFile of segmentFiles) {
            const segmentPath = join(segmentsDir, segmentFile);
            const segmentKey = `${outputVideoName}/hls_${resolution}/${segmentFile}`;

            if (
              !stateManager.isFileUploaded(segmentKey) &&
              existsSync(segmentPath)
            ) {
              uploadPromises.push(
                uploadFileWithTracking(
                  b2Service,
                  segmentPath,
                  segmentKey,
                  "video/mp2t",
                  BUCKET_TYPES.HLS_OUTPUT,
                  stateManager,
                ),
              );
            }
          }
        }
      }

      if (uploadPromises.length > 0) {
        await Promise.all(uploadPromises);
      }

      stateManager.updateStage(JOB_STAGES.UPLOADED);
    }

    job.progress(95);
    const masterPlaylistKey = `${outputVideoName}/index.m3u8`;
    await sendCompletionCallback(
      jobId,
      originalKey,
      masterPlaylistKey,
      videoInfo,
      outputVideoName,
    );

    const totalSize = stateManager.state.uploadedFiles.reduce(
      (sum, file) => sum + (file.fileSize || 0),
      0,
    );

    stateManager.updateStage(JOB_STAGES.COMPLETED, {
      completedAt: new Date().toISOString(),
      totalSize,
    });

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
    stateManager.updateStage(JOB_STAGES.FAILED, {
      error: error.message,
      failedAt: new Date().toISOString(),
    });

    try {
      await sendFailureCallback(jobId, originalKey, error.message);
    } catch (callbackError) {
      console.error("Failed to send failure callback:", callbackError.message);
    }

    throw error;
  } finally {
    // Note: Don't cleanup temporary files immediately to allow for resume
    // You can add a cleanup job that runs periodically to remove old completed jobs
    console.log(`Keeping temporary files for potential resume: ${tempDir}`);
  }
}

async function uploadFileWithTracking(
  b2Service,
  localPath,
  remoteKey,
  contentType,
  bucketType,
  stateManager,
) {
  const result = await b2Service.uploadFile(
    localPath,
    remoteKey,
    contentType,
    bucketType,
  );
  stateManager.addUploadedFile(basename(localPath), remoteKey);
  return result;
}

async function generateThumbnails(inputPath, outputDir, videoName) {
  const thumbnailPaths = [];

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
  progressCallback,
) {
  const config = RESOLUTION_CONFIGS[resolution];
  const outputDir = dirname(outputPath);

  return new Promise((resolve, reject) => {
    const command = ffmpeg(inputPath)
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

    command.on("start", (commandLine) => {
      console.log(`FFmpeg command: ${commandLine}`);
    });

    command.on("progress", (progress) => {
      if (progressCallback) {
        progressCallback(progress.percent || 0);
      }
    });

    command.on("end", () => {
      resolve();
    });

    command.on("error", (err) => {
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

  // Sort resolutions highest first to match AWS ETS behavior
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

async function sendCompletionCallback(
  jobId,
  originalKey,
  outputKey,
  videoInfo,
  videoName,
) {
  if (!process.env.WEBAPP_CALLBACK_URL) {
    return;
  }

  try {
    const callbackData = {
      jobId,
      originalKey,
      outputKey,
      videoName,
      status: "completed",
      timestamp: new Date().toISOString(),
      metadata: {
        duration: videoInfo.duration,
        originalResolution: `${videoInfo.width}x${videoInfo.height}`,
      },
    };

    const response = await axios.post(
      process.env.WEBAPP_CALLBACK_URL,
      callbackData,
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${process.env.WEBAPP_API_KEY || "none"}`,
        },
        timeout: 10000,
      },
    );
  } catch (error) {
    throw error;
  }
}

async function sendFailureCallback(jobId, originalKey, errorMessage) {
  if (!process.env.WEBAPP_CALLBACK_URL) {
    return;
  }

  try {
    const callbackData = {
      jobId,
      originalKey,
      status: "failed",
      error: errorMessage,
      timestamp: new Date().toISOString(),
    };

    await axios.post(process.env.WEBAPP_CALLBACK_URL, callbackData, {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.WEBAPP_API_KEY || "none"}`,
      },
      timeout: 10000,
    });
  } catch (error) {
    console.error("Failed to send failure callback:", error.message);
  }
}

export async function cleanupOldJobs(maxAgeHours = 24) {
  const uploadsDir = process.env.TEMP_UPLOAD_DIR || "./uploads";

  if (!existsSync(uploadsDir)) {
    return;
  }

  const now = Date.now();
  const maxAge = maxAgeHours * 60 * 60 * 1000;

  try {
    const dirs = readdirSync(uploadsDir);

    for (const dir of dirs) {
      const dirPath = join(uploadsDir, dir);
      const stateFile = join(dirPath, "job_state.json");

      if (existsSync(stateFile)) {
        try {
          const stateData = JSON.parse(readFileSync(stateFile, "utf8"));
          const updatedAt = new Date(stateData.updatedAt).getTime();

          // Only cleanup completed or failed jobs older than maxAge
          if (
            (stateData.stage === JOB_STAGES.COMPLETED ||
              stateData.stage === JOB_STAGES.FAILED) &&
            now - updatedAt > maxAge
          ) {
            rmSync(dirPath, { recursive: true, force: true });
          }
        } catch (error) {
          console.warn(
            `Could not process job directory ${dir}:`,
            error.message,
          );
        }
      }
    }
  } catch (error) {
    console.error(`Error during cleanup:`, error.message);
  }
}

export default transcodingWorker;
