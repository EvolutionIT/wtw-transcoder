import B2 from "backblaze-b2";
import { createWriteStream, readFileSync, statSync } from "fs";
import { basename, extname } from "path";

const BUCKET_TYPES = {
  ORIGINAL_VIDEO: "ov",
  HLS_OUTPUT: "hls",
};

class B2Service {
  constructor() {
    this.b2 = new B2({
      applicationKeyId: process.env.B2_APPLICATION_KEY_ID,
      applicationKey: process.env.B2_APPLICATION_KEY,
    });
    this.authorized = false;
    this.authPromise = null;

    this.buckets = {
      [BUCKET_TYPES.ORIGINAL_VIDEO]: {
        id: process.env.B2_OV_BUCKET_ID,
        name: process.env.B2_OV_BUCKET_NAME,
      },
      [BUCKET_TYPES.HLS_OUTPUT]: {
        id: process.env.B2_HLS_BUCKET_ID,
        name: process.env.B2_HLS_BUCKET_NAME,
      },
    };

    this.validateBucketConfig();
  }

  validateBucketConfig() {
    const requiredEnvVars = [
      "B2_APPLICATION_KEY_ID",
      "B2_APPLICATION_KEY",
      "B2_OV_BUCKET_ID",
      "B2_OV_BUCKET_NAME",
      "B2_HLS_BUCKET_ID",
      "B2_HLS_BUCKET_NAME",
    ];

    const missing = requiredEnvVars.filter((varName) => !process.env[varName]);

    if (missing.length > 0) {
      throw new Error(
        `Missing required environment variables: ${missing.join(", ")}`,
      );
    }
  }

  getBucketConfig(bucketType) {
    const bucket = this.buckets[bucketType];
    if (!bucket) {
      throw new Error(
        `Invalid bucket type: ${bucketType}. Use BUCKET_TYPES.ORIGINAL_VIDEO or BUCKET_TYPES.HLS_OUTPUT`,
      );
    }
    return bucket;
  }

  async authorize() {
    if (this.authorized) return;

    if (this.authPromise) {
      return await this.authPromise;
    }

    this.authPromise = this._doAuthorize();
    return await this.authPromise;
  }

  async _doAuthorize() {
    try {
      await this.b2.authorize();
      this.authorized = true;
      console.log("BackBlaze B2 authorized successfully");
    } catch (error) {
      console.error("B2 authorization failed:", error.message);
      this.authorized = false;
      throw new Error(`B2 authorization failed: ${error.message}`);
    } finally {
      this.authPromise = null;
    }
  }

  async downloadFile(
    fileName,
    localPath,
    bucketType = BUCKET_TYPES.ORIGINAL_VIDEO,
  ) {
    try {
      await this.authorize();

      const bucket = this.getBucketConfig(bucketType);
      const downloadAuth = await this.b2.getDownloadAuthorization({
        bucketId: bucket.id,
        fileNamePrefix: fileName,
        validDurationInSeconds: 3600, // 1 hour
      });
      const response = await this.b2.downloadFileByName({
        bucketName: bucket.name,
        fileName: fileName,
        responseType: "stream",
      });
      const writeStream = createWriteStream(localPath);

      return new Promise((resolve, reject) => {
        response.data.pipe(writeStream);

        writeStream.on("finish", () => {
          console.log(`Downloaded ${fileName} to ${localPath}`);
          resolve(localPath);
        });

        writeStream.on("error", (error) => {
          console.error(`Download failed for ${fileName}:`, error);
          reject(error);
        });

        response.data.on("error", (error) => {
          console.error(`Stream error for ${fileName}:`, error);
          reject(error);
        });
      });
    } catch (error) {
      console.error(`Failed to download ${fileName}:`, error.message);
      throw error;
    }
  }

  async uploadFile(
    localFilePath,
    remoteFileName,
    contentType = "application/octet-stream",
    bucketType = BUCKET_TYPES.HLS_OUTPUT,
  ) {
    try {
      await this.authorize();

      const bucket = this.getBucketConfig(bucketType);
      console.log(
        `Uploading ${localFilePath} to B2 bucket ${bucket.name} as ${remoteFileName}...`,
      );

      const uploadUrl = await this.b2.getUploadUrl({
        bucketId: bucket.id,
      });

      const fileData = readFileSync(localFilePath);
      const fileSize = statSync(localFilePath).size;

      const response = await this.b2.uploadFile({
        uploadUrl: uploadUrl.data.uploadUrl,
        uploadAuthToken: uploadUrl.data.authorizationToken,
        fileName: remoteFileName,
        data: fileData,
        hash: null,
        info: {
          src_last_modified_millis: Date.now().toString(),
        },
        contentType: contentType,
      });

      console.log(
        `Uploaded ${remoteFileName} (${this.formatFileSize(fileSize)}) to ${bucket.name}`,
      );

      return {
        fileId: response.data.fileId,
        fileName: response.data.fileName,
        fileSize: fileSize,
        contentType: response.data.contentType,
        uploadTimestamp: response.data.uploadTimestamp,
        bucketType: bucketType,
        bucketName: bucket.name,
      };
    } catch (error) {
      console.error(`Failed to upload ${localFilePath}:`, error.message);
      throw error;
    }
  }

  async uploadMultipleFiles(files, bucketType = BUCKET_TYPES.HLS_OUTPUT) {
    const results = [];

    for (const file of files) {
      try {
        const result = await this.uploadFile(
          file.localPath,
          file.remoteName,
          file.contentType,
          bucketType,
        );
        results.push({
          ...result,
          localPath: file.localPath,
          success: true,
        });
      } catch (error) {
        console.error(`Failed to upload ${file.localPath}:`, error.message);
        results.push({
          localPath: file.localPath,
          remoteName: file.remoteName,
          success: false,
          error: error.message,
        });
      }
    }

    return results;
  }

  async deleteFile(fileName, bucketType = BUCKET_TYPES.HLS_OUTPUT) {
    try {
      await this.authorize();

      const bucket = this.getBucketConfig(bucketType);
      console.log(`Deleting ${fileName} from B2 bucket ${bucket.name}...`);

      const fileInfo = await this.b2.listFileNames({
        bucketId: bucket.id,
        startFileName: fileName,
        maxFileCount: 1,
      });

      if (
        fileInfo.data.files.length === 0 ||
        fileInfo.data.files[0].fileName !== fileName
      ) {
        console.warn(`File ${fileName} not found in B2 bucket ${bucket.name}`);
        return false;
      }

      const file = fileInfo.data.files[0];

      await this.b2.deleteFileVersion({
        fileId: file.fileId,
        fileName: file.fileName,
      });

      console.log(`Deleted ${fileName} from B2 bucket ${bucket.name}`);
      return true;
    } catch (error) {
      console.error(`Failed to delete ${fileName}:`, error.message);
      throw error;
    }
  }

  async listFiles(
    prefix = "",
    maxCount = 100,
    bucketType = BUCKET_TYPES.HLS_OUTPUT,
  ) {
    try {
      await this.authorize();

      const bucket = this.getBucketConfig(bucketType);

      const response = await this.b2.listFileNames({
        bucketId: bucket.id,
        startFileName: prefix,
        maxFileCount: maxCount,
      });

      return response.data.files.map((file) => ({
        fileName: file.fileName,
        fileId: file.fileId,
        size: file.size,
        uploadTimestamp: file.uploadTimestamp,
        contentType: file.contentType,
        bucketType: bucketType,
        bucketName: bucket.name,
      }));
    } catch (error) {
      console.error("Failed to list files:", error.message);
      throw error;
    }
  }

  async getFileInfo(fileName, bucketType = BUCKET_TYPES.ORIGINAL_VIDEO) {
    try {
      await this.authorize();

      const bucket = this.getBucketConfig(bucketType);

      const response = await this.b2.listFileNames({
        bucketId: bucket.id,
        startFileName: fileName,
        maxFileCount: 1,
      });

      if (
        response.data.files.length === 0 ||
        response.data.files[0].fileName !== fileName
      ) {
        return null;
      }

      const file = response.data.files[0];
      return {
        fileName: file.fileName,
        fileId: file.fileId,
        size: file.size,
        uploadTimestamp: file.uploadTimestamp,
        contentType: file.contentType,
        formattedSize: this.formatFileSize(file.size),
        bucketType: bucketType,
        bucketName: bucket.name,
      };
    } catch (error) {
      console.error(`Failed to get file info for ${fileName}:`, error.message);
      return null;
    }
  }

  getPublicUrl(fileName, bucketType = BUCKET_TYPES.HLS_OUTPUT) {
    const bucket = this.getBucketConfig(bucketType);
    return `https://f002.backblazeb2.com/file/${bucket.name}/${fileName}`;
  }

  formatFileSize(bytes) {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
  }

  generateOutputPath(originalKey, resolution, segmentNumber = null) {
    const baseName = basename(originalKey, extname(originalKey));
    const timestamp = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

    if (segmentNumber !== null) {
      return `transcoded/${timestamp}/${baseName}/${resolution}/segment_${segmentNumber}.ts`;
    } else {
      return `transcoded/${timestamp}/${baseName}/${resolution}/playlist.m3u8`;
    }
  }

  generateMasterPlaylistPath(originalKey) {
    const baseName = basename(originalKey, extname(originalKey));
    const timestamp = new Date().toISOString().slice(0, 10);
    return `transcoded/${timestamp}/${baseName}/master.m3u8`;
  }
}

let b2Service = null;

function getB2Service() {
  if (!b2Service) {
    b2Service = new B2Service();
  }
  return b2Service;
}

export { B2Service, getB2Service, BUCKET_TYPES };
