import dotenv from "dotenv";
import fs from "node:fs";
import path from "node:path";
import mime from "mime-types";
import pLimit from "p-limit";
import {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
  CloudFrontClient,
  CreateInvalidationCommand,
} from "@aws-sdk/client-cloudfront";

dotenv.config();

const isProduction =
  process.env.NODE_ENV === "production" || process.env.ENV === "production";

if (!isProduction) {
  console.log(
    "Skipped post-build process. Set NODE_ENV=production to run this script."
  );
  process.exit(0);
}

const {
  promises: { readdir, stat: getStats },
} = fs;
const { resolve, join, extname } = path;

const AWS_CONFIG = {
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
};

const s3Client = new S3Client(AWS_CONFIG);
const cloudfrontClient = new CloudFrontClient(AWS_CONFIG);

const UPLOAD_CONCURRENCY_LIMIT =
  Number(process.env.UPLOAD_CONCURRENCY_LIMIT) || 5;
const limit = pLimit(UPLOAD_CONCURRENCY_LIMIT);
const BUCKET_NAME = process.env.AWS_BUCKET_NAME;
const OUTPUT_DIR = process.env.ASTRO_OUTPUT_DIR || "./dist";
const DISTRIBUTION_ID = process.env.AWS_DISTRIBUTION_ID;

if (!BUCKET_NAME) {
  console.error("❌ Missing AWS_BUCKET_NAME in environment.");
  process.exit(1);
}

const LONG_CACHE_CONTROL = "public, max-age=31556926, immutable";
const SHORT_CACHE_CONTROL = "public, max-age=0, must-revalidate";
const NO_CACHE_EXTENSIONS = new Set([
  ".html",
  ".xml",
  ".txt",
  ".json",
  ".webmanifest",
]);

// Function to normalize paths (Fix Windows `\` issue)
const normalizeS3Key = (key) => key.replace(/\\/g, "/");
const getMimeType = (filePath) =>
  mime.lookup(filePath) || "application/octet-stream";

// List all files in S3 bucket
const listFiles = async () => {
  try {
    let continuationToken;
    const files = [];

    do {
      const { Contents, IsTruncated, NextContinuationToken } =
        await s3Client.send(
          new ListObjectsV2Command({
            Bucket: BUCKET_NAME,
            ContinuationToken: continuationToken,
          })
        );

      if (Contents) {
        files.push(...Contents.map((file) => ({ Key: file.Key })));
      }

      continuationToken = IsTruncated ? NextContinuationToken : undefined;
    } while (continuationToken);

    return files;
  } catch (error) {
    console.error("❌ Failed to list S3 files:", error);
    return [];
  }
};

// Delete all old files from S3 bucket
const deleteFiles = async () => {
  const filesToDelete = await listFiles();
  if (filesToDelete.length === 0) {
    console.log("✅ No old files to delete in S3.");
    return;
  }

  try {
    await s3Client.send(
      new DeleteObjectsCommand({
        Bucket: BUCKET_NAME,
        Delete: { Objects: filesToDelete },
      })
    );
    console.log(`🗑️ Deleted ${filesToDelete.length} old files from ${BUCKET_NAME}`);
  } catch (error) {
    console.error("❌ Failed to delete old files:", error);
  }
};

let errors = [];
// Upload a single file to S3
const uploadFile = async (filePath, key) => {
  try {
    const normalizedKey = normalizeS3Key(key);
    const extension = extname(normalizedKey).toLowerCase();
    const cacheControl = NO_CACHE_EXTENSIONS.has(extension)
      ? SHORT_CACHE_CONTROL
      : LONG_CACHE_CONTROL;
    const fileStream = fs.createReadStream(filePath);
    const params = {
      Bucket: BUCKET_NAME,
      Key: normalizedKey,
      Body: fileStream,
      ContentType: getMimeType(filePath),
      CacheControl: cacheControl,
    };

    await s3Client.send(new PutObjectCommand(params));
    console.log(`✅ Uploaded: ${normalizedKey}`);
    return normalizedKey; // Return key for CloudFront invalidation tracking
  } catch (err) {
    console.error(`❌ Upload failed for ${filePath}:`, err.message);
    errors.push({ filePath, key });
    return null;
  }
};

// Upload a directory recursively with proper waiting
const uploadDirectory = async (directoryPath, rootKey = "") => {
  try {
    const dirPath = resolve(directoryPath);
    const dirStats = await getStats(dirPath);
    if (!dirStats.isDirectory())
      throw new Error(`${dirPath} is not a directory`);

    console.info(`📂 Uploading directory: ${dirPath}...`);
    const filenames = await readdir(dirPath);

    // Collect and await all uploads
    const uploadedFiles = await Promise.all(
      filenames.map(async (filename) => {
        const filePath = join(dirPath, filename);
        const fileStats = await getStats(filePath);
        const key = normalizeS3Key(join(rootKey, filename)); // Preserve full path

        if (fileStats.isFile()) {
          return await limit(() => uploadFile(filePath, key));
        }
        if (fileStats.isDirectory()) {
          // 🔥 Fix: Ensure recursive calls return all uploaded files
          return await uploadDirectory(filePath, key);
        }
        return null;
      })
    );

    return uploadedFiles.flat().filter(Boolean); // Ensure all uploads finish
  } catch (error) {
    console.error(
      `❌ Error uploading directory ${directoryPath}:`,
      error.message
    );
    return [];
  }
};

// Optimize CloudFront invalidation
const invalidateCloudFrontCache = async () => {
  if (!DISTRIBUTION_ID) {
    console.warn("⚠️ Missing AWS_DISTRIBUTION_ID; skipping invalidation.");
    return;
  }

  const invalidationPaths = ["/*"];

  try {
    await cloudfrontClient.send(
      new CreateInvalidationCommand({
        DistributionId: DISTRIBUTION_ID,
        InvalidationBatch: {
          CallerReference: `${Date.now()}`,
          Paths: {
            Quantity: invalidationPaths.length,
            Items: invalidationPaths,
          },
        },
      })
    );
    console.log(
      `🚀 CloudFront invalidation requested for ${invalidationPaths.length} files.`
    );
  } catch (error) {
    console.error("❌ CloudFront invalidation failed:", error);
  }
};

const retryUploads = async () => {
  console.log("Retrying failed uploads...");
  const failedUploads = [...errors];
  errors = [];
  const retryUploads = await Promise.all(
    failedUploads.map(({ filePath, key }) =>
      limit(() => uploadFile(filePath, key))
    )
  );

  const retryErrors = retryUploads.filter((file) => !file);
  if (retryErrors.length > 0) {
    console.error("❌ Failed to upload files:", retryErrors);
    errors = [...retryErrors];
  } else {
    console.log("✅ Retry uploads completed successfully!");
  }
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Main Deployment Function
(async () => {
  try {
    console.time("S3 Cleanup");
    await deleteFiles();
    console.timeEnd("S3 Cleanup");

    console.time("S3 Upload");

    await uploadDirectory(OUTPUT_DIR);

    while (errors.length > 0) {
      await sleep(1000);
      await retryUploads();
    }

    console.timeEnd("S3 Upload");

    console.time("CloudFront Invalidation");
    await invalidateCloudFrontCache();
    console.timeEnd("CloudFront Invalidation");

    console.log("🚀 Deployment completed successfully!");
    process.exit(0); // Explicitly exit after all uploads are complete
  } catch (error) {
    console.error("❌ Deployment failed:", error);
    process.exit(1);
  }
})();
