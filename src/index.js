const { exec } = require('child_process');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');
const mongoose = require('mongoose');  // Import mongoose
require('dotenv').config();

const inputS3Url = process.env.INPUT_S3_URL;
const outputBucket = process.env.OUTPUT_BUCKET_NAME;
const videoFileKey = process.env.VIDEO_FILE_KEY;
const localInputPath = `/tmp/${videoFileKey}`;  
const localOutputPath = `/tmp/processed_${videoFileKey}`;  


const mongoUri = process.env.MONGO_URI; 
const Video = require('./models/Video'); 

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
}});

async function downloadVideo() {
  const bucketName = inputS3Url.split('/')[2]; 
  const objectKey = inputS3Url.split('/').slice(3).join('/'); 

  const command = new GetObjectCommand({
    Bucket: bucketName,  
    Key: objectKey,      
  });

  const { Body } = await s3Client.send(command);
  await pipeline(Body, fs.createWriteStream(localInputPath));
  console.log(`Downloaded video to ${localInputPath}`);
}


function processVideo() {
  return new Promise((resolve, reject) => {
    const command = `ffmpeg -i ${localInputPath} \
  -filter_complex \
  "[0:v]split=4[v1][v2][v3][v4]; \
  [v1]scale=-2:360[v360]; \
  [v2]scale=-2:480[v480]; \
  [v3]scale=-2:720[v720]; \
  [v4]scale=-2:1080[v1080]" \
  -map "[v360]" -c:v:0 libx264 -b:v:0 800k -map a:0 -c:a:0 aac -b:a:0 96k \
  -map "[v480]" -c:v:1 libx264 -b:v:1 1400k -map a:0 -c:a:1 aac -b:a:1 128k \
  -map "[v720]" -c:v:2 libx264 -b:v:2 2800k -map a:0 -c:a:2 aac -b:a:2 128k \
  -map "[v1080]" -c:v:3 libx264 -b:v:3 5000k -map a:0 -c:a:3 aac -b:a:3 192k \
  -var_stream_map "v:0,a:0 v:1,a:1 v:2,a:2 v:3,a:3" \
  -master_pl_name master.m3u8 \
  -f hls -hls_time 6 -hls_list_size 0 -hls_segment_filename "${localOutputPath}/%v/segment%d.ts" \
  "${localOutputPath}/%v/playlist.m3u8"`;

    exec(command, (error, stdout, stderr) => {
      if (error) {
        console.error(`Error processing video: ${stderr}`);
        reject(error);
      } else {
        console.log(`Video processed: ${stdout}`);
        resolve();
      }
    });
  });
}

async function uploadProcessedVideo() {
  const walkSync = (dir, filelist = []) => {
    fs.readdirSync(dir).forEach(file => {
      const dirFile = path.join(dir, file);
      try {
        filelist = fs.statSync(dirFile).isDirectory()
          ? walkSync(dirFile, filelist)
          : filelist.concat(dirFile);
      } catch (err) {
        console.error('Error accessing file:', dirFile, err);
      }
    });
    return filelist;
  };
  
  const files = walkSync(localOutputPath);
  
  for (const filePath of files) {
    const key = `processed/${videoFileKey}/${path.relative(localOutputPath, filePath)}`;
    
    const command = new PutObjectCommand({
      Bucket: outputBucket,
      Key: key,
      Body: fs.createReadStream(filePath),
      ContentType: filePath.endsWith('.m3u8') ? 'application/x-mpegURL' : 'video/MP2T',
    });

    await s3Client.send(command);
  }

  console.log(`Uploaded processed video files to S3: ${outputBucket}/processed/${videoFileKey}/`);
}

async function updateVideoInMongoDB(videoFileKey) {
  try {
    const masterPlaylistUrl = `https://${outputBucket}.s3.${process.env.AWS_REGION}.amazonaws.com/processed/${videoFileKey}/master.m3u8`;

    const updatedVideo = await Video.findByIdAndUpdate(
      videoFileKey,
      { url: masterPlaylistUrl },
      { new: true }
    );

    console.log(`Video record updated in MongoDB: ${updatedVideo}`);
  } catch (error) {
    console.error('Error updating video record in MongoDB:', error);
  }
}


async function main() {
  let mongoConnection;
  try {
    mongoConnection = await mongoose.connect(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });
    logger('MongoDB connected');

    await downloadVideo();
    await processVideo();
    await uploadProcessedVideo();
    await updateVideoInMongoDB(videoFileKey);
  
    logger("Video processing completed successfully.");
  } catch (error) {
    logger(`Video processing failed: ${error.message}`, 'error');
    process.exit(1);
  } finally {
    try {
      if (fs.existsSync(localInputPath)) {
        fs.unlinkSync(localInputPath);
      }
      if (fs.existsSync(localOutputPath)) {
        fs.rmSync(localOutputPath, { recursive: true, force: true });
      }
    } catch (cleanupError) {
      logger(`Error during cleanup: ${cleanupError.message}`, 'error');
    }
    if (mongoConnection) {
      await mongoConnection.disconnect();
    }
  }
}

// Ensure unhandled errors are logged
process.on('uncaughtException', (error) => {
  logger(`Uncaught Exception: ${error.message}`, 'error');
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logger(`Unhandled Rejection at: ${promise}, reason: ${reason}`, 'error');
  process.exit(1);
});

main().catch(error => {
  logger(`Fatal error: ${error.message}`, 'error');
  process.exit(1);
});

main();
