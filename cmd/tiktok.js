const axios = require("axios");
const fs = require("fs");
const path = require("path");

const DOWNLOAD_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
  Accept: "video/mp4,video/*;q=0.9,application/octet-stream;q=0.8,*/*;q=0.5",
  Referer: "https://www.tiktok.com/",
  Origin: "https://www.tiktok.com",
  "Accept-Language": "en-US,en;q=0.9",
};

const MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024;

function safeUnlink(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  } catch (error) {
    console.error(`[TIKTOK] Failed to remove ${filePath}:`, error.message);
  }
}

function getTikTokCandidateUrls(videoData) {
  return [
    videoData.video_hd,
    videoData.video,
    videoData.play,
    videoData.play_url,
    videoData.download_url,
    videoData.no_watermark,
    videoData.nowm,
    videoData.wmplay,
  ].filter((value, index, values) => typeof value === "string" && value.trim() && values.indexOf(value) === index);
}

async function downloadTikTokVideo(videoData, filePath) {
  const candidateUrls = getTikTokCandidateUrls(videoData);

  if (!candidateUrls.length) {
    throw new Error("TikTok API did not return a downloadable video URL.");
  }

  let lastError = null;

  for (let index = 0; index < candidateUrls.length; index += 1) {
    const candidateUrl = candidateUrls[index];

    try {
      console.log(`[TIKTOK] Trying download URL ${index + 1}/${candidateUrls.length}: ${candidateUrl}`);

      const response = await axios({
        url: candidateUrl,
        method: "GET",
        responseType: "stream",
        timeout: 120000,
        maxRedirects: 5,
        headers: DOWNLOAD_HEADERS,
        validateStatus: (status) => status >= 200 && status < 400,
      });

      const contentType = response.headers["content-type"] || "unknown";
      const contentLength = Number(response.headers["content-length"] || 0);
      const finalUrl = response.request?.res?.responseUrl || candidateUrl;

      console.log(
        `[TIKTOK] Download response status=${response.status} bytes=${contentLength || "unknown"} type=${contentType} finalUrl=${finalUrl}`,
      );

      if (/text\/html|application\/json|text\/plain/i.test(contentType)) {
        throw new Error(`Download URL returned ${contentType} instead of a video file.`);
      }

      if (contentLength > MAX_ATTACHMENT_BYTES) {
        response.data.destroy();
        return {
          tooLarge: true,
          bytes: contentLength,
          contentType,
          finalUrl,
        };
      }

      const fileStream = fs.createWriteStream(filePath);
      let totalBytes = 0;

      const streamResult = await new Promise((resolve, reject) => {
        let settled = false;

        function finish(error, result) {
          if (settled) {
            return;
          }

          settled = true;

          if (error) {
            reject(error);
            return;
          }

          resolve(result);
        }

        response.data.on("data", (chunk) => {
          totalBytes += chunk.length;

          if (totalBytes > MAX_ATTACHMENT_BYTES) {
            response.data.destroy(new Error("Attachment exceeds Messenger upload size limit."));
            fileStream.destroy();
            safeUnlink(filePath);
            finish(null, {
              tooLarge: true,
              bytes: totalBytes,
              contentType,
              finalUrl,
            });
          }
        });

        response.data.on("error", (error) => {
          if (error.message === "Attachment exceeds Messenger upload size limit.") {
            return;
          }

          finish(error);
        });

        fileStream.on("error", finish);

        fileStream.on("finish", () => {
          if (!totalBytes) {
            finish(new Error(`Download URL returned 0 bytes (${contentType}).`));
            return;
          }

          finish(null, {
            tooLarge: false,
            bytes: totalBytes,
            contentType,
            finalUrl,
          });
        });

        response.data.pipe(fileStream);
      });

      return streamResult;
    } catch (error) {
      lastError = error;
      console.error(`[TIKTOK] Download attempt ${index + 1} failed:`, error.message);
    }
  }

  throw lastError || new Error("All TikTok download URLs failed.");
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB"];
  let value = bytes;
  let unitIndex = 0;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  return `${value.toFixed(value >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

async function replyWithDownloadLink(reply, tiktokUrl, videoData, sizeBytes, directUrl) {
  const sizeLine = sizeBytes ? `\nFile size: ${formatBytes(sizeBytes)}` : "";
  const downloadLine = directUrl ? `\nDirect download: ${directUrl}` : "";

  await reply(
    `TikTok video is ready, but Facebook rejected the upload because the file is too large.\n\nAuthor: ${videoData.author || "Unknown"}\nTitle: ${videoData.title || "No title"}${sizeLine}\nSource: ${tiktokUrl}${downloadLine}`,
  );
}

function react(api, reaction, messageID) {
  if (typeof api.setMessageReaction !== "function") return;
  api.setMessageReaction(reaction, messageID).catch(() => {});
}

module.exports = {
  description: "Download a TikTok video.",
  usage: "<url>",
  async execute({ api, args, event, reply, rootDir }) {
    const tiktokUrl = args[0];

    if (!tiktokUrl) {
      await reply("Please provide a TikTok video URL.");
      return;
    }

    const filePath = path.join(rootDir, `tiktok_${Date.now()}.mp4`);
    let processingMsg = null;

    // React with ⏳ to signal the bot is working on it
    react(api, "⏳", event.messageID);

    try {
      // Use the HTTP sendMessage directly to get a real Facebook-assigned message
      // ID. The MQTT path returns only a synthetic local ID (mid.<otid>) which
      // the unsend endpoint silently ignores, so the processing message would
      // never actually be deleted.
      processingMsg = await api.sendMessage(
        "Processing TikTok download...",
        event.threadID,
        event.messageID,
      );

      const response = await axios.get(`https://api.zenithapi.qzz.io/tiktok?url=${encodeURIComponent(tiktokUrl)}`);
      const result = response.data;
      const videoData = result?.data;

      if (!result?.success || !videoData || !getTikTokCandidateUrls(videoData).length) {
        console.log("[TIKTOK] Invalid API response:", JSON.stringify(result));
        react(api, "❌", event.messageID);
        await reply("TikTok API response was invalid.");
        return;
      }

      const downloadInfo = await downloadTikTokVideo(videoData, filePath);
      console.log(
        `[TIKTOK] Download finished: ${filePath} (${downloadInfo.bytes} bytes, ${downloadInfo.contentType})`,
      );

      if (downloadInfo.tooLarge) {
        console.log(
          `[TIKTOK] Skipping Messenger upload because file size ${downloadInfo.bytes} exceeds limit ${MAX_ATTACHMENT_BYTES}.`,
        );
        react(api, "❌", event.messageID);
        await replyWithDownloadLink(reply, tiktokUrl, videoData, downloadInfo.bytes, downloadInfo.finalUrl);
        return;
      }

      const { size } = fs.statSync(filePath);
      if (!size) {
        throw new Error("Downloaded video file is empty.");
      }

      await reply({
        body: `TikTok video downloaded.\n\nAuthor: ${videoData.author || "Unknown"}\nTitle: ${videoData.title || "No title"}`,
        attachment: fs.createReadStream(filePath),
      });

      if (processingMsg?.messageID && typeof api.unsendMessage === "function") {
        try {
          await api.unsendMessage(processingMsg.messageID);
        } catch (unsendError) {
          console.error("[TIKTOK] Failed to unsend processing message:", unsendError.message);
        }
      }

      // React with ✅ to confirm success
      react(api, "✅", event.messageID);
      console.log("[TIKTOK] Video sent successfully.");
    } catch (error) {
      console.error("[TIKTOK] Command failed:", error);

      react(api, "❌", event.messageID);

      if (error && error.statusCode === 413) {
        try {
          const response = await axios.get(`https://api.zenithapi.qzz.io/tiktok?url=${encodeURIComponent(tiktokUrl)}`);
          const videoData = response.data?.data || {};
          const size = fs.existsSync(filePath) ? fs.statSync(filePath).size : 0;
          await replyWithDownloadLink(reply, tiktokUrl, videoData, size, getTikTokCandidateUrls(videoData)[0]);
          return;
        } catch (fallbackError) {
          console.error("[TIKTOK] Link fallback failed:", fallbackError);
        }
      }

      await reply(`TikTok download failed: ${error.message || "Unknown error"}`);
    } finally {
      safeUnlink(filePath);
    }
  },
};
