require("dotenv").config();

const fs = require("fs");
const path = require("path");
const axios = require("axios");

const SETTINGS_PATH = path.join(__dirname, "settings.json");
const COMMANDS_DIR = path.join(__dirname, "cmd");
const LOCAL_BIAR_FCA_PATH = path.join(__dirname, "biar-fca");

function loadBiarFca() {
  const candidates = [LOCAL_BIAR_FCA_PATH, "biar-fca"];
  let lastError = null;

  for (const candidate of candidates) {
    try {
      const resolvedPath = require.resolve(candidate);
      console.log(`[BOOT] Loading biar-fca from ${resolvedPath}`);
      return require(candidate);
    } catch (error) {
      lastError = error;
    }
  }

  console.error("[BOOT] Failed to load biar-fca from the local folder or node_modules.");
  throw lastError;
}

const { login } = loadBiarFca();

function readJsonFile(filePath, label) {
  if (!fs.existsSync(filePath)) {
    console.error(`[BOOT] Missing ${label} in the project root.`);
    process.exit(1);
  }

  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    console.error(`[BOOT] ${label} is not valid JSON:`, error.message);
    process.exit(1);
  }
}

function loadSettings() {
  const settings = readJsonFile(SETTINGS_PATH, "settings.json");

  if (typeof settings.prefix !== "string" || !settings.prefix.trim()) {
    console.error("[BOOT] settings.json must contain a non-empty string prefix.");
    process.exit(1);
  }

  return settings;
}

async function fetchCredentials() {
  const url = process.env.APPSTATE_URL;

  if (!url || !url.trim()) {
    console.error("[BOOT] APPSTATE_URL is not set in your .env file.");
    console.error("[BOOT] Upload your appstate JSON to https://pastebin.com, copy the raw link, and set APPSTATE_URL=<raw_url> in .env");
    process.exit(1);
  }

  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    console.error("[BOOT] APPSTATE_URL is not a valid URL.");
    process.exit(1);
  }

  if (parsed.protocol !== "https:") {
    console.error("[BOOT] APPSTATE_URL must use HTTPS.");
    process.exit(1);
  }

  if (parsed.hostname !== "pastebin.com" || !parsed.pathname.startsWith("/raw/")) {
    console.error("[BOOT] APPSTATE_URL must be a Pastebin raw URL (e.g. https://pastebin.com/raw/XXXXXXXX).");
    process.exit(1);
  }

  try {
    const response = await axios.get(url, {
      timeout: 10000,
      maxRedirects: 0,
      headers: { "User-Agent": "biar-fca-bot" },
    });

    const appState = typeof response.data === "string"
      ? JSON.parse(response.data)
      : response.data;

    if (!Array.isArray(appState)) {
      console.error("[BOOT] Fetched appstate is not a valid JSON array. Check your Pastebin paste.");
      process.exit(1);
    }

    console.log("[BOOT] Appstate fetched successfully from APPSTATE_URL.");
    return { appState };
  } catch (error) {
    console.error("[BOOT] Failed to fetch appstate from APPSTATE_URL:", error.message);
    process.exit(1);
  }
}

function loadCommands() {
  if (!fs.existsSync(COMMANDS_DIR)) {
    console.error("[BOOT] Missing cmd folder.");
    process.exit(1);
  }

  const commandFiles = fs
    .readdirSync(COMMANDS_DIR)
    .filter((file) => file.endsWith(".js"))
    .sort((left, right) => left.localeCompare(right));

  if (!commandFiles.length) {
    console.error("[BOOT] No command files were found in cmd.");
    process.exit(1);
  }

  const commands = new Map();

  for (const fileName of commandFiles) {
    const commandName = path.basename(fileName, ".js").toLowerCase();
    const filePath = path.join(COMMANDS_DIR, fileName);
    const commandModule = require(filePath);

    if (!commandModule || typeof commandModule.execute !== "function") {
      console.error(`[BOOT] ${fileName} must export an object with an execute function.`);
      process.exit(1);
    }

    commands.set(commandName, {
      description: commandModule.description || "",
      usage: commandModule.usage || "",
      execute: commandModule.execute,
    });
  }

  console.log(`[BOOT] Loaded commands: ${[...commands.keys()].join(", ")}`);
  return commands;
}

function sendMessageMqtt(api, message, threadID, replyToMessageID) {
  return new Promise((resolve, reject) => {
    api.sendMessageMqtt(message, threadID, replyToMessageID, (error, info) => {
      if (error) {
        reject(error);
        return;
      }

      resolve(info);
    });
  });
}

async function sendReply(api, message, threadID, replyToMessageID) {
  if (typeof api.sendMessageMqtt === "function") {
    try {
      return await sendMessageMqtt(api, message, threadID, replyToMessageID);
    } catch (error) {
      console.error("[SEND] MQTT send failed, falling back to sendMessage:", error.message);
    }
  }

  return api.sendMessage(message, threadID, replyToMessageID);
}

function getUserInfo(api, userID) {
  return new Promise((resolve, reject) => {
    api.getUserInfo(userID, (error, data) => {
      if (error) {
        reject(error);
        return;
      }

      resolve(data);
    });
  });
}

function isMessageEvent(event) {
  return event && (event.type === "message" || event.type === "message_reply");
}

async function main() {
  const settings = loadSettings();
  const commands = loadCommands();
  const prefix = settings.prefix;
  const loginOptions = {
    online: false,
    updatePresence: false,
    selfListen: false,
    listenEvents: true,
    listenTyping: false,
    advancedProtection: false,
    autoMarkRead: false,
    autoMarkDelivery: false,
    autoReconnect: true,
    forceLogin: false,
    emitReady: false,
    mqttReconnectPolicy: {
      clientReconnectPeriod: 0,
      initialRetryDelay: 2000,
      maxReconnectAttempts: 10,
      maxRetryDelay: 30000,
      periodicReconnect: false,
    },
  };

  console.log("[BOOT] Starting biar-fca-bot...");
  console.log(`[BOOT] Using prefix "${prefix}" from settings.json`);
  console.log("[BOOT] Fetching appstate from APPSTATE_URL...");

  const credentials = await fetchCredentials();

  console.log("[BOOT] Logging in to Facebook...");

  login(
    credentials,
    loginOptions,
  async (error, api) => {
    if (error) {
      console.error("[LOGIN] Failed:", error);
      return;
    }

    api.setOptions({
      listenEvents: true,
      selfListen: false,
      autoMarkRead: false,
      autoMarkDelivery: false,
      autoReconnect: true,
      updatePresence: false,
      online: false,
      emitReady: false,
      advancedProtection: false,
      mqttReconnectPolicy: {
        clientReconnectPeriod: 0,
        initialRetryDelay: 2000,
        maxReconnectAttempts: 10,
        maxRetryDelay: 30000,
        periodicReconnect: false,
      },
    });

    const botID = api.getCurrentUserID();
    console.log(`[LOGIN] Logged in successfully. Bot ID: ${botID}`);

    setInterval(() => {
      const stats = typeof api.getProtectionStats === "function" ? api.getProtectionStats() : null;
      const sessionId = stats?.sessionID ? stats.sessionID.slice(0, 12) : "n/a";
      console.log(`[HEARTBEAT] Bot process alive. Session: ${sessionId}`);
    }, 300000);

    try {
      const listener = await api.listenMqtt(async (listenError, event) => {
        if (listenError) {
          console.error("[MQTT] Listener error:", listenError);
          return;
        }

        if (!isMessageEvent(event) || !event.body) {
          return;
        }

        console.log(`[MQTT] ${event.senderID} -> ${event.threadID}: ${event.body}`);

        if (!event.body.startsWith(prefix)) {
          return;
        }

        const commandLine = event.body.slice(prefix.length).trim();
        if (!commandLine) {
          return;
        }

        const args = commandLine.split(/\s+/);
        const commandName = (args.shift() || "").toLowerCase();
        const command = commands.get(commandName);

        if (!command) {
          return;
        }

        try {
          await command.execute({
            api,
            args,
            commands,
            event,
            getUserInfo: (userID) => getUserInfo(api, userID),
            prefix,
            reply: (message, threadID = event.threadID, replyToMessageID = event.messageID) =>
              sendReply(api, message, threadID, replyToMessageID),
            rootDir: __dirname,
            sendReply: (message, threadID, replyToMessageID) => sendReply(api, message, threadID, replyToMessageID),
            settings,
          });
        } catch (commandError) {
          console.error(`[BOT] Command "${commandName}" failed:`, commandError);
          await sendReply(
            api,
            `Command "${commandName}" failed: ${commandError.message || "Unknown error"}`,
            event.threadID,
            event.messageID,
          );
        }
      });

      if (listener && typeof listener.on === "function") {
        listener.on("error", (listenerError) => {
          console.error("[MQTT] Listener emitter error:", listenerError);
        });
      }

      console.log("[MQTT] Listener started.");
    } catch (listenerError) {
      console.error("[MQTT] Failed to start listener:", listenerError);
    }
  },
);

}

main().catch((err) => {
  console.error("[BOOT] Fatal error:", err);
  process.exit(1);
});
