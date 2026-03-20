"use strict";
const utils = require('../../../utils');
const mqtt = require('mqtt');
const websocket = require('websocket-stream');
const HttpsProxyAgent = require('https-proxy-agent');
const EventEmitter = require('events');
const { parseDelta } = require('./deltas/value');

let form = {};
let getSeqID;

const accountBlockTracker = {
    consecutiveFailures: 0,
    lastFailureTime: 0,
    isBlocked: false,
    blockReason: null
};

const MQTT_TOPICS = [
    "/legacy_web", "/webrtc", "/rtc_multi", "/onevc", "/br_sr", "/sr_res",
    "/t_ms", "/thread_typing", "/orca_typing_notifications", "/notify_disconnect",
    "/orca_presence", "/inbox", "/mercury", "/messaging_events",
    "/orca_message_notifications", "/pp", "/webrtc_response",
    "/ls_req", "/ls_resp" // Added Layer Sync topics
];

// Added jitter helper
const getJitter = (range) => Math.floor(Math.random() * (range * 2 + 1)) - range;

const MQTT_CONFIG = {
    get KEEPALIVE_INTERVAL() { return 60 + Math.floor(Math.random() * 30); },
    CONNECT_TIMEOUT: 60000,
    get RECONNECT_PERIOD() { return 5000 + Math.floor(Math.random() * 2000); },
    MAX_RECONNECT_ATTEMPTS: 15,
    get PRESENCE_UPDATE_INTERVAL() { return 180000 + getJitter(30000); },
    get IDLE_CHECK_INTERVAL() { return 120000 + getJitter(20000); },
    MAX_IDLE_TIME: 8 * 60 * 1000,
    MIN_RECONNECT_TIME: 4 * 60 * 60 * 1000,
    MAX_RECONNECT_TIME: 8 * 60 * 60 * 1000,
    PROTOCOL_VERSION: 3,
    QOS_LEVEL: 0, // QoS 0 avoids PUBACK - FB's MQTT server sends non-standard PUBACK that mqtt@5.x rejects
    INITIAL_RETRY_DELAY: 2000,
    MAX_RETRY_DELAY: 60000,
    RETRY_MULTIPLIER: 1.5
};

const SYNC_CONFIG = {
    API_VERSION: 11, // Updated version
    MAX_DELTAS: 1000,
    BATCH_SIZE: 500,
    ENCODING: "JSON"
};

const mqttReconnectionTracker = {
    attemptCount: 0,
    lastAttemptTime: 0,
    currentDelay: 2000,
    isReconnecting: false
};

function getReconnectPolicy(ctx) {
    const policy = ctx.globalOptions?.mqttReconnectPolicy || {};
    return {
        clientReconnectPeriod: Number.isFinite(policy.clientReconnectPeriod) ? policy.clientReconnectPeriod : 0,
        initialRetryDelay: Number.isFinite(policy.initialRetryDelay) ? policy.initialRetryDelay : MQTT_CONFIG.INITIAL_RETRY_DELAY,
        keepAlive: Number.isFinite(policy.keepAlive) ? policy.keepAlive : MQTT_CONFIG.KEEPALIVE_INTERVAL,
        maxReconnectAttempts: Number.isFinite(policy.maxReconnectAttempts) ? policy.maxReconnectAttempts : MQTT_CONFIG.MAX_RECONNECT_ATTEMPTS,
        maxReconnectTime: Number.isFinite(policy.maxReconnectTime) ? policy.maxReconnectTime : MQTT_CONFIG.MAX_RECONNECT_TIME,
        maxRetryDelay: Number.isFinite(policy.maxRetryDelay) ? policy.maxRetryDelay : MQTT_CONFIG.MAX_RETRY_DELAY,
        minReconnectTime: Number.isFinite(policy.minReconnectTime) ? policy.minReconnectTime : MQTT_CONFIG.MIN_RECONNECT_TIME,
        periodicReconnect: policy.periodicReconnect !== false,
        retryMultiplier: Number.isFinite(policy.retryMultiplier) ? policy.retryMultiplier : MQTT_CONFIG.RETRY_MULTIPLIER
    };
}

function getReconnectionDelay(ctx) {
    const policy = getReconnectPolicy(ctx);
    const delay = Math.min(
        mqttReconnectionTracker.currentDelay + getJitter(500),
        policy.maxRetryDelay
    );
    mqttReconnectionTracker.currentDelay *= policy.retryMultiplier;
    return delay;
}

function resetReconnectionState(ctx) {
    const policy = getReconnectPolicy(ctx);
    mqttReconnectionTracker.attemptCount = 0;
    mqttReconnectionTracker.currentDelay = policy.initialRetryDelay;
    mqttReconnectionTracker.isReconnecting = false;
}

function clearPendingReconnect(ctx) {
    if (ctx.pendingReconnectTimeout) {
        clearTimeout(ctx.pendingReconnectTimeout);
        ctx.pendingReconnectTimeout = null;
    }
}

function isAccountBlocked(error) {
    const errorMsg = String(error?.message || '').toLowerCase();
    const errorDetail = String(error?.error || '').toLowerCase();
    const originalError = String(error?.details?.originalError?.error || '').toLowerCase();
    
    return errorMsg.includes('facebook blocked the login') ||
           errorMsg.includes('not logged in') ||
           errorMsg.includes('account disabled') ||
           errorMsg.includes('security check') ||
           errorMsg.includes('checkpoint') ||
           errorDetail === 'not logged in.' ||
           originalError === 'not logged in.';
}

function handleAccountBlockDetection(error, ctx) {
    if (isAccountBlocked(error)) {
        accountBlockTracker.consecutiveFailures++;
        accountBlockTracker.lastFailureTime = Date.now();
        
        if (accountBlockTracker.consecutiveFailures >= 3) {
            accountBlockTracker.isBlocked = true;
            accountBlockTracker.blockReason = 'Facebook has flagged or logged out this account';
            
            ctx.loggedIn = false;
            
            utils.error("\n" + "=".repeat(80));
            utils.error("ACCOUNT SECURITY ALERT / BLOCKED");
            utils.error("=".repeat(80));
            utils.error("Your Facebook account has been flagged or logged out.");
            utils.error("");
            utils.error("Possible reasons:");
            utils.error("  - Automated activity detected");
            utils.error("  - Suspicious login location");
            utils.error("  - Rate limits exceeded");
            utils.error("  - Appstate/Cookies expired");
            utils.error("");
            utils.error("STOPPING all reconnection attempts to protect the account.");
            utils.error("=".repeat(80) + "\n");
            
            if (ctx.mqttClient) {
                try {
                    ctx.mqttClient.end(true);
                    ctx.mqttClient = null;
                } catch (e) {
                    // Ignore
                }
            }
            
            if (ctx.presenceInterval) {
                clearInterval(ctx.presenceInterval);
                ctx.presenceInterval = null;
            }
            
            if (ctx.idleCheckInterval) {
                clearInterval(ctx.idleCheckInterval);
                ctx.idleCheckInterval = null;
            }
            
            return true;
        }
    } else {
        accountBlockTracker.consecutiveFailures = 0;
    }
    
    return false;
}

function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

function getRandomReconnectTime(ctx) {
    const policy = getReconnectPolicy(ctx);
    const range = policy.maxReconnectTime - policy.minReconnectTime;
    return Math.floor(Math.random() * (range + 1)) + policy.minReconnectTime;
}

async function markAsRead(ctx, api, threadID, receivedMessageText) {
    if (!ctx.globalOptions.autoMarkRead || !threadID) {
        return;
    }

    // Anti-detection: Simulate human reading time
    const readingDelay = utils.calculateReadingTime(receivedMessageText);
    await new Promise(resolve => setTimeout(resolve, readingDelay));
    
    api.markAsRead(threadID, (err) => {
        if (err) {
            utils.error("autoMarkRead", `Failed to mark thread ${threadID} as read:`, err);
        }
    });
}

function buildMqttUsername(ctx, sessionID) {
    const username = {
        u: ctx.userID,
        s: sessionID,
        chat_on: ctx.globalOptions.online,
        fg: false,
        d: ctx.clientID,
        ct: 'websocket',
        aid: ctx.mqttAppID || '218474845183941',
        mqtt_sid: '',
        cp: 3,
        ecp: 10,
        st: [],
        pm: [],
        dc: '',
        no_auto_fg: true,
        gas: null,
        pack: [],
        a: ctx.globalOptions.userAgent,
        vi: '3', // Version Info
        lc: 'en_US'
    };
    return username;
}

function buildMqttOptions(ctx, host, username) {
    const cookies = ctx.jar.getCookiesSync('https://www.facebook.com').join('; ');
    const reconnectPolicy = getReconnectPolicy(ctx);
    
    const options = {
        clientId: 'mqttwsclient',
        protocolId: 'MQIsdp',
        protocolVersion: MQTT_CONFIG.PROTOCOL_VERSION,
        username: JSON.stringify(username),
        clean: true,
        wsOptions: {
            headers: {
                'Cookie': cookies,
                'Origin': 'https://www.messenger.com',
                'User-Agent': username.a,
                'Referer': 'https://www.messenger.com/',
                'Host': new URL(host).hostname
            },
            origin: 'https://www.messenger.com',
            protocolVersion: 13,
            binaryType: 'arraybuffer'
        },
        keepalive: reconnectPolicy.keepAlive,
        reschedulePings: true,
        connectTimeout: MQTT_CONFIG.CONNECT_TIMEOUT,
        reconnectPeriod: reconnectPolicy.clientReconnectPeriod
    };

    if (ctx.globalOptions.proxy) {
        options.wsOptions.agent = new HttpsProxyAgent(ctx.globalOptions.proxy);
    }

    return options;
}

async function listenMqtt(defaultFuncs, api, ctx, globalCallback) {
    try {
        const sessionID = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER) + 1;
        
        const domain = "wss://edge-chat.messenger.com/chat";
        const host = `${domain}?sid=${sessionID}&cid=${ctx.clientID}`;

        utils.log("Connecting to MQTT...", host);

        const username = buildMqttUsername(ctx, sessionID);
        const options = buildMqttOptions(ctx, host, username);

        const mqttClient = new mqtt.Client(() => websocket(host, options.wsOptions), options);
        
        mqttClient.publishSync = mqttClient.publish.bind(mqttClient);
        
        mqttClient.publish = (topic, message, opts = {}, callback = () => {}) => {
            return new Promise((resolve, reject) => {
                mqttClient.publishSync(topic, message, opts, (err, data) => {
                    if (err) {
                        callback(err);
                        return reject(err);
                    }
                    callback(null, data);
                    resolve(data);
                });
            });
        };

        ctx.mqttClient = mqttClient;

        mqttClient.on('error', (err) => {
            const errorMsg = err.message || String(err);
            const reconnectPolicy = getReconnectPolicy(ctx);

            // Facebook's MQTT server sends non-standard PUBACK/SUBACK header flags.
            // mqtt@5.x (via mqtt-packet) strictly validates these and emits an error.
            // This is harmless — treat it as non-fatal and keep the connection alive.
            if (errorMsg.includes('Invalid header flag bits') ||
                errorMsg.includes('must be 0x0 for puback') ||
                errorMsg.includes('must be 0x0 for suback')) {
                utils.warn('Received non-standard MQTT packet from Facebook server (ignored)');
                return;
            }
            
            if (errorMsg.includes('Server unavailable') || errorMsg.includes('Connection refused')) {
                mqttReconnectionTracker.attemptCount++;
                utils.warn(`WARNING: MQTT server unavailable (attempt ${mqttReconnectionTracker.attemptCount}/${reconnectPolicy.maxReconnectAttempts})`);
                
                if (mqttReconnectionTracker.attemptCount >= reconnectPolicy.maxReconnectAttempts) {
                    utils.error("Max MQTT reconnection attempts reached. Waiting longer before retry...");
                    mqttReconnectionTracker.currentDelay = reconnectPolicy.maxRetryDelay;
                }
            }
            
            const networkError = new utils.NetworkError(
                `MQTT connection error: ${errorMsg}`,
                { originalError: err, host, attemptCount: mqttReconnectionTracker.attemptCount }
            );
            utils.error("listenMqtt", networkError);
        });

        mqttClient.on('offline', () => {
            utils.warn("WARNING: MQTT client went offline. Will attempt reconnection...");
            ctx.mqttConnected = false;
        });

        mqttClient.on('close', () => {
            utils.warn("MQTT connection closed.");
            ctx.mqttConnected = false;
            
            if (accountBlockTracker.isBlocked) {
                utils.log("Reconnection cancelled: Account blocked by Facebook.");
                return;
            }

            if (ctx.mqttClient !== mqttClient) {
                return;
            }
            
            if (ctx.loggedIn !== false && ctx.globalOptions && ctx.globalOptions.autoReconnect !== false) {
                if (mqttReconnectionTracker.isReconnecting || ctx.pendingReconnectTimeout) {
                    utils.log("Reconnection already scheduled. Skipping duplicate close handler.");
                    return;
                }

                const reconnectDelay = getReconnectionDelay(ctx);
                const delaySeconds = (reconnectDelay / 1000).toFixed(1);
                
                utils.log(`Will attempt reconnection in ${delaySeconds} seconds... (attempt ${mqttReconnectionTracker.attemptCount + 1})`);
                mqttReconnectionTracker.isReconnecting = true;
                
                ctx.pendingReconnectTimeout = setTimeout(() => {
                    ctx.pendingReconnectTimeout = null;

                    if (accountBlockTracker.isBlocked) {
                        utils.log("Reconnection cancelled: Account blocked by Facebook.");
                        mqttReconnectionTracker.isReconnecting = false;
                        return;
                    }
                    
                    if (ctx.loggedIn !== false && (!ctx.mqttClient || !ctx.mqttClient.connected)) {
                        utils.log("Triggering MQTT reconnection...");
                        mqttReconnectionTracker.attemptCount++;
                        getSeqID();
                    } else {
                        utils.log("Reconnection cancelled: Bot logged out or already connected.");
                        mqttReconnectionTracker.isReconnecting = false;
                    }
                }, reconnectDelay);
            } else {
                if (ctx.loggedIn === false) {
                    utils.log("Reconnection cancelled: Bot logged out.");
                } else if (ctx.globalOptions && ctx.globalOptions.autoReconnect === false) {
                    utils.log("Reconnection disabled by configuration.");
                } else {
                    utils.log("Reconnection cancelled.");
                }
            }
        });

        mqttClient.on('reconnect', () => {
            utils.log("MQTT reconnecting...");
        });

        mqttClient.on('connect', async () => {
            try {
                clearPendingReconnect(ctx);
                mqttReconnectionTracker.isReconnecting = false;

                MQTT_TOPICS.forEach(topic => {
                    mqttClient.subscribe(topic, (err) => {
                        if (err) {
                            utils.error(`Failed to subscribe to ${topic}:`, err);
                        }
                    });
                });

                const queue = {
                    sync_api_version: SYNC_CONFIG.API_VERSION,
                    max_deltas_able_to_process: SYNC_CONFIG.MAX_DELTAS,
                    delta_batch_size: SYNC_CONFIG.BATCH_SIZE,
                    encoding: SYNC_CONFIG.ENCODING,
                    entity_fbid: ctx.userID,
                    device_params: {
                        "app_version": "444.0.0.35.118",
                        "os_version": "14",
                        "device_model": "iPhone15,3"
                    }
                };

                let syncTopic;
                if (ctx.syncToken) {
                    syncTopic = "/messenger_sync_get_diffs";
                    queue.last_seq_id = ctx.lastSeqId;
                    queue.sync_token = ctx.syncToken;
                } else {
                    syncTopic = "/messenger_sync_create_queue";
                    queue.initial_titan_sequence_id = ctx.lastSeqId;
                }

                utils.log("Successfully connected to MQTT");
                ctx.mqttConnected = true;
                
                resetReconnectionState(ctx);

                try {
                    const botData = await api.getBotInitialData();
                    if (botData) {
                        utils.log(`Logged in as: ${botData.name || "Facebook User"} (${botData.uid || ctx.userID})`);
                    }
                } catch (botInfoErr) {
                    // Ignore bot info errors
                }

                await mqttClient.publish(
                    syncTopic, 
                    JSON.stringify(queue), 
                    { qos: MQTT_CONFIG.QOS_LEVEL, retain: false }
                );
            } catch (connectErr) {
                utils.error("Error in connect handler:", connectErr);
                globalCallback(new utils.NetworkError(
                    "Failed to initialize MQTT connection",
                    { originalError: connectErr }
                ));
            }
        });

        const idleCheckInterval = setInterval(() => {
            if (!ctx.lastMessageTime) {
                return;
            }

            const timeSinceLastMessage = Date.now() - ctx.lastMessageTime;
            
            if (timeSinceLastMessage > MQTT_CONFIG.MAX_IDLE_TIME) {
                if (mqttClient && mqttClient.connected) {
                    utils.warn(`WARNING: MQTT idle for ${Math.floor(timeSinceLastMessage / 1000)}s. Testing connection...`);
                    
                    try {
                        const testPayload = utils.generatePresence(ctx.userID);
                        mqttClient.publish(
                            '/orca_presence',
                            JSON.stringify({ "p": testPayload }),
                            { qos: 0, retain: false },
                            (err) => {
                                if (err && ctx.reconnectMqtt) {
                                    utils.error("Connection test failed. Forcing reconnection...");
                                    ctx.reconnectMqtt().catch(() => {});
                                } else {
                                    utils.log("Connection test passed. MQTT is responsive.");
                                }
                            }
                        );
                    } catch (testErr) {
                        if (ctx.reconnectMqtt) ctx.reconnectMqtt().catch(() => {});
                    }
                }
            }
        }, MQTT_CONFIG.IDLE_CHECK_INTERVAL);

        ctx.idleCheckInterval = idleCheckInterval;
        
        if (ctx.globalOptions.updatePresence) {
            const presenceInterval = setInterval(() => {
                if (!mqttClient || !mqttClient.connected) {
                    return;
                }

                try {
                    const presencePayload = utils.generatePresence(ctx.userID);
                    mqttClient.publish(
                        '/orca_presence',
                        JSON.stringify({ "p": presencePayload }),
                        { qos: 0, retain: false }
                    );
                } catch (presenceErr) {
                    // Ignore presence errors
                }
            }, MQTT_CONFIG.PRESENCE_UPDATE_INTERVAL);

            ctx.presenceInterval = presenceInterval;
        }

        ctx.lastMessageTime = Date.now();
        ctx.messageCount = 0;

        mqttClient.on('message', async (topic, message, _packet) => {
            try {
                const msgString = message.toString();
                ctx.lastMessageTime = Date.now();
                ctx.messageCount++;

                const jsonMessage = JSON.parse(msgString);
                
                if (topic === "/t_ms") {
                    if (jsonMessage.lastIssuedSeqId) {
                        ctx.lastSeqId = parseInt(jsonMessage.lastIssuedSeqId, 10);
                    }
                    
                    if (jsonMessage.syncToken) {
                        ctx.syncToken = jsonMessage.syncToken;
                    }

                    if (jsonMessage.deltas && Array.isArray(jsonMessage.deltas)) {
                        for (const delta of jsonMessage.deltas) {
                            try {
                                parseDelta(defaultFuncs, api, ctx, globalCallback, { delta });
                            } catch (deltaErr) {
                                utils.error("Error parsing delta:", deltaErr);
                            }
                        }
                    }
                } else if (topic === "/mercury" || topic === "/messaging_events" || topic === "/orca_message_notifications") {
                    if (jsonMessage.deltas && Array.isArray(jsonMessage.deltas)) {
                        for (const delta of jsonMessage.deltas) {
                            try {
                                parseDelta(defaultFuncs, api, ctx, globalCallback, { delta });
                            } catch (deltaErr) {
                                // Ignore delta errors
                            }
                        }
                    }
                } else if (topic === "/thread_typing" || topic === "/orca_typing_notifications") {
                    const typingEvent = {
                        type: "typ",
                        isTyping: !!jsonMessage.state,
                        from: jsonMessage.sender_fbid?.toString() || "",
                        threadID: utils.formatID(
                            (jsonMessage.thread || jsonMessage.sender_fbid)?.toString() || ""
                        ),
                        fromMobile: jsonMessage.from_mobile || false
                    };
                    globalCallback(null, typingEvent);
                }
            } catch (parseErr) {
                // Ignore parse errors
            }
        });

    } catch (setupErr) {
        const error = new utils.NetworkError(
            "Failed to setup MQTT listener",
            { originalError: setupErr }
        );
        utils.error("listenMqtt setup error:", error);
        globalCallback(error);
    }
}

module.exports = (defaultFuncs, api, ctx) => {
    let globalCallback = () => {};
    let reconnectInterval;

    getSeqID = async () => {
        try {
            if (accountBlockTracker.isBlocked) {
                utils.log("Skipping getSeqID: Account blocked by Facebook.");
                return;
            }
            
            utils.log("Fetching sequence ID...");
            
            form = {
                "queries": JSON.stringify({
                    "o0": {
                        "doc_id": "3336396659757871",
                        "query_params": {
                            "limit": 1,
                            "before": null,
                            "tags": ["INBOX"],
                            "includeDeliveryReceipts": false,
                            "includeSeqID": true
                        }
                    }
                })
            };

            const resData = await defaultFuncs
                .post("https://www.facebook.com/api/graphqlbatch/", ctx.jar, form)
                .then(utils.parseAndCheckLogin(ctx, defaultFuncs));

            if (utils.getType(resData) !== "Array") {
                throw new utils.ValidationError("Invalid response format from sequence ID endpoint");
            }

            if (resData.error && resData.error !== 1357001) {
                throw new utils.AuthenticationError(
                    "Authentication error while fetching sequence ID",
                    { errorCode: resData.error }
                );
            }

            ctx.lastSeqId = resData[0]?.o0?.data?.viewer?.message_threads?.sync_sequence_id;
            
            if (!ctx.lastSeqId) {
                throw new utils.ValidationError("Sequence ID not found in response");
            }

            if (ctx.lastSeqId < 100) {
                utils.warn("getSeqID", `Received low sequence ID (${ctx.lastSeqId}). Resetting to force fresh sync.`);
                ctx.lastSeqId = null;
            }

            utils.log(`Sequence ID retrieved: ${ctx.lastSeqId || "(fresh sync)"}`);
            
            accountBlockTracker.consecutiveFailures = 0;

            listenMqtt(defaultFuncs, api, ctx, globalCallback);
            
        } catch (err) {
            const shouldStop = handleAccountBlockDetection(err, ctx);
            
            if (shouldStop) {
                return;
            }
            
            const authError = new utils.AuthenticationError(
                "Failed to get sequence ID. Please try generating a new appstate.json.",
                { originalError: err }
            );
            utils.error("getSeqID error:", authError);
            return globalCallback(authError);
        }
    };

    ctx.reconnectMqtt = async () => {
        try {
            if (accountBlockTracker.isBlocked) {
                utils.log("Skipping reconnect: Account blocked by Facebook.");
                return;
            }
            
            utils.log("Reconnecting MQTT...");
            
            if (ctx.mqttClient) {
                try {
                    ctx.mqttClient.end(true);
                } catch (endErr) {
                    // Ignore
                }
                ctx.mqttClient = null;
            }

            clearPendingReconnect(ctx);
            
            ctx.clientID = generateUUID();
            await getSeqID();
        } catch (err) {
            utils.error("Failed to reconnect MQTT:", err);
            throw err;
        }
    };

    return async (callback) => {
        class MessageEmitter extends EventEmitter {
            stop() {
                utils.log("Stopping MQTT listener...");
                
                globalCallback = () => {};
                
                if (reconnectInterval) {
                    clearTimeout(reconnectInterval);
                    reconnectInterval = null;
                }

                clearPendingReconnect(ctx);
                
                if (ctx.presenceInterval) {
                    clearInterval(ctx.presenceInterval);
                    ctx.presenceInterval = null;
                }
                
                if (ctx.idleCheckInterval) {
                    clearInterval(ctx.idleCheckInterval);
                    ctx.idleCheckInterval = null;
                }
                
                if (ctx.mqttClient) {
                    try {
                        ctx.mqttClient.end();
                        ctx.mqttClient = undefined;
                    } catch (err) {
                        // Ignore
                    }
                }
                
                this.emit('stop');
                utils.log("MQTT listener stopped");
            }
        }

        const msgEmitter = new MessageEmitter();

        globalCallback = (error, message) => {
            if (error) {
                return msgEmitter.emit("error", error);
            }
            
            if (message && (message.type === "message" || message.type === "message_reply")) {
                markAsRead(ctx, api, message.threadID, message.body);
            }
            
            msgEmitter.emit("message", message);
        };

        if (typeof callback === 'function') {
            const userCallback = callback;
            globalCallback = async (error, message) => {
                try {
                    if (message && (message.type === "message" || message.type === "message_reply")) {
                        await markAsRead(ctx, api, message.threadID, message.body);
                    }
                    userCallback(error, message);
                } catch (callbackError) {
                    utils.error("Error in user callback:", callbackError);
                    if (!error) {
                        msgEmitter.emit("error", callbackError);
                    }
                }
            };
        }

        if (!ctx.firstListen || !ctx.lastSeqId) {
            await getSeqID();
        } else {
            listenMqtt(defaultFuncs, api, ctx, globalCallback);
        }

        if (ctx.firstListen && ctx.globalOptions.autoMarkRead) {
            try {
                utils.log("Marking all messages as read on startup...");
                await api.markAsReadAll();
            } catch (err) {
                // Ignore
            }
        }

        ctx.firstListen = false;

        async function scheduleReconnect() {
            const reconnectPolicy = getReconnectPolicy(ctx);
            if (!reconnectPolicy.periodicReconnect) {
                return;
            }

            const time = getRandomReconnectTime(ctx);
            reconnectInterval = setTimeout(() => {
                utils.log("Periodic reconnection...");
                
                if (ctx.mqttClient) {
                    try {
                        ctx.mqttClient.end(true);
                    } catch (err) {
                        // Ignore
                    }
                }
                
                ctx.clientID = generateUUID();
                listenMqtt(defaultFuncs, api, ctx, globalCallback);
                
                scheduleReconnect();
            }, time);
        }

        scheduleReconnect();

        return msgEmitter;
    };
};
