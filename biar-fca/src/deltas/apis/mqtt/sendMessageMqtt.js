"use strict";

const utils = require('../../../utils');
// const delay = async ms => await new Promise(res => setTimeout(res, ms));

module.exports = (defaultFuncs, api, ctx) => {
  function invokeCallback(callback, error, data) {
    if (typeof callback === "function") {
      callback(error, data);
    }
  }

  /**
   * Uploads an attachment to Facebook's servers.
   * @param {Array<Stream>} attachments An array of readable streams.
   * @param {Function} callback The callback function.
   */
  function uploadAttachment(attachments, callback) {
    callback = callback || function () {};
    const uploads = [];
    for (let i = 0; i < attachments.length; i++) {
      if (!utils.isReadableStream(attachments[i])) {
        throw { error: "Attachment should be a readable stream and not " + utils.getType(attachments[i]) + "." };
      }
      const form = {
        upload_1024: attachments[i],
        voice_clip: "true",
      };
      uploads.push(
        defaultFuncs
          .postFormData("https://upload.facebook.com/ajax/mercury/upload.php", ctx.jar, form, {})
          .then(utils.parseAndCheckLogin(ctx, defaultFuncs))
          .then(resData => {
            if (resData.error) throw resData;
            return resData.payload.metadata[0];
          }),
      );
    }
    Promise.all(uploads)
      .then(resData => callback(null, resData))
      .catch(err => {
        utils.error("uploadAttachment", err);
        return callback(err);
      });
  }

  function getSendPayload(threadID, msg, otid) {
    const isString = typeof msg === 'string';
    const body = isString ? msg : msg.body || "";
    otid = otid.toString() || utils.generateOfflineThreadingID().toString();
    const payload = {
      thread_id: threadID.toString(),
      otid,
      source: 0,
      send_type: 1,
      sync_group: 1,
      text: body,
      initiating_source: 1,
      skip_url_preview_gen: 0,
    };
    if (typeof msg === 'object') {
      if (msg.sticker) {
        payload.send_type = 2;
        payload.sticker_id = msg.sticker;
        payload.text = null;
      }
      if (msg.attachment) {
        payload.send_type = 3;
        payload.attachment_fbids = Array.isArray(msg.attachment) ? msg.attachment : [msg.attachment];
      }
    }
    return payload;
  }

  /**
   * Sends a message to a thread via MQTT with optional sequential editing.
   * @param {object|string} msg The message to send. Can be a string or an object.
   * @param {string} msg.body The main text of the message.
   * @param {*} [msg.attachment] An attachment to send.
   * @param {*} [msg.sticker] A sticker to send.
   * @param {*} [msg.emoji] An emoji to send.
   * @param {string} threadID The ID of the thread.
   * @param {string} [replyToMessage] The ID of the message to reply to.
   */
  
  async function sendMessageMqttInternal(msg, threadID, replyToMessage, callback, fallbackAttempted = false) {
    let prepared = null;
      
    if (typeof msg !== 'string' && typeof msg !== 'object') {
      throw new Error("Message should be of type string or object, not " + utils.getType(msg) + ".");
    }

    if (typeof threadID !== 'string' && typeof threadID !== 'number') {
      throw new Error("threadID must be a string or number.");
    }

    if (!callback && typeof threadID === "function") {
      throw new Error("Pass a threadID as a second argument.");
    }
    
    if (!callback && typeof replyToMessage === "function") {
      callback = replyToMessage;
      replyToMessage = null;
    }

    let resolveFunc = () => {};
    let rejectFunc = () => {};
    const returnPromise = new Promise((resolve, reject) => {
      resolveFunc = resolve;
      rejectFunc = reject;
    });

    const userCallback = callback;
    callback = (err, data) => {
      if (typeof userCallback === "function") {
        userCallback(err, data);
      }
      if (err) return rejectFunc(err);
      resolveFunc(data);
    };

    if (!ctx.mqttClient || !ctx.mqttClient.connected) {
      const error = new Error("MQTT client is not connected. Cannot send message.");
      invokeCallback(callback, error);
      return returnPromise;
    }
    
    const timestamp = Date.now();
    const otid = utils.generateOfflineThreadingID();
    const epoch_id = utils.generateOfflineThreadingID();
    if (typeof msg === "string") {
      msg = { body: msg };
    }

    try {
      prepared = await utils.prepareMessageForSend(ctx, msg, {
        api,
        threadID,
        replyToMessage,
        transport: "mqtt",
      });
      msg = prepared.message;
    } catch (error) {
      if (!fallbackAttempted && utils.isAttachmentTooLargeError(error)) {
        const fallbackMessage = await utils.resolveUploadFallback(ctx, {
          api,
          error,
          message: msg,
          replyToMessage,
          threadID,
          transport: "mqtt",
        });

        if (fallbackMessage) {
          await prepared?.cleanup?.();
          return sendMessageMqttInternal(fallbackMessage, threadID, replyToMessage, callback, true);
        }
      }

      invokeCallback(callback, error);
      return returnPromise;
    }

    const payload = getSendPayload(threadID, msg, otid);
        
    const tasks = [{
      label: "46",
      payload,
      queue_name: threadID.toString(),
      task_id: 0,
      failure_count: null,
    }, {
      label: "21",
      payload: {
        thread_id: threadID.toString(),
        last_read_watermark_ts: timestamp,
        sync_group: 1,
      },
      queue_name: threadID.toString(),
      task_id: 1,
      failure_count: null,
    }];

    if (replyToMessage) {
      tasks[0].payload.reply_metadata = {
        reply_source_id: replyToMessage,
        reply_source_type: 1,
        reply_type: 0,
      };
    }

    const form = {
      app_id: "2220391788200892",
      payload: {
        tasks,
        epoch_id,
        version_id: "6120284488008082",
        data_trace_id: null,
      },
      request_id: 1,
      type: 3,
    };
     
    if (msg.attachment) {
      try {
        const files = await new Promise((resolve, reject) => {
          uploadAttachment(
            utils.normalizeAttachmentArray(msg.attachment),
            (err, files) => {
              if (err) return reject(err);
              return resolve(files);
            }
          );
        });
        form.payload.tasks[0].payload.attachment_fbids = files.map(file => Object.values(file)[0]);
      } catch (err) {
        if (!fallbackAttempted && utils.isAttachmentTooLargeError(err)) {
          const fallbackMessage = await utils.resolveUploadFallback(ctx, {
            api,
            error: err,
            message: msg,
            replyToMessage,
            threadID,
            transport: "mqtt",
          });

          if (fallbackMessage) {
            await prepared?.cleanup?.();
            return sendMessageMqttInternal(fallbackMessage, threadID, replyToMessage, callback, true);
          }
        }

        utils.error("Attachment upload failed:", err);
        invokeCallback(callback, err);
        return returnPromise;
      }
    }

    form.payload.tasks.forEach(task => {
      task.payload = JSON.stringify(task.payload);
    });
    form.payload = JSON.stringify(form.payload);

    const messageID = `mid.${otid}`;
    const responseTimeout = setTimeout(() => {
      if (ctx.reqCallbacks && ctx.reqCallbacks[messageID]) {
        delete ctx.reqCallbacks[messageID];
      }
    }, 10000);

    try {
      if (!ctx.reqCallbacks) {
        ctx.reqCallbacks = {};
      }

      ctx.reqCallbacks[messageID] = {
        callback: (err, _result) => {
          clearTimeout(responseTimeout);
          delete ctx.reqCallbacks[messageID];

          if (err) {
            if (err.error === 1545012 || err.errorCode === 1545012) {
              utils.warn("sendMessageMqtt [MQTT]", `Got error 1545012. Bot is not part of the conversation ${threadID}`);
              invokeCallback(callback, null, null);
              return;
            }
            
            if (err.transientError) {
              utils.warn("sendMessageMqtt [MQTT]", `Transient error ${err.error || err.errorCode}: ${err.errorDescription || 'Temporary failure'} (thread: ${threadID})`);
              invokeCallback(callback, null, null);
              return;
            }

            invokeCallback(callback, err);
            return;
          }

          invokeCallback(callback, null, {
            threadID: threadID.toString(),
            messageID,
            timestamp,
            type: replyToMessage ? "message_reply" : "message"
          });
        },
        timestamp: Date.now(),
        threadID: threadID.toString()
      };

      await ctx.mqttClient.publish("/ls_req", JSON.stringify(form), {
        qos: 0,
        retain: false
      });

      setTimeout(() => {
        if (ctx.reqCallbacks && ctx.reqCallbacks[messageID]) {
          invokeCallback(callback, null, {
            threadID: threadID.toString(),
            messageID,
            timestamp,
            type: replyToMessage ? "message_reply" : "message"
          });
          clearTimeout(responseTimeout);
          delete ctx.reqCallbacks[messageID];
        }
      }, 1000);

    } catch (err) {
      clearTimeout(responseTimeout);
      if (ctx.reqCallbacks && ctx.reqCallbacks[messageID]) {
        delete ctx.reqCallbacks[messageID];
      }
      utils.error("sendMessageMqtt", "Failed to send message:", err);
      invokeCallback(callback, err);
    } finally {
      if (prepared) {
        await prepared.cleanup();
      }
    }

    return returnPromise;
  }

  return async (msg, threadID, replyToMessage, callback) => {
    const pending = sendMessageMqttInternal(msg, threadID, replyToMessage, callback);
    if (callback) {
      pending.catch(() => {});
      return pending;
    }
    return pending;
  };
};
