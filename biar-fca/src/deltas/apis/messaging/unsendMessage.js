"use strict";

const utils = require('../../../utils');

module.exports = function (defaultFuncs, api, ctx) {
  return async function unsendMessage(messageID) {
    // Use MQTT (task 33 = DeleteMessages in Facebook's Lightspeed protocol).
    // This works with the mid.<otid> IDs produced by sendMessageMqtt, whereas
    // the old HTTP endpoint only accepts server-assigned message IDs.
    if (ctx.mqttClient && ctx.mqttClient.connected) {
      ctx.wsReqNumber = (ctx.wsReqNumber || 0) + 1;
      ctx.wsTaskNumber = (ctx.wsTaskNumber || 0) + 1;

      const query = {
        failure_count: null,
        label: '33',
        payload: JSON.stringify({ message_id: messageID }),
        queue_name: 'delete_message',
        task_id: ctx.wsTaskNumber,
      };

      const context = {
        app_id: '2220391788200892',
        payload: JSON.stringify({
          data_trace_id: null,
          epoch_id: parseInt(utils.generateOfflineThreadingID()),
          tasks: [query],
          version_id: '6903494529735864',
        }),
        request_id: ctx.wsReqNumber,
        type: 3,
      };

      return new Promise((resolve, reject) => {
        ctx.mqttClient.publish('/ls_req', JSON.stringify(context), { qos: 0, retain: false }, (err) => {
          if (err) return reject(err);
          resolve({ success: true });
        });
      });
    }

    // Fallback: legacy HTTP endpoint (only works with server-assigned message IDs).
    const defData = await defaultFuncs.post('https://www.facebook.com/messaging/unsend_message/', ctx.jar, {
      message_id: messageID,
    });
    const resData = await utils.parseAndCheckLogin(ctx, defaultFuncs)(defData);
    if (resData.error) {
      throw new Error(JSON.stringify(resData));
    }
    return resData;
  };
};
