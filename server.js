'use strict';

const debug = require('debug')('sensbox:mqtt');
const chalk = require('chalk');
const Parse = require('parse/node');
const aedes = require('aedes')();
const server = require('net').createServer(aedes.handle);
const port = parseInt(process.env.PORT) || 1883;

const { bufferToUTF8, parsePayload } = require('./utils');

Parse.initialize(process.env.CORE_APP_ID);
Parse.serverURL = process.env.CORE_URL;

function initServer() {
  const clients = new Map();

  server.listen(port, function() {
    debug(`${chalk.green('[sensbox-mqtt]')} server is running on ${port}`);
  });

  aedes.authenticate = async function(client, username, password, callback) {
    try {
      await Parse.Cloud.run('mqttAuthorizeClient', {
        username: bufferToUTF8(username),
        password: bufferToUTF8(password)
      });
      callback(null, true);
    } catch (error) {
      debug(error);
      error.returnCode = 4;
      callback(error, null);
    }
  };

  aedes.on('client', client => {
    debug(`Client Connected: ${client.id}`);
    clients.set(client.id, null);
  });

  aedes.on('clientDisconnect', async client => {
    debug(`Client Disconnected: ${client.id}`);
    const device = clients.get(client.id);
    if (device) {
      // Set Device as Disconnected
      const {
        device: serverDevice
      } = await Parse.Cloud.run('mqttDisconnectDevice', { uuid: device.uuid });
      // Delete Agente from Clients List
      clients.delete(client.id);
      aedes.publish({
        topic: 'agent/disconnected',
        payload: JSON.stringify({
          agent: {
            uuid: serverDevice.uuid
          }
        })
      });
      debug(
        `Client (${client.id}) associated to Agente (${device.uuid}) id disconnected`
      );
    }
  });

  aedes.on('publish', async function(packet, client) {
    debug(`Received: ${packet.topic}`);

    switch (packet.topic) {
      case 'agent/connected':
      case 'agent/disconnected':
        debug(`Payload: ${packet.payload}`);
        break;
      case 'agent/message':
        // debug(`Payload: ${packet.payload}`)
        const payload = parsePayload(packet.payload);
        if (payload) {
          try {
            // Notify Device is Connected
            if (!clients.get(client.id)) {
              const { device } = await Parse.Cloud.run('mqttConnectDevice', {
                uuid: payload.agent.uuid
              });
              debug(`Device ${JSON.stringify(device)} connected`);
              clients.set(client.id, device);
              aedes.publish({
                topic: 'agent/connected',
                payload: JSON.stringify({
                  agent: {
                    uuid: device.uuid,
                    hostname: device.hostname,
                    connected: device.connected
                  }
                })
              });
            }
          } catch (error) {
            debug(error);
            break;
          }
          Parse.Cloud.run('mqttHandlePayload', { payload })
            .then(result => {
              debug(`Device ${result.device.uuid}, stored ${result.stored}`);
            })
            .catch(handleError);
        }
        break;
    }
  });

  const listenDevicesConfigurations = async () => {
    const query = new Parse.Query('DeviceMessage');
    query.equalTo('topic', 'agent/configuration');
    const subscription = await query.subscribe();
    subscription.on('create', message => {
      const { uuid } = message.toJSON();
      // console.log("EVENTS", jsonMessage);
      clients.forEach((device, clientId) => {
        if (device.uuid === uuid) {
          const client = aedes.clients[clientId];
          if (client) {
            client.publish({
              topic: 'agent/configuration',
              payload: JSON.stringify({
                agent: {
                  uuid: message.uuid
                },
                configurations: message.payload
              })
            });
          }
        }
      });
    });
  };
  listenDevicesConfigurations();
  server.on('error', handleFatalError);
}

function handleFatalError(err) {
  console.error(`${chalk.red('[fatal error]')} ${err.message}`);
  console.error(err.stack);
  process.exit(1);
}

function handleError(err) {
  console.error(`${chalk.red('[error]')} ${err.message}`);
  console.error(err.stack);
}

process.on('uncaughtException', handleFatalError);
process.on('unhandledRejection', handleFatalError);

initServer();
