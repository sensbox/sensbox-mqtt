'use strict'

const debug = require('debug')('sensbox:mqtt')
const chalk = require('chalk')
const Parse = require('parse/node')
const os = require('os')
const Redis = require('ioredis')
const redisPersistence = require('aedes-persistence-redis')
const mqemitter = require('mqemitter-redis')
const DSNParser = require('dsn-parser')
const port = parseInt(process.env.PORT) || 1883
const { bufferToUTF8, parsePayload } = require('./utils')

const redis = new Redis(process.env.REDIS_DSN)

var dsn = new DSNParser(process.env.REDIS_DSN)
const { host, port: redisPort, database, password } = dsn.getParts()

const aedes = require('aedes')({
  id: os.hostname(),
  mq: mqemitter({
    host,
    port: redisPort,
    db: database || 0,
    password
  }),
  persistence: redisPersistence({
    port: redisPort,
    host,
    family: 4, // 4 (IPv4) or 6 (IPv6)
    password,
    db: database || 0,
    maxSessionDelivery: 1000 // maximum offline messages deliverable on client CONNECT, default is 1000
  })
})
const server = require('net').createServer(aedes.handle)

Parse.initialize(process.env.CORE_APP_ID, undefined, process.env.PARSE_SERVER_MASTER_KEY)
Parse.serverURL = process.env.CORE_URL

function initServer() {
  server.listen(port, function () {
    debug(`${chalk.green('[sensbox-mqtt]')} server is running on ${port}`)
    debug(`${chalk.green('[sensbox-mqtt]')} redis parameters:`, dsn.getParts())
  })

  aedes.authenticate = async function (client, username, password, callback) {
    try {
      await Parse.Cloud.run(
        'mqttAuthorizeClient',
        {
          username: bufferToUTF8(username),
          password: bufferToUTF8(password)
        },
        { useMasterKey: true }
      )
      callback(null, true)
    } catch (error) {
      debug(error)
      error.returnCode = 4
      callback(error, null)
    }
  }

  aedes.on('client', (client) => {
    debug(`Client Connected: ${client.id}`)
    redis.set(client.id, null)
  })

  aedes.on('clientDisconnect', async (client) => {
    debug(`Client Disconnected: ${client.id}`)
    const result = await redis.get(client.id)
    const device = JSON.parse(result)
    if (device) {
      // Set Device as Disconnected
      const { device: serverDevice } = await Parse.Cloud.run(
        'mqttDisconnectDevice',
        {
          uuid: device.uuid
        },
        { useMasterKey: true }
      )
      // Delete Agente from Clients List
      await redis.del(client.id)
      aedes.publish({
        topic: 'agent/disconnected',
        payload: JSON.stringify({
          agent: {
            uuid: serverDevice.uuid
          }
        })
      })
      debug(`Client (${client.id}) associated to Agent (${device.uuid}) id disconnected`)
    }
  })

  aedes.on('publish', async function (packet, client) {
    debug(`Received: ${packet.topic}`)

    switch (packet.topic) {
      case 'agent/connected':
      case 'agent/disconnected':
        debug(`Payload: ${packet.payload}`)
        break
      case 'agent/message':
        // debug(`Payload: ${packet.payload}`)
        const payload = parsePayload(packet.payload)
        if (payload) {
          try {
            // Notify Device is Connected
            const result = await redis.get(client.id)
            const parsedClient = result ? JSON.parse(result) : undefined
            if (!parsedClient) {
              const { device } = await Parse.Cloud.run(
                'mqttConnectDevice',
                {
                  uuid: payload.agent.uuid
                },
                { useMasterKey: true }
              )
              debug(`Device ${JSON.stringify(device)} connected`)
              await redis.set(client.id, JSON.stringify(device))
              aedes.publish({
                topic: 'agent/connected',
                payload: JSON.stringify({
                  agent: {
                    uuid: device.uuid,
                    hostname: device.hostname,
                    connected: device.connected
                  }
                })
              })
            }
          } catch (error) {
            debug(error)
            break
          }
          Parse.Cloud.run('mqttHandlePayload', { payload }, { useMasterKey: true })
            .then((result) => {
              debug(`Device ${result.device.uuid}, stored ${result.stored}`)
            })
            .catch(handleError)
        }
        break
    }
  })

  const listenDevicesConfigurations = async () => {
    const query = new Parse.Query('DeviceMessage')
    query.equalTo('topic', 'agent/configuration')
    const subscription = await query.subscribe()
    subscription.on('create', async (message) => {
      const { uuid } = message.toJSON()
      // console.log("EVENTS", jsonMessage);
      const clientKeys = await redis.keys('mqttjs*')
      clientKeys.forEach(async (clientId) => {
        const result = await redis.get(clientId)
        const device = result ? JSON.parse(result) : {}
        if (device.uuid === uuid) {
          const client = aedes.clients[clientId]
          if (client) {
            client.publish({
              topic: 'agent/configuration',
              payload: JSON.stringify({
                agent: {
                  uuid: message.uuid
                },
                configurations: message.payload
              })
            })
          }
        }
      })
    })
  }
  listenDevicesConfigurations()
  server.on('error', handleFatalError)
}

initServer()

function handleFatalError(err) {
  console.error(`${chalk.red('[fatal error]')} ${err.message}`)
  console.error(err.stack)
  process.exit(1)
}

function handleError(err) {
  console.error(`${chalk.red('[error]')} ${err.message}`)
  console.error(err.stack)
}
process.on('uncaughtException', handleFatalError)
process.on('unhandledRejection', handleFatalError)
