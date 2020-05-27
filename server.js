'use strict'

const debug = require('debug')('sensbox:mqtt')
const chalk = require('chalk')
const ws = require('websocket-stream')
const Parse = require('parse/node')
const os = require('os')
const Redis = require('ioredis')
const redisPersistence = require('aedes-persistence-redis')
const mqemitter = require('mqemitter-redis')
const DSNParser = require('dsn-parser')
const port = parseInt(process.env.PORT) || 1883
const wsPort = parseInt(process.env.WS_PORT) || 8888

const { bufferToUTF8, parsePayload } = require('./utils')

const redis = new Redis(process.env.REDIS_DSN)

var dsn = new DSNParser(process.env.REDIS_DSN)
const { host, port: redisPort, database, password } = dsn.getParts()

debug(`${chalk.green('[sensbox-mqtt]')} redis parameters:`, dsn.getParts())

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

// Instantiate a TCP mqtt server
const server = require('net').createServer(aedes.handle)

// Instantiate a websocket server
const httpServer = require('http').createServer()
ws.createServer({ server: httpServer }, aedes.handle)

Parse.initialize(process.env.CORE_APP_ID, undefined, process.env.PARSE_SERVER_MASTER_KEY)
Parse.serverURL = process.env.CORE_URL

function initServer() {
  // TCP Server Listen
  server.listen(port, function () {
    debug(`${chalk.green('[sensbox-mqtt]')} server is running on ${port}`)
  })

  // Ws Server Listen
  httpServer.listen(wsPort, function () {
    debug(`${chalk.green('[sensbox-mqtt]')} websocket is running on ${wsPort}`)
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
    } catch (err) {
      debug(`${chalk.red('[Authentication Error]')} ${err.message}`)
      err.returnCode = 4
      callback(err, null)
    }
  }

  aedes.on('client', (client) => {
    debug(`Client Connected: ${client.id}`)
    redis.set(client.id, null)
  })

  aedes.on('clientDisconnect', async (client) => {
    debug(`Client Disconnected: ${client.id}`)
    try {
      const result = await redis.get(client.id)
      const device = result ? JSON.parse(result) : undefined
      if (device) {
        // Delete Agent from Clients List
        await redis.del(client.id)
        aedes.publish({
          topic: 'agent/disconnected',
          payload: JSON.stringify({
            agent: {
              uuid: device.uuid
            }
          })
        })
        // Set Device as Disconnected
        await Parse.Cloud.run(
          'mqttDisconnectDevice',
          {
            uuid: device.uuid
          },
          { useMasterKey: true }
        )

        debug(`Client (${client.id}) associated to Agent (${device.uuid}) id disconnected`)
      }
    } catch (err) {
      debug(`${chalk.red('[clientDisconnect Error]')} ${err.message}`)
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
            const payloadResult = await Parse.Cloud.run('mqttHandlePayload', { payload }, { useMasterKey: true })
            debug(`Device ${payloadResult.device.uuid}, stored ${payloadResult.stored}`)
          } catch (err) {
            debug(`${chalk.red('[publish agent/message Error]')} ${err.message}`)
            break
          }
        }
        break
    }
  })

  const listenDevicesConfigurations = async () => {
    const query = new Parse.Query('DeviceMessage')
    query.equalTo('topic', 'agent/configuration')
    const subscription = await query.subscribe()

    // suscribe to new agent/configuration message
    subscription.on('create', async (message) => {
      const { uuid } = message.toJSON()
      const clientKeys = await redis.keys('mqttjs*')
      clientKeys.forEach(async (clientId) => {
        // sercho for clients in redis db
        const result = await redis.get(clientId)
        const device = result ? JSON.parse(result) : {}
        if (device.uuid === uuid) {
          const client = aedes.clients[clientId]

          if (client) {
            // publish new configuration to client
            client.publish({
              topic: 'agent/configuration',
              payload: JSON.stringify({
                agent: {
                  uuid: message.get('uuid')
                },
                configurations: message.get('payload')
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

// Init servers
initServer()

function handleFatalError(err) {
  console.error(`${chalk.red('[fatal error]')} ${err.message}`)
  console.error(err.stack)
  process.exit(1)
}

process.on('uncaughtException', handleFatalError)
process.on('unhandledRejection', handleFatalError)
