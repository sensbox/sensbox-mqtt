'use strict'

const debug = require('debug')('sensbox:mqtt')
const mosca = require('mosca')
const redis = require('redis')
const chalk = require('chalk')
const db = require('@sensbox/sensbox-db')
const request = require('request')

const { parsePayload } = require('./utils')

const _influx = {
  url: process.env.INFLUX_URL || 'http://localhost:8081'
}

const _influxUrl = `${_influx.url}/api`

const backend = {
  type: 'redis',
  redis,
  host: process.env.REDIS_HOST ||"localhost",
  db: process.env.REDIS_DB || 0,
  prefix: process.env.REDIS_CHANNEL ? `${process.env.REDIS_CHANNEL}` : 'default',
  return_buffers: true
}

const settings = {
  port: parseInt(process.env.PORT) || 1883,
  backend
}

const config = {
  database: process.env.DB_NAME || 'localhost',
  username: process.env.DB_USERNAME || 'user',
  password: process.env.DB_PASSWORD || 'pass',
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT) || 3306,
  dialect: 'mysql',
  // logging: s => debug(s)
  logging: false
}

async function init () {
  const services = await db(config).catch(handleFatalError)

  let Agente = services.Agente

  const server = new mosca.Server(settings)
  const clients = new Map()

  server.on('clientConnected', client => {
    debug(`Client Connected: ${client.id}`)
    clients.set(client.id, null)
  })

  server.on('clientDisconnected', async (client) => {
    debug(`Client Disconnected: ${client.id}`)
    const agent = clients.get(client.id)

    if (agent) {
      // Mark Agente as Disconnected
      agent.conectado = false

      try {
        await Agente.createOrUpdate(agent)
      } catch (e) {
        return handleError(e)
      }

      // Delete Agente from Clients List
      clients.delete(client.id)

      server.publish({
        topic: 'agent/disconnected',
        payload: JSON.stringify({
          agent: {
            uuid: agent.uuid
          }
        })
      })
      debug(`Client (${client.id}) associated to Agente (${agent.uuid}) marked as disconnected`)
    }
  })

  server.on('published', async (packet, client) => {
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
          payload.agent.conectado = true

          let agent = await Agente.findByUuid(payload.agent.uuid)

          if (!agent || !agent.activo) break

          try {
            agent = await Agente.createOrUpdate(payload.agent)
          } catch (e) {
            return handleError(e)
          }

          debug(`Agente ${JSON.stringify(agent)} saved`)

          // Notify Agent is Connected
          if (!clients.get(client.id)) {
            clients.set(client.id, agent.toJSON())
            server.publish({
              topic: 'agent/connected',
              payload: JSON.stringify({
                agent: {
                  uuid: agent.uuid,
                  hostname: agent.hostname,
                  conectado: agent.conectado
                }
              })
            })
          }

          let postMetrics = []
          // Store Metrics
          for (let metric of payload.metrics) {
            postMetrics.push({
              timestamp: metric.time,
              measurement: metric.type,
              tags: { host: agent.uuid },
              fields: { value: metric.value}
            })
          }

          if (postMetrics.length) {
            try {
              // POST request to influx db writing metrics.
              request.post({
                url: `${_influxUrl}/metrics`,
                json: true,
                body: postMetrics
              }, function done (err, httpResponse, body) {
                if (err || body.error) {
                  return debug('POST failed:', err || body.error)
                }
                debug('POST successful!  Server responded with:', body)
              })
            } catch (e) {
              return handleError(e)
            }
          }
        }
        break
    }
  })

  server.on('ready', async () => {
    console.log(`${chalk.green('[sensbox-mqtt]')} server is running on ${settings.port}`)
  })

  server.on('error', handleFatalError)
}

function handleFatalError (err) {
  console.error(`${chalk.red('[fatal error]')} ${err.message}`)
  console.error(err.stack)
  process.exit(1)
}

function handleError (err) {
  console.error(`${chalk.red('[error]')} ${err.message}`)
  console.error(err.stack)
}

process.on('uncaughtException', handleFatalError)
process.on('unhandledRejection', handleFatalError)

// Start Server!!!
init()
