'use strict'

function parsePayload (payload) {
  if (payload instanceof Buffer) {
    payload = payload.toString('utf8')
  }

  try {
    payload = JSON.parse(payload)
  } catch (e) {
    payload = null
  }

  return payload
}

const bufferToUTF8 = (buffer) => buffer ? buffer.toString('utf8') : undefined;

module.exports = {
  parsePayload,
  bufferToUTF8
}
