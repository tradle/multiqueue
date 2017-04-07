
const { EventEmitter } = require('events')
const Promise = require('bluebird')
const co = Promise.coroutine
const pump = require('pump')
const through = require('through2')

module.exports = function processMultiqueue ({ multiqueue, worker }) {
  const streams = {}
  const source = multiqueue.createReadStream({ live: true })

  let on
  const ee = new EventEmitter()
  ee.on('on', () => on = true)
  ee.on('off', () => on = false)

  const gate = through.obj(function (data, enc, cb) {
    if (on) return cb(null, data)

    ee.once('on', () => cb(null, data))
  })

  const splitter = through.obj(function (data, enc, cb) {
    const { lane } = data
    if (!streams[lane]) {
      streams[lane] = createWorkerStream(lane)
    }

    streams[lane].write(data)
    cb()
  })

  const work = pump(
    source,
    gate,
    splitter
  )

  function createWorkerStream (lane) {
    const transform = co(function* (data, enc, cb) {
      const { key, value } = data
      try {
        const maybePromise = worker({ lane, value })
        if (isPromise(maybePromise)) yield maybePromise

        yield multiqueue.queue(lane).dequeue({ key })
      } catch (err) {
        return cb(err)
      }

      cb()
    })

    return through.obj({ highWaterMark: 1 }, transform)
  }

  function start () {
    ee.emit('on')
    return api
  }

  function pause () {
    ee.emit('off')
    return api
  }

  function stop () {
    work.end()
    return api
  }

  const api = { start, pause, stop }
  return api
}

function isPromise (obj) {
  return obj && typeof obj.then === 'function'
}
