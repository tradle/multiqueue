
const { EventEmitter } = require('events')
const Promise = require('bluebird')
const co = Promise.coroutine
const pump = require('pump')
const through = require('through2')
const merge = require('merge2')
const duplexify = require('duplexify')
const extend = require('xtend/mutable')
const reorder = require('./sort-transform')
const { MAX_INT } = require('./utils')

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
      streams[lane] = createSortingStream(lane)
      pump(
        streams[lane],
        createWorkerStream(lane),
        function (err) {
          if (err) api.emit('error', err)
        }
      )
    }

    streams[lane].write(data)
    cb()
  })

  const work = pump(
    source,
    gate,
    splitter
  )

  function createSortingStream (lane) {
    const getCheckpoint = multiqueue.getLaneCheckpoint({ lane })

    let checkpoint
    let sortStream
    const ensureCheckpoint = through.obj({ highWaterMark: MAX_INT }, co(function* (data, enc, cb) {
      if (typeof checkpoint === 'undefined') {
        checkpoint = yield getCheckpoint
        if (typeof checkpoint === 'undefined') {
          checkpoint = multiqueue.autoincrement ? 1 : 0
        }

        sortStream = reorder({
          getPosition: data => data.seq,
          start: checkpoint
        })

        duplex.setReadable(sortStream)
      }

      sortStream.write(data)
      cb(null, data)
    }))

    const duplex = duplexify(null, null, { objectMode: true })
    duplex.setWritable(ensureCheckpoint)
    return duplex
  }

  function createWorkerStream (lane) {
    return through.obj({ highWaterMark: 0 }, co(function* (data, enc, cb) {
      const { key, value } = data
      try {
        const maybePromise = worker({ lane, value })
        if (isPromise(maybePromise)) yield maybePromise

        yield multiqueue.queue(lane).dequeue({ key })
      } catch (err) {
        return cb(err)
      }

      cb()
    }))
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

  const api = new EventEmitter()
  extend(api, { start, pause, stop })
  return api
}

function isPromise (obj) {
  return obj && typeof obj.then === 'function'
}