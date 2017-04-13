
const { EventEmitter } = require('events')
const Promise = require('any-promise')
const co = require('co').wrap
const pump = require('pump')
const through = require('through2')
const merge = require('merge2')
const duplexify = require('duplexify')
const extend = require('xtend/mutable')
const reorder = require('./sort-transform')
const { MAX_INT } = require('./utils')
const AsyncEmitter = require('./async-emitter')
const createGates = require('./gates')

module.exports = function processMultiqueue ({ multiqueue, worker }) {
  const streams = {}
  const source = multiqueue.createReadStream({ live: true })
  const gates = createGates()
  const mainGate = createGate()
  const splitter = through.obj(function (data, enc, cb) {
    const { lane } = data
    if (!streams[lane]) {
      streams[lane] = createSortingStream(lane)
      pump(
        streams[lane],
        createGate(lane),
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
    mainGate,
    splitter
  )

  function createGate (lane) {
    return through.obj(function (data, enc, cb) {
      if (gates.isOpen(lane)) return cb(null, data)

      gates.awaitOpen(lane).then(() => cb(null, data))
    })
  }

  function createSortingStream (lane) {
    const getCheckpoint = multiqueue.getLaneCheckpoint({ lane })

    let checkpoint
    let sortStream

    // TODO:
    //
    // make this and createWorkerStream more efficient
    // currently it creates too many promises (at least one per item!)
    const ensureCheckpoint = through.obj({ highWaterMark: MAX_INT }, co(function* (data, enc, cb) {
      if (typeof checkpoint === 'undefined') {
        checkpoint = yield getCheckpoint
        if (typeof checkpoint === 'undefined') {
          // autoincrement: changes-feed starts at 1
          // normally we start at 0
          // checkpoint is seq of the last processed item
          checkpoint = multiqueue.autoincrement ? 0 : -1
        }

        sortStream = reorder({
          getPosition: data => data.seq,
          start: checkpoint + 1
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
        if (!gates.isOpen(lane)) {
          yield gates.awaitOpen(lane)
        }

        const maybePromise = worker({ lane, value })
        if (isPromise(maybePromise)) yield maybePromise

        yield multiqueue.queue(lane).dequeue({ key })
      } catch (err) {
        return cb(err)
      }

      api.emitAsync('processed', data)
      cb()
    }))
  }

  function start (lane) {
    gates.open(lane)
    return api
  }

  function pause (lane) {
    gates.close(lane)
    return api
  }

  function stop (lane) {
    pause(lane)
    if (!lane) work.end()
    return api
  }

  const api = new AsyncEmitter()
  extend(api, {
    start,
    resume: start,
    pause,
    stop
  })

  return api
}

function isPromise (obj) {
  return obj && typeof obj.then === 'function'
}
