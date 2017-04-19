
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
    const { queue } = data
    if (!streams[queue]) {
      streams[queue] = createSortingStream(queue)
      pump(
        streams[queue],
        createGate(queue),
        createWorkerStream(queue),
        function (err) {
          if (err) api.emit('error', err)
        }
      )
    }

    streams[queue].write(data)
    cb()
  })

  const work = pump(
    source,
    mainGate,
    splitter
  )

  function createGate (queue) {
    return through.obj(function (data, enc, cb) {
      if (gates.isOpen(queue)) return cb(null, data)

      gates.awaitOpen(queue).then(() => cb(null, data))
    })
  }

  function createSortingStream (queue) {
    const getCheckpoint = multiqueue.queue(queue).checkpoint()

    let checkpoint
    let sortStream

    // TODO:
    //
    // make this and createWorkerStream more efficient
    // currently it creates too many promises (at least one per item!)
    const ensureCheckpoint = through.obj({ highWaterMark: MAX_INT }, co(function* (data, enc, cb) {
      if (typeof checkpoint === 'undefined') {
        checkpoint = yield getCheckpoint
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

  function createWorkerStream (queue) {
    return through.obj({ highWaterMark: 0 }, co(function* (data, enc, cb) {
      const { key, value } = data
      try {
        if (!gates.isOpen(queue)) {
          yield gates.awaitOpen(queue)
        }

        const maybePromise = worker({ queue, value })
        if (isPromise(maybePromise)) yield maybePromise

        yield multiqueue.queue(queue).dequeue()
      } catch (err) {
        return cb(err)
      }

      api.emitAsync('processed', data)
      cb()
    }))
  }

  function start (queue) {
    gates.open(queue)
    return api
  }

  function pause (queue) {
    gates.close(queue)
    return api
  }

  function stop (queue) {
    pause(queue)
    if (!queue) work.end()
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
