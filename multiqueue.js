
const { EventEmitter } = require('events')
const Promise = require('bluebird')
const co = Promise.coroutine
const lexint = require('lexicographic-integer')
const collect = Promise.promisify(require('stream-collector'))
const clone = require('xtend')
const changesFeed = require('changes-feed')
const subdown = require('subleveldown')
const pump = require('pump')
const through = require('through2')
// const CombinedStream = require('combined-stream2')
const merge = require('merge2')
const SEPARATOR = '!'

module.exports = function createQueues ({ db, separator=SEPARATOR }) {
  const { valueEncoding } = db.options
  Promise.promisifyAll(db)
  const queues = {}
  const ee = new EventEmitter()

  function getQueue (identifier) {
    if (!queues[identifier]) {
      queues[identifier] = createQueue(identifier)
    }

    return queues[identifier]
  }

  function createQueue (lane) {
    const sub = subdown(db, lane, { valueEncoding, separator })
    const feed = Promise.promisifyAll(changesFeed(sub))
    const enqueue = co(function* (value) {
      const { change } = yield feed.appendAsync(value)
      const key = getLanePrefix(lane) + hexint(change)
      return { key, value, lane }
    })

    function createQueueStream (opts) {
      opts = clone(opts)
      opts.gt = queue.prefix
      opts.lt = queue.prefix + '\xff'
      return createReadStream(opts)
    }

    const queue = queues[lane] = {
      enqueue,
      dequeue,
      createReadStream: createQueueStream,
      get prefix () {
        return sub.db.prefix
      }
    }

    return queues[lane]
  }

  const enqueue = co(function* ({ value, lane }) {
    assert(typeof lane === 'string', 'expected string "lane"')
    if (lane.indexOf(separator) !== -1) {
      throw new Error('"lane" must not contain "separator"')
    }

    validateEncoding({ value, encoding: valueEncoding })
    const data = yield getQueue(lane).enqueue(value)
    ee.emit('enqueue', data)
  })

  function dequeue ({ key }) {
    assert(typeof key === 'string', 'expected string "key"')
    return db.delAsync(key)
  }

  function createReadStream (opts={}) {
    const old = pump(
      db.createReadStream(clone(opts, {
        keys: true,
        values: true
      })),
      through.obj(function (data, enc, cb) {
        data.lane = getLaneFromKey(data.key)
        cb(null, data)
      })
    )

    const merged = merge([old], { end: !opts.live })
    if (opts.live) {
      const live = createPassThrough()
      ee.on('enqueue', onEnqueue)
      merged.add(live)
      merged.on('queueDrain', () => {
        ee.removeListener('enqueue', onEnqueue)
      })

      function onEnqueue (data) {
        live.write(data)
      }
    }

    return merged
  }

  const getNextLane = co(function* (lane) {
    const opts = {
      values: false,
      limit: 1
    }

    if (lane) {
      opts.gt = getLanePrefix(lane) + '\xff'
    }

    const results = yield collect(db.createReadStream(opts))
    if (results.length) {
      return getLaneFromKey(results[0])
    }
  })

  const getLanes = co(function* () {
    const lanes = []
    let lane
    while (true) {
      lane = yield getNextLane(lane)
      if (!lane) break

      lanes.push(lane)
    }

    return lanes
  })

  function getLanePrefix (lane) {
    // BAD as it assumes knowledge of subleveldown internals
    // the less efficient but better way would be to either export the prefixer function from subleveldown
    // or use getQueue(lane).prefix instead
    return separator + lane + separator
  }

  function getLaneFromKey (key) {
    return key.split(separator)[1]
  }

  return {
    queue: getQueue,
    enqueue,
    dequeue,
    createReadStream,
    getLanes,
    getNextLane
  }
}

function assert (statement, err) {
  if (!statement) throw new Error(err || 'assertion failed')
}

function validateEncoding ({ value, encoding }) {
  if (encoding === 'binary') {
    assert(Buffer.isBuffer(value), 'expected Buffer')
  } else if (encoding === 'json') {
    assert(value && typeof value === 'object', 'expected object')
  } else if (encoding === 'string') {
    assert(typeof value === 'string', 'expected string')
  }
}

function createPassThrough () {
  return through.obj(function (data, enc, cb) {
    cb(null, data)
  })
}

function hexint (n) {
  return lexint.pack(n, 'hex')
}
