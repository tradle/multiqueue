
const { EventEmitter } = require('events')
const Promise = require('bluebird')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const clone = require('xtend')
const changesFeed = require('changes-feed')
const subdown = require('subleveldown')
const pump = require('pump')
const through = require('through2')
const extend = require('xtend/mutable')
// const CombinedStream = require('combined-stream2')
const merge = require('merge2')
const {
  hexint,
  unhexint,
  createPassThrough,
  assert,
  validateEncoding,
  firstInStream
} = require('./utils')

const SEPARATOR = '!'
const LANE_CHECKPOINT_PREFIX = '\x00'

module.exports = function createQueues ({ db, separator=SEPARATOR, autoincrement=true }) {
  const { valueEncoding } = db.options
  Promise.promisifyAll(db)
  const queues = {}
  const ee = new EventEmitter()
  const tips = {}

  const implAutoincrement = {
    tip: function ({ lane }) {
      // autoincrement is always in order
      return firstInStream(createQueueStream(lane, {
        reverse: true,
        limit: 1
      }))
    },
    enqueuer: function ({ db, lane }) {
      const feed = Promise.promisifyAll(changesFeed(db))
      return co(function* ({ value }) {
        const { change } = yield feed.appendAsync(value)
        return change
      })
    }
  }

  function getQueueKeyRange ({ lane }) {
    const prefix = getLanePrefix(lane)
    return {
      gt: prefix,
      lt: prefix + '\xff'
    }
  }

  const implCustom = {
    tip: function ({ lane }) {
      let prev = -1
      return new Promise(resolve => {
        const stream = createQueueStream(lane, { values: false })
          .on('data', key => {
            const { seq } = parseKey(key)
            if (prev && seq > prev + 1) {
              // we hit a gap
              resolve(prev)
              stream.destroy()
            }

            prev = seq
          })
          .on('end', () => resolve(prev))
      })
    },
    enqueuer: function ({ db, lane }) {
      return co(function* ({ value, seq }) {
        yield db.putAsync(hexint(seq), value)
        return seq
      })
    }
  }

  const impl = autoincrement ? implAutoincrement : implCustom

  function getQueue (identifier) {
    if (!queues[identifier]) {
      queues[identifier] = createQueue(identifier)
    }

    return queues[identifier]
  }

  function getInternalQueueAPI ({ db, lane }) {
    const sub = subdown(db, lane, { valueEncoding, separator })
    Promise.promisifyAll(sub)

    const opts = { db: sub, lane }
    return {
      get prefix () {
        return sub.db.prefix
      },
      enqueue: impl.enqueuer(opts),
      checkpoint: lane => getLaneCheckpoint({ lane }),
      tip: impl.tip,
      // createReadStream: function (opts) {
      //   return pump(
      //     sub.createReadStream(opts),
      //     keyParser(opts)
      //   )
      // }
    }
  }

  const getTip = co(function* ({ lane }) {
    let tip = tips[lane]
    if (typeof tip !== 'undefined') {
      return tip
    }

    tip = tips[lane] = yield impl.tip({ lane })
    return tip
  })

  function getKey ({ lane, seq }) {
    return getLanePrefix(lane) + hexint(seq)
  }

  function createQueue (lane) {
    const internal = getInternalQueueAPI({ db, lane })
    const promiseTip = getTip({ lane })
    let tip

    const enqueue = co(function* ({ value, seq }) {
      if (!tip) tip = yield promiseTip

      seq = yield internal.enqueue({ value, seq })
      if (tips[lane] + 1 === seq) {
        tip = tips[lane] = seq
      }

      const key = getKey({ lane, seq })
      return { key, value, lane, tip, seq }
    })

    const queue = queues[lane] = {
      enqueue,
      dequeue,
      createReadStream: createQueueStream.bind(null, lane),
      get prefix () {
        return getLanePrefix(lane)
      },
      tip: internal.tip
    }

    return queues[lane]
  }

  function createQueueStream (lane, opts) {
    opts = extend(getQueueKeyRange({ lane }), opts)
    return createReadStream(opts)
  }

  const enqueue = co(function* ({ value, lane, seq }) {
    assert(typeof lane === 'string', 'expected string "lane"')
    if (!autoincrement) {
      assert(typeof seq === 'number', 'expected "seq"')
    }

    if (lane.indexOf(separator) !== -1) {
      throw new Error('"lane" must not contain "separator"')
    }

    validateEncoding({ value, encoding: valueEncoding })
    const data = yield getQueue(lane).enqueue({ value, seq })
    ee.emit('enqueue', data)
  })

  function dequeue ({ key }) {
    assert(typeof key === 'string', 'expected string "key"')
    const { lane, seq } = parseKey(key)
    const batch = [
      { type: 'del', key },
      { type: 'put', key: LANE_CHECKPOINT_PREFIX + lane, value: seq }
    ]

    return db.batchAsync(batch)
  }

  function getLaneCheckpoint ({ lane }) {
    return firstInStream(db.createReadStream({
      limit: 1,
      keys: false,
      start: LANE_CHECKPOINT_PREFIX + lane,
      end: LANE_CHECKPOINT_PREFIX + lane + '\xff'
    }))
  }

  function createReadStream (opts={}) {
    const old = db.createReadStream(extend({
      keys: true,
      values: true,
      gt: separator
    }, opts))

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

    return pump(merged, keyParser(opts))
  }

  function keyParser (opts) {
    return through.obj(function (data, enc, cb) {
      if (opts.keys !== false) {
        if (opts.values === false) {
          return cb(null, parseKey(data))
        }

        extend(data, parseKey(data.key))
      }

      cb(null, data)
    })
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
      return parseKey(results[0]).lane
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

  function parseKey (key) {
    // if (key.slice(0, separator.length) !== separator) {
    //   return { seq: unhexint(key) }
    // }

    const [ignore, lane, seq] = key.split(separator)
    return {
      lane,
      seq: unhexint(seq)
    }
  }

  return extend(ee, {
    autoincrement,
    queue: getQueue,
    enqueue,
    dequeue,
    createReadStream,
    getLanes,
    getNextLane,
    getLaneCheckpoint
  })
}
