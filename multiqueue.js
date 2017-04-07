
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

module.exports = function createQueues ({
  db,
  separator=SEPARATOR,
  autoincrement=true
}) {
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

  function getAutoincrementEnqueue ({ db, lane }) {
    const feed = Promise.promisifyAll(changesFeed(db))
    return co(function* ({ value }) {
      const { change } = yield feed.appendAsync(value)
      return getKey({ lane, seq: change })
    })
  }

  function getManualIncrementEnqueue ({ db, lane }) {
    return co(function* ({ value, seq }) {
      const key = getKey({ lane, seq })
      yield db.putAsync(key, value)
      return key
    })
  }

  function getInternalQueueAPI ({ db, lane }) {
    const sub = subdown(db, lane, { valueEncoding, separator })
    Promise.promisifyAll(sub)

    const opts = { db: sub, lane }
    return {
      get prefix () {
        return sub.db.prefix
      },
      enqueue: autoincrement ? getAutoincrementEnqueue(opts) : getManualIncrementEnqueue(opts),
      checkpoint: lane => getLaneCheckpoint({ lane })
    }
  }

  function getKey ({ lane, seq }) {
    return getLanePrefix(lane) + hexint(seq)
  }

  function createQueue (lane) {
    const api = getInternalQueueAPI({ db, lane })
    const enqueue = co(function* ({ value, seq }) {
      const key = yield api.enqueue({ value, seq })
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
        return api.prefix
      }
    }

    return queues[lane]
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
    const old = db.createReadStream(clone(opts, {
      keys: true,
      values: true,
      gt: separator
    }))

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

    return pump(
      merged,
      through.obj(function (data, enc, cb) {
        extend(data, parseKey(data.key))
        cb(null, data)
      })
    )
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
    const [ignore, lane, seq] = key.split(separator)
    return {
      lane,
      seq: unhexint(seq)
    }
  }

  return Object.freeze({
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
