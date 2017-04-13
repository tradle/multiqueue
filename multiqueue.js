
const Promise = require('any-promise')
const co = require('co').wrap
const promisify = require('pify')
const collect = promisify(require('stream-collector'))
const clone = require('xtend')
const changesFeed = require('changes-feed')
const subdown = require('subleveldown')
const pump = require('pump')
const through = require('through2')
const extend = require('xtend/mutable')
// const CombinedStream = require('combined-stream2')
const merge = require('merge2')
const AsyncEmitter = require('./async-emitter')
const implAutoincrement = require('./impl-autoincrement')
const implCustomSeq = require('./impl-custom-seq')
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
  const batchAsync = promisify(db.batch.bind(db))
  const queues = {}
  const ee = new AsyncEmitter()
  const tips = {}
  const have = {}

  function markHave ({ lane, seq }) {
    if (!have[lane]) have[lane] = {}

    have[lane][seq] = true
  }

  function clearHave ({ lane, seq }) {
    if (have[lane]) {
      if (have[lane][seq]) {
        delete have[lane][seq]
        return true
      }
    }
  }

  function getQueueKeyRange ({ lane }) {
    const prefix = getLanePrefix(lane)
    return {
      gt: prefix,
      lt: prefix + '\xff'
    }
  }

  const impl = (autoincrement ? implAutoincrement : implCustomSeq)({ createQueueStream })

  function getQueue (identifier) {
    if (!queues[identifier]) {
      queues[identifier] = createQueue(identifier)
    }

    return queues[identifier]
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
    const sub = subdown(db, lane, { valueEncoding, separator })
    const batchEnqueueInternal = impl.batchEnqueuer({ db: sub, lane })
    const promiseTip = getTip({ lane })

    let tip
    const updateTip = co(function* ({ seq }) {
      if (typeof tip === 'undefined') tip = yield promiseTip

      let newTip = tip
      if (tips[lane] + 1 === seq) {
        clearHave({ lane, seq })
        newTip = seq
      } else {
        markHave({ lane, seq })
      }

      while (clearHave({ lane, seq: newTip + 1 })) {
        newTip++
      }

      if (newTip !== tip) {
        tip = tips[lane] = newTip
        ee.emitAsync('tip', { lane, tip })
      }

      return tip
    })

    const enqueue = co(function* ({ value, seq }) {
      const results = yield batchEnqueue({
        data: [{ value, seq }]
      })

      return results[0]
    })

    const batchEnqueue = co(function* ({ data }) {
      data = data.slice()
      if (!autoincrement) {
        data.sort(sortAscendingBySeq)
      }

      const seqs = yield batchEnqueueInternal({ data })
      let tip
      for (let seq of seqs) {
        tip = yield updateTip({ seq })
      }

      return data.map((item, i) => {
        const { value } = item
        const seq = seqs[i]
        const key = getKey({ lane, seq })
        return { key, value, lane, tip, seq }
      })
    })

    const queue = queues[lane] = {
      enqueue,
      dequeue,
      batchEnqueue,
      createReadStream: createQueueStream.bind(null, lane),
      tip: () => getTip({ lane })
    }

    return queues[lane]
  }

  function createQueueStream (lane, opts) {
    opts = extend(getQueueKeyRange({ lane }), opts)
    return createReadStream(opts)
  }

  function validateLaneName (lane) {
    assert(typeof lane === 'string', 'expected string "lane"')
    if (lane.indexOf(separator) !== -1) {
      throw new Error('"lane" must not contain "separator"')
    }
  }

  const enqueue = co(function* ({ value, lane, seq }) {
    validateLaneName(lane)
    if (!autoincrement) {
      assert(typeof seq === 'number', 'expected "seq"')
    }

    validateEncoding({ value, encoding: valueEncoding })
    const data = yield getQueue(lane).enqueue({ value, seq })
    ee.emitAsync('enqueue', data)
  })

  const batchEnqueue = co(function* ({ lane, data }) {
    validateLaneName(lane)
    if (!autoincrement) {
      assert(data.every(data => typeof data.seq === 'number'), 'expected every item to have a "seq"')
    }

    data.forEach(data => validateEncoding({ value: data.value, encoding: valueEncoding }))
    const results = yield getQueue(lane).batchEnqueue({ lane, data })
    results.forEach(item => ee.emitAsync('enqueue', item))
  })

  const dequeue = co(function* ({ key }) {
    assert(typeof key === 'string', 'expected string "key"')
    const { lane, seq } = parseKey(key)
    const batch = [
      { type: 'del', key },
      { type: 'put', key: LANE_CHECKPOINT_PREFIX + lane, value: seq }
    ]

    yield batchAsync(batch)
    ee.emitAsync('dequeue', { lane, seq })
  })

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

    const merged = merge(old, { end: !opts.live })
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

    const parts = key.split(separator)
    const seq = unhexint(parts.pop())
    const lane = parts.pop()
    return { lane, seq }
  }

  return extend(ee, {
    autoincrement,
    queue: getQueue,
    lane: getQueue,
    batchEnqueue,
    enqueue,
    dequeue,
    createReadStream,
    getLanes,
    getNextLane,
    getLaneCheckpoint
  })
}

function sortAscendingBySeq (a, b) {
  return a.seq - b.seq
}
