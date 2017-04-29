
const Promise = require('any-promise')
const co = require('co').wrap
const promisify = require('pify')
const collect = promisify(require('stream-collector'))
const clone = require('xtend')
const subdown = require('subleveldown')
const prefixer = require('sublevel-prefixer')
const pump = require('pump')
const through = require('through2')
const extend = require('xtend/mutable')
// const CombinedStream = require('combined-stream2')
const merge = require('merge2')
const omit = require('object.omit')
const AsyncEmitter = require('./async-emitter')
const implAutoincrement = require('./impl-autoincrement')
const implCustomSeq = require('./impl-custom-seq')
const {
  hexint,
  unhexint,
  createPassThrough,
  assert,
  validateEncoding,
  firstInStream,
  getSublevelPrefix,
  createKeyParserTransform,
  pluckValuesTransform
} = require('./utils')

const SEPARATOR = '!'
const MIN_CHAR = '\x00'
const MAX_CHAR = '\xff'
const NAMESPACE = {
  main: 'm',
  checkpoint: 'c'
}

const FIRST_SEQ = 0

module.exports = function createQueues ({ db, separator=SEPARATOR, autoincrement=true }) {
  const { valueEncoding } = db.options
  const prefix = prefixer(separator)
  const mainDB = subdown(db, NAMESPACE.main, { valueEncoding, separator })
  const checkpointsDB = subdown(db, NAMESPACE.checkpoint, { valueEncoding: 'json' })
  const batchAsync = promisify(db.batch.bind(db))
  const delCheckpointAsync = promisify(checkpointsDB.del.bind(checkpointsDB))
  const putCheckpointAsync = promisify(checkpointsDB.put.bind(checkpointsDB))
  const queues = {}
  const ee = new AsyncEmitter()
  const tips = {}
  const have = {}
  const keyParser = createKeyParserTransform(parseKey)

  function markHave ({ queue, seq }) {
    if (!have[queue]) have[queue] = {}

    have[queue][seq] = true
  }

  function clearHave ({ queue, seq }) {
    if (have[queue] && have[queue][seq]) {
      delete have[queue][seq]
      return true
    }
  }

  function getQueueKeyRange ({ queue }) {
    const gt = getQueuePrefix(queue)
    const lt = gt + MAX_CHAR
    return { gt, lt }
  }

  const impl = (autoincrement ? implAutoincrement : implCustomSeq)({ createQueueStream })

  function getQueue (identifier) {
    if (!queues[identifier]) {
      queues[identifier] = createQueue(identifier)
    }

    return queues[identifier]
  }

  /**
   * the "tip" is the seq of the latest queued item excluding gaps
   * e.g. if [0, 1, 2, 5, 6] are queued, the tip is 2
   */
  const getQueueTip = co(function* ({ queue }) {
    let tip = tips[queue]
    if (typeof tip !== 'undefined') {
      return tip
    }

    // the non-autoincrement implementation needs to know
    // the seq of the last dequeued item
    tip = yield impl.tip({
      queue,
      getCheckpoint: () => getQueueCheckpoint({ queue })
    })

    tip = typeof tip === 'undefined' ? FIRST_SEQ - 1 : tip
    return tips[queue] = tip
  })

  const clearQueue = co(function* ({ queue }) {
    yield Promise.all([
      yield collect(pump(
        mainDB.createReadStream(extend({
          values: false
        }, getQueueKeyRange({ queue }))),
        through.obj(function (key, enc, cb) {
          mainDB.del(key, cb)
        })
      )),
      delCheckpointAsync(queue)
    ])

    delete tips[queue]
  })

  function createQueue (queue) {
    const sub = subdown(mainDB, queue, { valueEncoding, separator })
    const batchEnqueueInternal = impl.batchEnqueuer({ db: sub, queue })
    const promiseTip = getQueueTip({ queue })

    let tip
    const updateTip = co(function* ({ seq }) {
      if (typeof tip === 'undefined') tip = yield promiseTip

      let newTip = tip
      if (tips[queue] + 1 === seq) {
        clearHave({ queue, seq })
        newTip = seq
      } else {
        markHave({ queue, seq })
      }

      while (clearHave({ queue, seq: newTip + 1 })) {
        newTip++
      }

      if (newTip !== tip) {
        tip = tips[queue] = newTip
        ee.emitAsync('tip', { queue, tip })
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
      if (seqs[0] === FIRST_SEQ) {
        yield putCheckpointAsync(queue, FIRST_SEQ - 1)
      }

      let tip
      for (let seq of seqs) {
        tip = yield updateTip({ seq })
      }

      return data.map((item, i) => {
        const { value } = item
        const seq = seqs[i]
        const key = getKey({ queue, seq })
        const ret = { key, value, queue, tip, seq }
        ee.emitAsync('enqueue', ret)
        return ret
      })
    })

    return {
      queued: () => getQueued(queue),
      enqueue,
      dequeue: () => dequeue({ queue }),
      batchEnqueue,
      createReadStream: createQueueStream.bind(null, queue),
      tip: () => getQueueTip({ queue }),
      clear: () => clearQueue({ queue }),
      checkpoint: () => getQueueCheckpoint({ queue })
    }
  }

  function createQueueStream (queue, opts) {
    opts = extend(getQueueKeyRange({ queue }), opts)
    return createReadStream(opts)
  }

  function validateQueueName (queue) {
    assert(typeof queue === 'string', 'expected string "queue"')
    if (queue.indexOf(separator) !== -1) {
      throw new Error('"queue" must not contain "separator"')
    }
  }

  const enqueue = co(function* ({ value, queue, seq }) {
    validateQueueName(queue)
    if (!autoincrement) {
      assert(typeof seq === 'number', 'expected "seq"')
    }

    validateEncoding({ value, encoding: valueEncoding })
    yield getQueue(queue).enqueue({ value, seq })
  })

  const batchEnqueue = co(function* ({ queue, data }) {
    validateQueueName(queue)
    if (!autoincrement) {
      assert(data.every(data => typeof data.seq === 'number'), 'expected every item to have a "seq"')
    }

    data.forEach(data => validateEncoding({ value: data.value, encoding: valueEncoding }))
    yield getQueue(queue).batchEnqueue({ queue, data })
  })

  const dequeue = co(function* ({ queue }) {
    assert(typeof queue === 'string', 'expected string "queue"')
    const checkpoint = yield getQueueCheckpoint({ queue })
    const seq = typeof checkpoint === 'undefined' ? FIRST_SEQ : checkpoint + 1
    const batch = [
      { type: 'del', key: getKey({ queue, seq }) },
      { type: 'put', key: getCheckpointKey(queue), value: seq }
    ]

    yield batchAsync(batch)
    ee.emitAsync('dequeue', { queue, seq })
  })

  /**
   * Get the seq of the last dequeued item
   */
  function getQueueCheckpoint ({ queue }) {
    return new Promise(resolve => {
      checkpointsDB.get(queue, function (err, result) {
        resolve(err ? FIRST_SEQ - 1 : result)
      })
    })
  }

  function createReadStream (opts={}) {
    if (opts.queue) {
      return createQueueStream(opts.queue, omit(opts, 'queue'))
    }

    const old = mainDB.createReadStream(extend({
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

  function getQueues () {
    return collect(checkpointsDB.createReadStream({ values: false }))
  }

  function getCheckpointKey (queue) {
    return prefix(NAMESPACE.checkpoint, queue)
  }

  function getKey ({ queue, seq }) {
    // BAD as it assumes knowledge of changes-feed internals
    return prefix(NAMESPACE.main, prefix(queue, hexint(seq)))
  }

  function getQueuePrefix (queue) {
    return prefix(queue, '')
  }

  function parseKey (key) {
    const parts = key.split(separator)
    const seq = unhexint(parts.pop())
    const queue = parts.pop()
    return { queue, seq }
  }

  function getQueued (queue, opts) {
    if (typeof queue === 'object') {
      opts = queue
      queue = null
    }

    const source = queue ? createQueueStream(queue, opts) : createReadStream(opts)
    return collect(source)
  }

  return extend(ee, {
    firstSeq: FIRST_SEQ,
    autoincrement,
    queue: getQueue,
    batchEnqueue,
    enqueue,
    dequeue,
    createReadStream,
    queues: getQueues,
    checkpoint: getQueueCheckpoint,
    queued: getQueued
  })
}

function sortAscendingBySeq (a, b) {
  return a.seq - b.seq
}
