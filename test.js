require('any-promise/register/bluebird')

const test = require('tape')
const Promise = require('any-promise')
const coeval = require('co')
const co = require('co').wrap
const promisify = require('pify')
const collect = promisify(require('stream-collector'))
const memdb = require('memdb')
const memdown = require('memdown')
const levelup = require('levelup')
const through = require('through2')
const pump = require('pump')
const { PassThrough } = require('readable-stream')
const reorder = require('./sort-transform')
const { createMultiqueue, processMultiqueue, monitorMissing } = require('./')
const createGates = require('./gates')

test('gates', co(function* (t) {
  const gates = createGates()
  t.notOk(gates.isOpen())

  gates.open()
  t.ok(gates.isOpen())

  gates.close()
  t.notOk(gates.isOpen())
  t.notOk(gates.isOpen('a'))

  process.nextTick(() => gates.open('a'))
  yield gates.awaitOpen('a')
  t.ok(gates.isOpen('a'))
  t.notOk(gates.isOpen())

  process.nextTick(() => gates.open())
  yield gates.awaitOpen()
  t.ok(gates.isOpen('a'))
  t.ok(gates.isOpen())

  process.nextTick(() => gates.close('a'))
  yield gates.awaitClosed('a')
  t.notOk(gates.isOpen('a'))
  t.ok(gates.isOpen())

  t.end()
}))

test('encoding', co(function* (t) {
  const items = {
    json: { a: 1 },
    binary: new Buffer('{"a":1}'),
    utf8: '{"a":1}'
  }

  for (let valueEncoding in items) {
    let db = memdb({ valueEncoding })
    let multiqueue = createMultiqueue({ db })
    let value = items[valueEncoding]
    yield multiqueue.enqueue({
      queue: 'bob',
      value
    })

    let queued = yield collect(multiqueue.queue('bob').createReadStream())
    t.same(values(queued), [value])
  }

  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })
  try {
    yield multiqueue.enqueue({
      queue: 'bob',
      value: 'hey'
    })

    t.fail('accepted invalid encoding')
  } catch (err) {
    t.ok(err)
  }

  t.end()
}))

test('queue basics - enqueue, queued, tip, dequeue', co(function* (t) {
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })
  const objects = [
    {
      queue: 'bob',
      value: { a: 1 },
    },
    {
      queue: 'bob',
      value: { b: 1 },
    },
    {
      queue: 'carol',
      value: { c: 1 }
    }
  ]

  const bodies = objects.map(m => m.value)
  yield Promise.all([
    multiqueue.enqueue(objects[0]),
    multiqueue.enqueue(objects[1]),
    multiqueue.enqueue(objects[2])
  ])

  const tip = yield multiqueue.queue('bob').tip()
  t.equal(tip, 2)

  const queues = yield multiqueue.queues()
  t.same(queues, ['bob', 'carol'])

  let toBob = yield collect(multiqueue.createReadStream({ queue: 'bob' }))
  t.same(values(toBob), bodies.slice(0, 2))
  t.ok(toBob.every(obj => obj.queue === 'bob'))

  let queued = yield collect(multiqueue.createReadStream())
  t.same(values(queued), bodies)

  // open new multiqueue against same db
  const multiqueue2 = createMultiqueue({ db })
  t.equal(yield multiqueue2.queue('bob').tip(), 2)

  queued = yield collect(multiqueue2.createReadStream())
  t.same(values(queued), bodies)

  yield multiqueue2.dequeue({ queue: 'bob' })
  queued = yield collect(multiqueue2.createReadStream())
  t.same(values(queued), bodies.slice(1))

  toBob = yield collect(multiqueue2.createReadStream({ queue: 'bob' }))
  t.same(values(toBob), bodies.slice(1, 2))

  yield multiqueue2.dequeue({ queue: 'bob' })
  toBob = yield collect(multiqueue2.createReadStream({ queue: 'bob' }))
  t.same(values(toBob), [])

  t.end()
}))

test('queue interface', co(function* (t) {
  const items = genSequence(0, 3)
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })
  const bob = multiqueue.queue('queue')
  yield Promise.all(items.map(i => {
    return bob.enqueue({
      value: { i },
      seq: i
    })
  }))

  t.equal(yield bob.tip(), items.length)

  let toBob = yield collect(bob.createReadStream())
  t.same(values(toBob).map(val => val.i), items)

  yield bob.dequeue()

  toBob = yield collect(bob.createReadStream())
  t.same(values(toBob).map(val => val.i), items.slice(1))

  t.end()
}))

test('clear', co(function* (t) {
  yield Promise.all([true, false].map(co(function* (autoincrement) {
    const db = memdb({ valueEncoding: 'json' })
    const multiqueue = createMultiqueue({ db, autoincrement })
    const items = genSequence(0, 3)
    const preTip = multiqueue.firstSeq - 1
    const postTip = preTip + items.length
    // const bob = multiqueue.queue('bob')
    // const alice = multiqueue.queue('alice')
    const queues = ['alice', 'bob']
    yield Promise.all(queues.map(co(function* (queue) {
      yield Promise.all(items.map((n, i) => {
        return multiqueue.enqueue({
          queue,
          value: { i },
          seq: i
        })
      }))
    })))

    const alice = multiqueue.queue('alice')
    const bob = multiqueue.queue('bob')
    const toBob = yield collect(bob.createReadStream())
    yield bob.dequeue()

    t.equal(yield bob.tip(), postTip)
    t.equal(yield bob.checkpoint(), preTip + 1)

    yield bob.clear()
    t.equal(yield bob.tip(), preTip)
    t.equal(yield alice.tip(), postTip)
  })))

  t.end()
}))

test('live', function (t) {
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })
  const live = multiqueue.createReadStream({ live: true })
  live.on('data', co(function* (data) {
    live.end()
    t.equal(data.queue, 'bob')
    t.same(data.value, { a: 1 })
    yield multiqueue.dequeue(data)
    t.same(yield collect(multiqueue.createReadStream()), [])
    t.end()
  }))

  multiqueue.enqueue({
    queue: 'bob',
    value: { a: 1 }
  })
})

test('process', co(function* (t) {
  const n = 5
  const queues = ['bob', 'carol', 'dave']
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })

  for (let i = 0; i < n; i++) {
    queues.forEach(queue => {
      multiqueue.enqueue({
        queue,
        value: { count: i }
      })
    })
  }

  const concurrency = {}
  const running = {}
  const processor = processMultiqueue({ multiqueue, worker })
  processor.start()

  let workerIterations = queues.length * n

  const on = {}
  queues.forEach(queue => on[queue] = true)

  const togglerIntervals = queues.map(queue => setInterval(function toggleProcessing () {
    on[queue] = !on[queue]
    if (on[queue]) {
      processor.start(queue)
    } else {
      processor.pause(queue)
    }
  }, 10))

  function worker ({ queue, value }) {
    t.equal(on[queue], true)

    running[queue] = true
    if (value.count === 1) {
      t.ok(queues.every(queue => running[queue]), 'concurrency inter-queue')
    }

    if (!(queue in concurrency)) concurrency[queue] = 0

    t.equal(++concurrency[queue], 1, 'sequence intra-queue')

    return new Promise(resolve => {
      setTimeout(function () {
        concurrency[queue]--
        resolve()
        if (--workerIterations) return

        togglerIntervals.forEach(clearInterval)
        t.end()
      }, 100)
    })
  }
}))

test('processor.stop()', co(function* (t) {
  const items = [0, 1, 2, 3, 4]
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db, autoincrement: false })
  yield Promise.all(items.map(i => {
    return multiqueue.enqueue({
      queue: 'bob',
      value: { i },
      seq: i
    })
  }))

  let processed = 0
  const processor = processMultiqueue({ multiqueue, worker }).start()

  function worker ({ queue, value }) {
    t.equal(++processed, 1)
    processor.stop()
    setTimeout(t.end, 100)
  }
}))

test('bad worker stops processing', co(function* (t) {
  t.plan(1)

  const items = [0]
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })
  ;[0, 1, 2].forEach(co(function* (i) {
    yield multiqueue.enqueue({
      queue: 'bob',
      value: { i }
    })
  }))

  const processor = processMultiqueue({ multiqueue, worker }).start()
  processor.on('error', t.ok)

  function worker ({ queue, value }) {
    throw new Error('blah')
  }
}))

test('order', function (t) {
  const unordered = new PassThrough({ objectMode: true })
  ;[5, 2, 4, 0, 3, 1].forEach(i => {
    unordered.push(i)
  })

  unordered.push(null)

  let i = 0
  pump(unordered, reorder({
      getPosition: item => item,
      start: 0
    }))
    .on('data', data => t.equal(data, i++))
    .on('end', t.end)
})

test('custom seq', co(function* (t) {
  const items = [5, 2, 4, 0, 3, 1]
  const postTip = items.length - 1
  t.plan(items.length + 3)

  let db
  let multiqueue

  const reinit = co(function* () {
    if (db) {
      yield new Promise(resolve => db.close(resolve))
      reinitialized = true
      t.equal(yield multiqueue.queue('bob').tip(), postTip)
    }

    db = openDB()
    multiqueue = createMultiqueue({ db, autoincrement: false })
    processMultiqueue({ multiqueue, worker }).start()
  })

  yield reinit()
  yield Promise.all(items.map(i => {
    return multiqueue.enqueue({
      queue: 'bob',
      value: { i },
      seq: i
    })
  }))

  t.equal(yield multiqueue.queue('bob').tip(), postTip)

  let j = 0
  let reinitialized

  function worker ({ queue, value }) {
    t.equal(value.i, j++)
    if (!reinitialized && j === 2) {
      j-- // this current task will need to reprocessed
      reinit()
    }
  }

  function openDB () {
    return levelup('./customseq.db', { valueEncoding: 'json', db: memdown })
  }
}))

test('custom seq tip', co(function* (t) {
  const items = [0, 1, 4]
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db, autoincrement: false })
  yield Promise.all(items.map(i => {
    return multiqueue.enqueue({
      queue: 'bob',
      value: { i },
      seq: i
    })
  }))

  const multiqueue2 = createMultiqueue({ db, autoincrement: false })
  t.equal(yield multiqueue2.queue('bob').tip(), 1)
  t.end()
}))

test('monitor missing', co(function* (t) {
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db, autoincrement: false })
  const monitor = monitorMissing({ multiqueue, debounce: 50, unref: false })

  ;[5, 3].forEach(i => {
    multiqueue.enqueue({
      queue: 'bob',
      value: { i },
      seq: i
    })
  })

  yield new Promise(resolve => monitor.once('batch', function ({ queue, missing }) {
    t.equal(queue, 'bob')
    t.same(missing, [0, 1, 2, 4])
    resolve()
  }))

  ;[2, 0, 1].forEach(i => {
    multiqueue.enqueue({
      queue: 'bob',
      value: { i },
      seq: i
    })
  })

  yield new Promise(resolve => monitor.once('batch', function ({ queue, missing }) {
    t.equal(queue, 'bob')
    t.same(missing, [4])
    t.end()
  }))
}))

test('batch enqueue', co(function* (t) {
  const n = 5
  t.plan(n + 1)

  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db, autoincrement: false })
  yield multiqueue.batchEnqueue({
    queue: 'bob',
    data: new Array(n).fill(0).map((n, i) => {
      return {
        seq: i,
        value: { i }
      }
    })
  })

  const tip = yield multiqueue.queue('bob').tip()
  t.equal(tip, n - 1)

  processMultiqueue({ multiqueue, worker }).start()

  let i = 0
  function worker ({ queue, value }) {
    t.equal(value.i, i++)
  }
}))

function values (arr) {
  return arr.map(obj => obj.value)
}

function genSequence (first, last) {
  const arr = []
  for (let i = first; i <= last; i++) {
    arr.push(i)
  }

  return arr
}
