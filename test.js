require('any-promise/register/bluebird')

const test = require('tape')
const Promise = require('any-promise')
const coeval = require('co')
const co = require('co').wrap
const promisify = require('pify')
const collect = promisify(require('stream-collector'))
const memdb = require('memdb')
const through = require('through2')
const pump = require('pump')
const { PassThrough } = require('readable-stream')
const reorder = require('./sort-transform')
const { createMultiqueue, processMultiqueue, monitorMissing } = require('./')

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
      lane: 'bob',
      value
    })

    let queued = yield collect(multiqueue.queue('bob').createReadStream())
    t.same(values(queued), [value])
  }

  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })
  try {
    yield multiqueue.enqueue({
      lane: 'bob',
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
      lane: 'bob',
      value: { a: 1 },
    },
    {
      lane: 'bob',
      value: { b: 1 },
    },
    {
      lane: 'carol',
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

  const lanes = yield multiqueue.getLanes()
  t.same(lanes, ['bob', 'carol'])

  let toBob = yield collect(multiqueue.queue('bob').createReadStream())
  t.same(values(toBob), bodies.slice(0, 2))
  t.ok(toBob.every(obj => obj.lane === 'bob'))

  let queued = yield collect(multiqueue.createReadStream())
  t.same(values(queued), bodies)

  yield multiqueue.dequeue({ key: queued[0].key })
  queued = yield collect(multiqueue.createReadStream())
  t.same(values(queued), bodies.slice(1))

  toBob = yield collect(multiqueue.queue('bob').createReadStream())
  t.same(values(toBob), bodies.slice(1, 2))

  yield multiqueue.queue('bob').dequeue({ key: toBob[0].key })
  toBob = yield collect(multiqueue.queue('bob').createReadStream())
  t.same(values(toBob), [])

  t.end()
}))

test('live', function (t) {
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })
  const live = multiqueue.createReadStream({ live: true })
  live.on('data', co(function* (data) {
    live.end()
    t.equal(data.lane, 'bob')
    t.same(data.value, { a: 1 })
    yield multiqueue.dequeue(data)
    t.same(yield collect(multiqueue.createReadStream()), [])
    t.end()
  }))

  multiqueue.enqueue({
    lane: 'bob',
    value: { a: 1 }
  })
})

test('process', co(function* (t) {
  const n = 5
  const lanes = ['bob', 'carol', 'dave']
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })

  for (let i = 0; i < n; i++) {
    lanes.forEach(lane => {
      multiqueue.enqueue({
        lane,
        value: { count: i }
      })
    })
  }

  const concurrency = {}
  const running = {}
  const processor = processMultiqueue({ multiqueue, worker })

  let on
  let workerIterations = lanes.length * n
  let togglerInterval = setInterval(function toggleProcessing () {
    on = !on
    if (on) {
      processor.start()
    } else {
      processor.pause()
    }
  }, 10)

  function worker ({ lane, value }) {
    t.equal(on, true)

    running[lane] = true
    if (value.count === 1) {
      t.ok(lanes.every(lane => running[lane]), 'concurrency inter-lane')
    }

    if (!(lane in concurrency)) concurrency[lane] = 0

    t.equal(++concurrency[lane], 1, 'sequence intra-lane')

    return new Promise(resolve => {
      setTimeout(function () {
        concurrency[lane]--
        resolve()
        if (--workerIterations) return

        clearInterval(togglerInterval)
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
      lane: 'bob',
      value: { i },
      seq: i
    })
  }))

  let processed = 0
  const processor = processMultiqueue({ multiqueue, worker }).start()

  function worker ({ lane, value }) {
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
      lane: 'bob',
      value: { i }
    })
  }))

  const processor = processMultiqueue({ multiqueue, worker }).start()
  processor.on('error', t.ok)

  function worker ({ lane, value }) {
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
  t.plan(items.length + 2)

  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db, autoincrement: false })
  // multiqueue.on('missing', e => console.log('missing', e))
  // multiqueue.on('tip', tip => console.log('tip', tip))
  // multiqueue.on('have', have => console.log('have', have))

  // multiqueue.on('tip', function ({ lane, tip }) {
  //   console.log('tip', tip)
  // })

  yield Promise.all(items.map(i => {
    return multiqueue.enqueue({
      lane: 'bob',
      value: { i },
      seq: i
    })
  }))

  t.equal(yield multiqueue.queue('bob').tip(), 5)

  const multiqueue2 = createMultiqueue({ db, autoincrement: false })
  t.equal(yield multiqueue2.queue('bob').tip(), 5)

  processMultiqueue({ multiqueue, worker }).start()

  let i = 0
  function worker ({ lane, value }) {
    t.equal(value.i, i++)
  }
}))

test('custom seq tip', co(function* (t) {
  const items = [0, 1, 4]
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db, autoincrement: false })
  yield Promise.all(items.map(i => {
    return multiqueue.enqueue({
      lane: 'bob',
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
      lane: 'bob',
      value: { i },
      seq: i
    })
  })

  yield new Promise(resolve => monitor.once('batch', function ({ lane, missing }) {
    t.equal(lane, 'bob')
    t.same(missing, [0, 1, 2, 4])
    resolve()
  }))

  ;[2, 0, 1].forEach(i => {
    multiqueue.enqueue({
      lane: 'bob',
      value: { i },
      seq: i
    })
  })

  yield new Promise(resolve => monitor.once('batch', function ({ lane, missing }) {
    t.equal(lane, 'bob')
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
    lane: 'bob',
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
  function worker ({ lane, value }) {
    t.equal(value.i, i++)
  }
}))

function values (arr) {
  return arr.map(obj => obj.value)
}
