
const test = require('tape')
const Promise = require('bluebird')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const memdb = require('memdb')
const through = require('through2')
const pump = require('pump')
const { PassThrough } = require('readable-stream')
const reorder = require('./sort-transform')
const { createMultiqueue, processMultiqueue } = require('./')

test('basic', co(function* (t) {
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
  t.plan(9)

  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db })
  const lanes = ['bob', 'carol', 'dave']

  for (let i = 0; i < 2; i++) {
    lanes.forEach(lane => {
      multiqueue.enqueue({
        lane,
        value: { count: i }
      })
    })
  }

  const concurrency = {}
  const running = {}
  processMultiqueue({ multiqueue, worker }).start()

  function worker ({ lane, value }) {
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
      }, 100)
    })
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
  t.plan(items.length)

  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db, autoincrement: false })

  items.forEach(i => {
    multiqueue.enqueue({
      lane: 'bob',
      value: { i },
      seq: i
    })
  })

  processMultiqueue({ multiqueue, worker }).start()

  let i = 0
  function worker ({ lane, value }) {
    t.equal(value.i, i++)
  }
}))

function values (arr) {
  return arr.map(obj => obj.value)
}
