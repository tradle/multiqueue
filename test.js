
const test = require('tape')
const Promise = require('any-promise')
const co = require('co').wrap
const promisify = require('pify')
const collect = promisify(require('stream-collector'))
const memdb = require('memdb')
const through = require('through2')
const pump = require('pump')
const { PassThrough } = require('readable-stream')
const reorder = require('./sort-transform')
const { createMultiqueue, processMultiqueue, monitorMissing } = require('./')

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
  t.plan(items.length + 1)

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

  const tip = yield multiqueue.queue('bob').tip()
  t.equal(tip, 5)

  processMultiqueue({ multiqueue, worker }).start()

  let i = 0
  function worker ({ lane, value }) {
    t.equal(value.i, i++)
  }
}))

test('monitor missing', function (t) {
  const db = memdb({ valueEncoding: 'json' })
  const multiqueue = createMultiqueue({ db, autoincrement: false })
  const monitor = monitorMissing({ multiqueue, debounce: 50, unref: false })

  ;[5, 2].forEach(i => {
    multiqueue.enqueue({
      lane: 'bob',
      value: { i },
      seq: i
    })
  })

  monitor.on('batch', function ({ lane, missing }) {
    t.equal(lane, 'bob')
    t.same(missing, [0, 1, 3, 4])
    t.end()
  })
})

function values (arr) {
  return arr.map(obj => obj.value)
}
