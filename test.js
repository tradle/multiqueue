
const test = require('tape')
const Promise = require('bluebird')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const memdb = require('memdb')
const createQueues = require('./')

test('basic', co(function* (t) {
  const db = memdb({ valueEncoding: 'json' })
  const queues = createQueues({ db })

  const messages = [
    {
      from: 'bob',
      message: { a: 1 },
    },
    {
      from: 'bob',
      message: { b: 1 },
    },
    {
      from: 'carol',
      message: { c: 1 }
    }
  ]

  const bodies = messages.map(m => m.message)
  yield Promise.all([
    queues.enqueue(messages[0]),
    queues.enqueue(messages[1]),
    queues.enqueue(messages[2])
  ])

  let toBob = yield collect(queues.queue('bob').createReadStream())
  t.same(values(toBob), bodies.slice(0, 2))

  let queued = yield collect(queues.createReadStream())
  t.same(values(queued), bodies)

  yield queues.dequeue({ key: queued[0].key })
  queued = yield collect(queues.createReadStream())
  t.same(values(queued), bodies.slice(1))

  toBob = yield collect(queues.queue('bob').createReadStream())
  t.same(values(toBob), bodies.slice(1, 2))

  yield queues.queue('bob').dequeue({ key: toBob[0].key })
  toBob = yield collect(queues.queue('bob').createReadStream())
  t.same(values(toBob), [])

  t.end()
}))

function values (arr) {
  return arr.map(obj => obj.value)
}
