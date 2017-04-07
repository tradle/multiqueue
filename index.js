
const crypto = require('crypto')
const Promise = require('bluebird')
const co = Promise.coroutine
const lexint = require('lexicographic-integer')
const collect = Promise.promisify(require('stream-collector'))
const clone = require('xtend')
const changesFeed = require('changes-feed')
const subdown = require('subleveldown')

module.exports = function createQueues ({ db }) {
  const { valueEncoding } = db.options
  Promise.promisifyAll(db)
  const queues = {}

  function getQueue (identifier) {
    if (!queues[identifier]) {
      queues[identifier] = createQueue(identifier)
    }

    return queues[identifier]
  }

  function createQueue (identifier) {
    const sub = subdown(db, identifier, { valueEncoding })
    const feed = Promise.promisifyAll(changesFeed(sub))
    const queue = queues[identifier] = {
      enqueue: feed.appendAsync.bind(feed),
      dequeue,
      createReadStream: createQueueStream
    }

    return queue

    function createQueueStream (opts) {
      opts = clone(opts)
      opts.gt = sub.db.prefix
      opts.lt = sub.db.prefix + '\xff'
      return createReadStream(opts)
    }
  }

  const enqueue = co(function* ({ message, from }) {
    assert(typeof from === 'string', 'expected string "from"')
    validateEncoding({ value: message, encoding: valueEncoding })
    yield getQueue(from).enqueue(message)
  })

  function dequeue ({ key }) {
    assert(typeof key === 'string', 'expected string "key"')
    return db.delAsync(key)
  }

  function createReadStream (opts={}) {
    return db.createReadStream(clone(opts, {
      keys: true,
      values: true
    }))
  }

  return {
    queue: getQueue,
    enqueue,
    dequeue,
    createReadStream
  }
}

function sha256 (data) {
  return crypto.createHash('sha256').update(data).digest('hex')
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
