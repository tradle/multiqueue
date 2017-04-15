
const { EventEmitter } = require('events')
const HAVE = 1
const MISSING = 0

module.exports = function monitorMissing ({ multiqueue, debounce=1000, unref }) {
  const state = {}
  const timeouts = {}
  const tips = {}

  multiqueue.on('enqueue', function ({ queue, seq, tip }) {
    if (!state[queue]) state[queue] = {}

    const queueState = state[queue]
    queueState[seq] = HAVE
    if (queue in tips) {
      for (let i = tips[queue]; i <= tip; i++) {
        delete queueState[i]
      }
    }

    tips[queue] = tip
    for (let i = tip + 1; i < seq; i++) {
      if (queueState[i] !== HAVE) {
        queueState[i] = MISSING
      }
    }

    update(queue)
  })

  const ee = new EventEmitter()
  ee.missing = function ({ queue }) {
    const queueState = state[queue] || {}
    return Object
      .keys(queueState)
      .filter(seq => queueState[seq] === MISSING)
      .map(Number)
      .sort(sortAscending)
  }

  function update (queue) {
    clearTimeout(timeouts[queue])
    const timeout = timeouts[queue] = setTimeout(() => {
      const missing = ee.missing({ queue })
      if (missing.length) {
        ee.emit('batch', { queue, missing })
      }
    }, debounce)

    if (unref && timeout.unref) timeout.unref()
  }

  return ee
}

function sortAscending (a, b) {
  return a - b
}
