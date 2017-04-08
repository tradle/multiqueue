
const { EventEmitter } = require('events')
const HAVE = 1
const MISSING = 0

module.exports = function monitorMissing ({ multiqueue, debounce=1000, unref }) {
  const state = {}
  const timeouts = {}
  const tips = {}

  multiqueue.on('enqueue', function ({ lane, seq, tip }) {
    if (!state[lane]) state[lane] = {}

    const laneState = state[lane]
    laneState[seq] = HAVE
    if (lane in tips) {
      for (let i = tips[lane]; i < tip; i++) {
        delete laneState[lane]
      }
    }

    tips[lane] = tip
    for (let i = tip + 1; i < seq; i++) {
      if (laneState[i] !== HAVE) {
        laneState[i] = MISSING
      }
    }

    update(lane)
  })

  const ee = new EventEmitter()
  ee.missing = function ({ lane }) {
    const laneState = state[lane] || {}
    return Object
      .keys(laneState)
      .filter(seq => laneState[seq] === MISSING)
      .map(Number)
      .sort(sortAscending)
  }

  function getLaneCache (lane) {
    if (!missing[lane]) missing[lane] = []

    return missing[lane]
  }

  function update (lane) {
    clearTimeout(timeouts[lane])
    const timeout = timeouts[lane] = setTimeout(() => {
      ee.emit('batch', {
        lane,
        missing: ee.missing({ lane })
      })
    }, debounce)

    if (unref && timeout.unref) timeout.unref()
  }

  return ee
}

function sortAscending (a, b) {
  return a - b
}
