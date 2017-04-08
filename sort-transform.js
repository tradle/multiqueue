const debug = require('debug')('multiqueue:sort')
const through = require('through2')
const { assert, MAX_INT } = require('./utils')

module.exports = function enforceOrder ({ start=0, getPosition }) {
  assert(typeof getPosition === 'function', 'expected function "getPosition"')

  let last
  const buffer = new Map()

  return through.obj({ highWaterMark: MAX_INT }, function (data, enc, cb) {
    let seq = getPosition(data)
    const expected = typeof last === 'undefined' ? start : last + 1
    if (seq === expected) {
      last = seq
      debug(`allowing item: ${seq}`)
      this.push(data)
      let item
      while (item = buffer.get(++seq)) {
        debug(`pushing delayed item: ${seq}`)
        buffer.delete(seq)
        last = seq
        this.push(item)
      }
    } else {
      debug(`delaying item: ${seq}, expected: ${expected}`)
      buffer.set(seq, data)
    }

    cb()
  })
}
