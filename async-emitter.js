const { inherits } = require('util')
const { EventEmitter } = require('events')

function AsyncEmitter () {
  EventEmitter.call(this)
}

inherits(AsyncEmitter, EventEmitter)

AsyncEmitter.prototype.emitAsync = function (...args) {
  process.nextTick(() => {
    EventEmitter.prototype.emit.call(this, ...args)
  })
}

module.exports = AsyncEmitter
