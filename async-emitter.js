const { inherits } = require('util')
const { EventEmitter } = require('events')

function AsyncEmitter () {
  EventEmitter.call(this)
}

AsyncEmitter.prototype.emitAsync = function (...args) {
  process.nextTick(() => {
    EventEmitter.prototype.emit.call(this, ...args)
  })
}

inherits(AsyncEmitter, EventEmitter)

module.exports = AsyncEmitter
