
const { EventEmitter } = require('events')
const Promise = require('any-promise')

module.exports = function () {
  let globalSwitch
  let localSwitches = {}

  const emitter = new EventEmitter()
  const eventToValue = {
    open: true,
    close: false
  }

  const eventToMethod = {
    open: 'open',
    close: 'close'
  }

  ;['open', 'close'].forEach(event => {
    const switchValue = eventToValue[event]
    emitter.on(event, id => {
      if (id) {
        localSwitches[id] = switchValue
        return
      }

      globalSwitch = switchValue
      localSwitches = {}
    })

    const method = eventToMethod[event]
    emitter[method] = function (id) {
      emitter.emit(event, id)
    }
  })

  emitter.isOpen = function isOpen (id) {
    return typeof localSwitches[id] === 'boolean' ? localSwitches[id] : globalSwitch
  }

  emitter.awaitOpen = function awaitOpen (id) {
    return emitter.isOpen(id) ? Promise.resolve() : awaitEvent('open', id)
  }

  emitter.awaitClosed = function awaitClosed (id) {
    return emitter.isOpen(id) ? awaitEvent('close', id) : Promise.resolve()
  }

  emitter.setMaxListeners(Infinity)

  return emitter

  function awaitEvent (event, id1) {
    return new Promise(resolve => {
      emitter.on(event, maybeResolve)

      function maybeResolve (id2) {
        // if id2 is undefined, the globalSwitch is being thrown
        if (typeof id2 === 'undefined' || id1 === id2) {
          emitter.removeListener(event, maybeResolve)
          resolve()
        }
      }
    })
  }
}
