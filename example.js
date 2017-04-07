
const memdb = require('memdb')
const { createMultiqueue, processMultiqueue } = require('./')
const multiqueue = createMultiqueue({
  db: memdb({ valueEncoding: 'json' })
})

;['wash the dishes', 'boil the broccoli'].forEach(nag => {
  multiqueue.enqueue({
    lane: 'bill',
    value: { nag }
  })
})

;['get a job', 'get a girlfriend already', 'get a different girlfriend'].forEach(nag => {
  multiqueue.enqueue({
    lane: 'ted',
    value: { nag }
  })
})

const processor = processMultiqueue({
  multiqueue: multiqueue,
  worker: function ({ lane, value }) {
    if (lane === 'bill') {
      return new Promise(resolve => {
        console.log(`Bill...${value.nag}!`)
        setTimeout(resolve, 100)
      })
    } else if (lane === 'ted') {
      return new Promise(resolve => {
        console.log(`Ted...${value.nag}!`)
        setTimeout(resolve, 1000)
      })
    }
  }
})

processor.start()
