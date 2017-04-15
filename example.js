
const memdb = require('memdb')
const Multiqueue = require('./')
const multiqueue = Multiqueue.create({
  db: memdb({ valueEncoding: 'json' })
})

;['wash the dishes', 'boil the broccoli'].forEach(nag => {
  multiqueue.enqueue({
    queue: 'bill',
    value: { nag }
  })
})

;['get a job', 'get a girlfriend already', 'get a different girlfriend'].forEach(nag => {
  multiqueue.enqueue({
    queue: 'ted',
    value: { nag }
  })
})

const processor = Multiqueue.process({
  multiqueue: multiqueue,
  worker: function ({ queue, value }) {
    if (queue === 'bill') {
      return new Promise(resolve => {
        console.log(`Bill...${value.nag}!`)
        setTimeout(resolve, 100)
      })
    } else if (queue === 'ted') {
      return new Promise(resolve => {
        console.log(`Ted...${value.nag}!`)
        setTimeout(resolve, 1000)
      })
    }
  }
})

processor.start()
