
// two things
// 1. specify { autoincrement: false } in create()
// 2. specify seq (position) in enqueue()

const memdb = require('memdb')
const Multiqueue = require('./')
const scrambledEggs = [5, 2, 4, 0, 3, 1]
const db = memdb({ valueEncoding: 'json' })
const multiqueue = Multiqueue.create({
  db,
  // 1
  autoincrement: false
})

scrambledEggs.forEach(i => {
  multiqueue.enqueue({
    queue: 'bob',
    value: { egg: i },
    // 2
    seq: i
  })
})

Multiqueue.process({
  multiqueue: multiqueue,
  worker: function ({ queue, value }) {
    console.log(value)
  }
})
.start()
