const {
  Promise,
  co,
  promisify,
  hexint
} = require('./utils')

module.exports = function ({ createQueueStream }) {
  return {
    tip: co(function* ({ queue, getCheckpoint }) {
      // get the seq of last dequeued item
      let prev = yield getCheckpoint()
      return new Promise(resolve => {
        const stream = createQueueStream(queue, { values: false })
          .on('data', function ({ seq }) {
            if (prev && seq > prev + 1) {
              // we hit a gap
              resolve(prev)
              stream.destroy()
            }

            prev = seq
          })
          .on('end', () => resolve(prev))
      })
    }),
    batchEnqueuer: function ({ db, queue }) {
      const batchAsync = promisify(db.batch.bind(db))
      return co(function* ({ data }) {
        const batch = data.map(({ seq, value }) => {
          return {
            type: 'put',
            key: hexint(seq),
            value
          }
        })

        yield batchAsync(batch)
        return data.map(data => data.seq)
      })
    }
  }
}
