const changesFeed = require('changes-feed')

const {
  Promise,
  co,
  promisify,
  hexint,
  firstInStream
} = require('./utils')

module.exports = function createAutoincrementBased ({ createQueueStream }) {
  function getAppend (db) {
    let cached = dbToAppend.get(db)
    if (cached) return cached

    const feed = changesFeed(db, { start: 0 })
    const append = promisify(feed.append.bind(feed))
    dbToAppend.set(db, append)
    return append
  }

  const dbToAppend = new Map()
  const api = {
    tip: co(function* ({ queue }) {
      // autoincrement is always in order
      const result = yield firstInStream(createQueueStream(queue, {
        values: false,
        reverse: true,
        limit: 1
      }))

      return result && result.seq
    }),
    batchEnqueuer: function batchEnqueuer ({ db, queue }) {
      const append = getAppend(db)
      return co(function* ({ data }) {
        const changes = yield Promise.all(data.map(item => append(item.value)))
        return changes.map(item => item.change)
      })
    }
  }

  return api
}
