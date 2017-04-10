const changesFeed = require('changes-feed')

const {
  Promise,
  co,
  promisify,
  hexint,
  firstInStream
} = require('./utils')

module.exports = function createAutoincrementBased ({ createQueueStream }) {
  const dbToAppend = new Map()
  return {
    tip: co(function* ({ lane }) {
      // autoincrement is always in order
      const result = yield firstInStream(createQueueStream(lane, {
        values: false,
        reverse: true,
        limit: 1
      }))

      if (result) return result.seq
    }),
    batchEnqueuer: function ({ db, lane }) {
      const append = getAppend(db)
      return co(function* ({ data }) {
        const changes = yield Promise.all(data.map(item => append(item.value)))
        return changes.map(item => item.change)
      })
    }
  }

  function getAppend (db) {
    let cached = dbToAppend.get(db)
    if (cached) return cached

    const feed = changesFeed(db)
    const append = promisify(feed.append.bind(feed))
    dbToAppend.set(db, append)
    return append
  }
}
