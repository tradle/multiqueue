
const createMultiqueue = require('./multiqueue')
const processMultiqueue = require('./process')
const monitorMissing = require('./monitor')
// const createSemaphore = require('./sempahore')
module.exports = {
  create: createMultiqueue,
  createMultiqueue,
  process: processMultiqueue,
  processMultiqueue,
  monitorMissing,
  // sempahore: createSemaphore,
  // createSemaphore
}
