
const createMultiqueue = require('./multiqueue')
const processMultiqueue = require('./process')
module.exports = {
  create: createMultiqueue,
  createMultiqueue,
  process: processMultiqueue,
  processMultiqueue
}
