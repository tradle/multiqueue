const lexint = require('lexicographic-integer')
const Promise = require('any-promise')
const co = require('co').wrap
const promisify = require('pify')
const collect = promisify(require('stream-collector'))
const through = require('through2')
const extend = require('xtend/mutable')
const MAX_INT = 2147483647

const firstInStream = co(function* (stream) {
  const results = yield collect(stream)
  return results[0]
})

module.exports = {
  Promise,
  co,
  promisify,
  assert,
  validateEncoding,
  createPassThrough,
  createKeyParserTransform,
  hexint,
  unhexint,
  firstInStream,
  getSublevelPrefix,
  MAX_INT
}

function assert (statement, err) {
  if (!statement) throw new Error(err || 'assertion failed')
}

function validateEncoding ({ value, encoding }) {
  if (encoding === 'binary') {
    assert(Buffer.isBuffer(value), 'expected Buffer')
  } else if (encoding === 'json') {
    assert(value && typeof value === 'object', 'expected object')
  } else if (encoding === 'utf8') {
    assert(typeof value === 'string', 'expected string')
  }
}

function createPassThrough () {
  return through.obj(function (data, enc, cb) {
    cb(null, data)
  })
}

function createKeyParserTransform (parseKey) {
  return function (opts) {
    return through.obj(function (data, enc, cb) {
      if (opts.keys !== false) {
        if (opts.values === false) {
          return cb(null, parseKey(data))
        }

        extend(data, parseKey(data.key))
      }

      cb(null, data)
    })
  }
}

function hexint (n) {
  return lexint.pack(n, 'hex')
}

function unhexint (hex) {
  return lexint.unpack(Array.prototype.slice.call(new Buffer(hex, 'hex')))
}

function getSublevelPrefix ({ prefix, separator }) {
  // BAD as it assumes knowledge of subleveldown internals
  // the less efficient but better way would be to either export the prefixer function from subleveldown
  // or use getQueue(queue).prefix instead
  return separator + prefix + separator
}
