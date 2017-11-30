const fs = require('fs')
const spawn = require('child_process').spawn
const util = require('util')
const stream = require('stream')
const Transform = stream.Transform

function EntryParser (config) {
  if (!(this instanceof EntryParser)) {
    return new EntryParser(config)
  }

  const parserPath = config && config.path
  const parserTraining = config && config.training

  if (!parserPath) {
    throw new Error('Path not set in configuration, please set config.path')
  }

  if (!parserTraining) {
    throw new Error('Path to training data not set in configuration, please set config.training')
  }

  const parserFilename = `${parserPath}/parse.py`

  if (!fs.existsSync(parserFilename)) {
    throw new Error(`Can't find parse.py in directory: ${parserPath}`)
  }

  if (!fs.existsSync(parserTraining)) {
    throw new Error(`Training file does not exist: ${parserTraining}`)
  }

  this.buff = ''
  this.objectQueue = []

  const python = spawn('python3', [parserFilename, '--training', parserTraining])
  python.stdin.setEncoding('utf-8')
  this._python = python

  this.pushParsed = function (parsed) {
    const obj = this.objectQueue[0]
    this.objectQueue = this.objectQueue.slice(1)

    if (this.writable) {
      this.push({
        ...obj,
        parsed
      })
    }
  }

  python.stdout.on('readable', () => {
    let data
    while ((data = python.stdout.read()) !== null) {
      const lines = (this.buff + data.toString()).split('\n')

      lines.forEach((line, index) => {
        if (index === lines.length - 1) {
          this.buff = line
        } else {
          this.pushParsed(JSON.parse(line))
        }
      })
    }
  })

  python.on('close', () => this.emit('close'))

  Transform.call(this, {
    objectMode: true
  })
}

util.inherits(EntryParser, Transform)

EntryParser.prototype._transform = function (obj, encoding, callback) {
  if (this._python && this._python.stdin && this._python.stdin.writable) {
    this._python.stdin.write(`${obj.text}\n`, encoding, callback)
  } else {
    return callback(new Error('Unable to parse, cannot write data to Python module'))
  }
}

EntryParser.prototype._flush = function (callback) {
  this._python.stdin.end(callback)
  this._python = null
}

module.exports = EntryParser
