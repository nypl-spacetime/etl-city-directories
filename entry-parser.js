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

  this.python = spawn('python3', [parserFilename, '--training', parserTraining])
  this.python.stdin.setEncoding('utf-8')

  this.pushParsed = function (parsed) {
    const obj = this.objectQueue[0]
    this.objectQueue = this.objectQueue.slice(1)
    this.push({
      ...obj,
      parsed
    })
  }

  this.python.stdout.on('data', (data) => {
    const lines = (this.buff + data.toString()).split('\n')

    lines.forEach((line, index) => {
      if (index === lines.length - 1) {
        this.buff = line
      } else {
        this.pushParsed(JSON.parse(line))
      }
    })
  })

  this.python.stdout.on('end', () => {
    if (this.buff.length) {
      this.pushParsed(JSON.parse(this.buff))
    }
  })

  Transform.call(this, {
    objectMode: true
  })
}
util.inherits(EntryParser, Transform)

EntryParser.prototype._transform = function (obj, enc, callback) {
  this.objectQueue.push(obj)
  this.python.stdin.write(`${obj.text}\n`)
  callback()
}

EntryParser.prototype._flush = function (callback) {
  this.python.stdin.end()
  callback()
}

module.exports = EntryParser
