const fs = require('fs')
const path = require('path')
const H = require('highland')
const R = require('ramda')
const tar = require('tar-stream')
const gunzip = require('gunzip-maybe')

const lunr = require('lunr')
const levenshtein = require('fast-levenshtein')

const detectColumns = require('hocr-detect-columns')
const parser = require('@spacetime/city-directory-entry-parser')
const normalizer = require('@spacetime/nyc-street-normalizer')

const STREETS_DATASET = 'nyc-streets'

const BASE_DIR = '/Users/bertspaan/data/city-directories/hocr/'

// function download (config, dirs, tools, callback) {
//   callback()
// }

function readDirectory (directory) {
  const pagesStream = H()

  const extract = tar.extract()

  extract.on('entry', (header, stream, next) => {
    const fileParts = header.name.split('/')
    const nameParts = fileParts[fileParts.length - 1].split('.')

    // Example filename:
    //   25.56886389.c6725860-7ce9-0134-fb06-00505686a51c.processed.hocr

    const pageNum = parseInt(nameParts[0])
    const imageId = nameParts[1]
    const pageUuid = nameParts[2]

    H(stream)
      .split('\n')
      .toArray((lines) => {
        const hocr = lines.join('\n')

        pagesStream.write({
          directory,
          hocr,
          pageNum,
          imageId,
          pageUuid
        })

        next()
      })
  })

  extract.on('finish', () => {
    pagesStream.end()
  })

  const tarGzFile = path.join(BASE_DIR, `${directory.uuid}.tar.gz`)

  fs.createReadStream(tarGzFile)
    .pipe(gunzip())
    .pipe(extract)

  return pagesStream
}

function parse (config, dirs, tools, callback) {
  const directories = [
    {
      uuid: '4adf9ec0-317a-0134-03ad-00505686a51c',
      year: [1850, 1851],
      startPage: 21,
      endPage: 560,
      columnCount: 2
    },
    {
      uuid: '4afa0510-317a-0134-cf84-00505686a51c',
      year: [1858, 1859],
      startPage: 21,
      endPage: 885,
      columnCount: 2
    },
    {
      uuid: '4b00bf60-317a-0134-32d0-00505686a51c',
      year: [1860, 1861],
      startPage: 21,
      endPage: 946,
      columnCount: 2
    },
    {
      uuid: '4b51d420-317a-0134-aa50-00505686a51c',
      year: [1877, 1878],
      startPage: 17,
      endPage: 1552,
      columnCount: 2
    }
  ]

  H(directories)
    .map(readDirectory)
    .sequence()
    .errors((err) => {
      console.error(err)
    })
    .filter((page) => page.pageNum >= page.directory.startPage && page.pageNum <= page.directory.endPage)
    .map((page) => {
      const detectedPages = detectColumns(page.hocr, {
        columnCount: page.directory.columnCount
      })

      return {
        ...page,
        detected: detectedPages[0]
      }
    })
    .map((page) => {
      return page.detected.lines
        .filter((line) => line.columnIndex !== undefined)
        .map((line) => ({
          uuid: page.directory.uuid,
          year: page.directory.year,
          imageId: page.imageId,
          pageUuid: page.pageUuid,
          pageNum: page.pageNum,
          // TODO: add bboxes of idented lines!
          bbox: line.properties.bbox,
          text: line.completeText.replace(/\.+/g, '.')
        }))
    })
    .flatten()
    .map((line) => ({
      ...line,
      parsed: parser(line.text)
    }))
    .errors((err) => {
      console.error(err.message)
    })
    .map(JSON.stringify)
    .intersperse('\n')
    .pipe(fs.createWriteStream(path.join(dirs.current, 'lines.ndjson')))
    .on('finish', callback)
}

function indexStreets (dirs) {
  const streetsNdjson = path.join(dirs.getDir('nyc-streets', 'transform'), `${STREETS_DATASET}.objects.ndjson`)

  let i = 0
  return new Promise((resolve, reject) => {
    H(fs.createReadStream(streetsNdjson))
      .split()
      .compact()
      .map(JSON.parse)
      .map(R.prop('name'))
      .uniq()
      .map((name) => ({
        id: i++,
        name: normalizer(name)
      }))
      .stopOnError(reject)
      .toArray((names) => {
        const index = lunr(function () {
          this.ref('id')
          this.field('name')

          names.forEach((name) => this.add(name))
        })

        resolve({
          search: (str) => index.search(str).map((result) => names[result.ref])
        })
      })
  })
}

function findAddress (streets, location) {
  const match = /^([\d½]+) (.*)/i.exec(location.value)

  if (!match) {
    return
  }

  const number = match[1]
  const street = match[2]

  const normalized = normalizer(street)

  const editDistancePerWord = 2
  const searchStr = normalized.split(' ')
    .map((word) => {
      if (word.length <= 3 || word.match(/^\d/)) {
        return word
      }

      return `${word}~${editDistancePerWord}`
    })
    .join(' ')

  const results = streets.search(searchStr)

  const bestResults = results
    .map((street) => Object.assign(street, {
      distance: levenshtein.get(normalized, street.name)
    }))
    .sort((a, b) => a.distance - b.distance)
    .filter((street) => street.distance <= 2)

  if (bestResults.length) {
    const address = `${number} ${bestResults[0].name}`

    return {
      matched: address,
      ...location
    }
  }
}

function makeId (line) {
  const yearPart = Array.isArray(line.year) ? line.year.join('-') : line.year
  const bboxPart = line.bbox.join('-')

  return `${yearPart}.${line.pageNum}.${bboxPart}`
}

function transform (config, dirs, tools, callback) {
  indexStreets(dirs)
    .then((streets) => new Promise((resolve, reject) => {
      H(fs.createReadStream(path.join(dirs.previous, 'lines.ndjson')))
        .split()
        .compact()
        .map(JSON.parse)
        // TODO: log if filtered
        .filter((line) => line.parsed && line.parsed.location && line.parsed.location.length)
        .filter((line) => line.parsed && line.parsed.subject && line.parsed.subject.length)
        .map((line) => {
          const logs = []

          const addresses = line.parsed.location
            .map((location) => {
              const address = findAddress(streets, location)
              if (address) {
                return address.matched
              }
            })
            .filter(R.identity)

          if (!addresses.length) {
            // TODO: log!
            return
          }

          const primarySubject = line.parsed.subject
            .filter((subject) => subject.type === 'primary')[0]

          if (!primarySubject) {
            // TODO: log!
            return
          }

          // // TODO: something with otherSubjects
          // const otherSubjects = line.parsed.subject
          //   .filter((subject) => subject.type !== 'primary')

          return [
            {
              type: 'object',
              obj: {
                id: makeId(line),
                type: 'st:Person',
                name: primarySubject.value,
                validSince: Array.isArray(line.year) ? line.year[0] : line.year,
                validUntil: Array.isArray(line.year) ? line.year[1] : line.year,
                data: {
                  uuid: line.uuid,
                  pageNum: line.pageNum,
                  bbox: line.bbox,
                  text: line.text,
                  occupation: primarySubject.occupation,
                  addresses
                }
              }
            },
            ...logs.map((log) => ({
              type: 'log',
              obj: {
                // line
                // uud
                // error
              }
            }))
          ]
        })
        .compact()
        .flatten()
        .map(H.curry(tools.writer.writeObject))
        .nfcall([])
        .series()
        .stopOnError(reject)
        .done(resolve)
    }))
    .then(callback)
    .catch(callback)
}

// ==================================== Steps ====================================

module.exports.steps = [
  // download,
  parse,
  transform
]
