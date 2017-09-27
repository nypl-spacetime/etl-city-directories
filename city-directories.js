const fs = require('fs')
const path = require('path')
const H = require('highland')
const R = require('ramda')
const got = require('got')
const cheerio = require('cheerio')
const tar = require('tar-stream')
const gunzip = require('gunzip-maybe')

const detectColumns = require('hocr-detect-columns')
const parser = require('@spacetime/city-directory-entry-parser')

const DIRECTORY_TABLE_URL = 'http://spacetime.nypl.org/city-directories/DIRECTORIES'
const BASE_URL = 'https://s3.amazonaws.com/spacetime-nypl-org/city-directories/hocr/'

const LOG_EVERY_PAGE = 100
const LOG_EVERY_LINE = 10000

function readDirectory (baseDir, directory) {
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

  const tarGzFile = path.join(baseDir, getFilename(directory.uuid))

  fs.createReadStream(tarGzFile)
    .pipe(gunzip())
    .pipe(extract)

  return pagesStream
}

function getFilename (uuid) {
  return `${uuid}.tar.gz`
}

function downloadDirectory (dirs, directory) {
  const uuid = directory.uuid
  const url = BASE_URL + getFilename(uuid)
  const filename = path.join(dirs.current, getFilename(uuid))

  return new Promise((resolve, reject) => {
    let error = false

    got.stream(url)
      .on('error', (err) => {
        console.log(`   Error downloading ${url}: ${err.message}`)
        error = true
      })
      .pipe(fs.createWriteStream(filename))
      .on('finish', () => {
        console.log(`   Successfully downloaded ${url}`)

        if (error) {
          try {
            const errorFilename = path.join(dirs.current, `${uuid}.xml`)
            fs.renameSync(filename, errorFilename)

            resolve()
          } catch (err) {
            reject(err)
          }
        } else {
          resolve()
        }
      })
  })
}

function parseTable (html) {
  const $ = cheerio.load(html)

  const keys = $('table thead th').map((index, th) => $(th).text()).get()

  const rows = $('table tbody tr').map((index, tr) => {
    const values = $('td', tr).map((index, td) => $(td).text())
      .get()
      .map(R.trim)
      .map((value) => value.length ? value : undefined)

    const row = R.zipObj(keys, values)

    const years = row.year.split('/')

    return Object.assign(row, {
      startPage: row.startPage && parseInt(row.startPage),
      endPage: row.endPage && parseInt(row.endPage),
      columnCount: row.columnCount && parseInt(row.columnCount),
      year: years.length === 2 ? [parseInt(years[0]), parseInt(years[0]) + 1] : parseInt(row.year)
    })
  }).get()
  .filter((row) => row.uuid && row.year && row.startPage && row.endPage && row.columnCount)

  return rows
}

function download (config, dirs, tools, callback) {
  got(DIRECTORY_TABLE_URL)
    .then((response) => response.body)
    .then(parseTable)
    .then((directories) => {
      fs.writeFileSync(path.join(dirs.current, 'directories.json'), JSON.stringify(directories, null, 2))
      return directories
    })
    .catch(callback)
    .then((directories) => {
      Promise.all(directories.map(R.curry(downloadDirectory)(dirs)))
        .then(() => callback())
        .catch(callback)
    })

}

function parse (config, dirs, tools, callback) {
  let count = 1
  const directories = require(path.join(dirs.download, 'directories.json'))

  H(directories)
    .filter((directory) => fs.existsSync(path.join(dirs.download, getFilename(directory.uuid))))
    .map(R.curry(readDirectory)(dirs.download))
    .sequence()
    .errors((err) => {
      console.error(err)
    })
    .map((page) => {
      if (count % LOG_EVERY_PAGE === 0) {
        console.log(`      Parsed ${count} pages`)
      }

      count += 1
      return page
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

function makeId (line) {
  const yearPart = Array.isArray(line.year) ? line.year.join('-') : line.year
  const bboxPart = line.bbox.join('-')

  return `${yearPart}.${line.pageNum}.${bboxPart}`
}

function transform (config, dirs, tools, callback) {
  let count = 1

  H(fs.createReadStream(path.join(dirs.previous, 'lines.ndjson')))
    .split()
    .compact()
    .map(JSON.parse)
    .map((line) => {
      if (count % LOG_EVERY_LINE === 0) {
        console.log(`      Transformed ${count} lines`)
      }

      count += 1
      return line
    })
    // TODO: log if filtered
    .filter((line) => line.parsed && line.parsed.location && line.parsed.location.length)
    .filter((line) => line.parsed && line.parsed.subject && line.parsed.subject.length)
    .map((line) => {
      const logs = []

      const primarySubject = line.parsed.subject
        .filter((subject) => subject.type === 'primary')[0]

      if (!primarySubject) {
        // TODO: log!
        return
      }

      const addresses = line.parsed.location

      if (!addresses || !addresses.length) {
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
    .stopOnError(callback)
    .done(callback)
}

// ==================================== Steps ====================================

module.exports.steps = [
  download,
  parse,
  transform
]
