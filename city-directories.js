const fs = require('fs')
const path = require('path')
const H = require('highland')
const R = require('ramda')
const got = require('got')
const cheerio = require('cheerio')
const tar = require('tar-stream')
const gunzip = require('gunzip-maybe')

const Geocoder = require('@spacetime/nyc-historical-geocoder')
const detectColumns = require('hocr-detect-columns')
const EntryParser = require('./entry-parser')

const LOG_EVERY_PAGE = 100
const LOG_EVERY_LINE = 10000

function readCityDirectory (baseDir, directory) {
  const pagesStream = H()

  const extract = tar.extract()

  extract.on('entry', (header, stream, next) => {
    if (!header.name.endsWith('.hocr')) {
      next()
      return
    }

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

  extract.on('error', (err) => {
    throw err
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

function downloadCityDirectory (dirs, baseUrl, directory) {
  const uuid = directory.uuid
  const url = baseUrl + getFilename(uuid)
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
        if (error) {
          try {
            const errorFilename = path.join(dirs.current, `${uuid}.xml`)
            fs.renameSync(filename, errorFilename)

            resolve()
          } catch (err) {
            reject(err)
          }
        } else {
          console.log(`   Successfully downloaded ${url}`)
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
  // URL to Markdown table containing list of city directories
  const tableUrl = config.tableUrl

  // Base URL from where tar.gzipped city directories can be downloaded
  const baseUrl = config.dataUrl

  if (!tableUrl || !baseUrl) {
    callback(new Error('Please set both baseUrl and tableUrl in the configuration file'))
    return
  }

  got(tableUrl)
    .then((response) => response.body)
    .then(parseTable)
    .then((directories) => {
      fs.writeFileSync(path.join(dirs.current, 'directories.json'), JSON.stringify(directories, null, 2))
      return directories
    })
    .catch(callback)
    .then((directories) => {
      Promise.all(directories.map(R.curry(downloadCityDirectory)(dirs, baseUrl)))
        .then(() => callback())
        .catch(callback)
    })
}

function getMinYear (year) {
  return Array.isArray(year) ? year[0] : year
}

function getMaxYear (year) {
  return Array.isArray(year) ? year[1] : year
}

function parse (config, dirs, tools, callback) {
  // Path of city-directory-entry-parser
  const parserPath = config.parser && config.parser.path

  // Path of city-directory-entry-parser's training data
  const parserTraining = config.parser && config.parser.training

  if (!parserPath || !parserTraining) {
    callback(new Error('Please set both parser.path and parser.training in the configuration file'))
    return
  }

  const minYear = config.minYear
  const maxYear = config.maxYear

  let count = 1
  const countsPerYear = {}

  const directories = require(path.join(dirs.download, 'directories.json'))

  H(directories)
    .filter((directory) => {
      const notTooOld = minYear ? getMinYear(directory.year) >= minYear : true
      const notTooYoung = maxYear ? getMaxYear(directory.year) <= maxYear : true
      return notTooOld && notTooYoung
    })
    .filter((directory) => fs.existsSync(path.join(dirs.download, getFilename(directory.uuid))))
    .map(R.curry(readCityDirectory)(dirs.download))
    .sequence()
    .stopOnError(callback)
    .filter((page) => page.pageNum >= page.directory.startPage && page.pageNum <= page.directory.endPage)
    .map((page) => {
      const year = page.directory.year

      if (!countsPerYear[year]) {
        countsPerYear[year] = 1
      }

      if (count % LOG_EVERY_PAGE === 0) {
        const countPerYear = countsPerYear[year]
        const total = page.directory.endPage - page.directory.startPage
        const percentage = Math.round((countPerYear / total) * 100)

        const countStr = `Parsed ${count} pages`
        const yearStr = `city directory: ${year}`
        const pageStr = `page: ${countsPerYear[year]} (${percentage}%)`
        console.log(`      ${countStr} - ${yearStr} - ${pageStr}`)
      }

      countsPerYear[year] += 1
      count += 1
      return page
    })
    .map((page) => {
      const detectedPages = detectColumns(page.hocr, {
        columnCount: page.directory.columnCount
      })

      return {
        ...page,
        detected: detectedPages[0]
      }
    })
    .filter((page) => page.detected)
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
    .compact()
    .through(EntryParser({
      path: parserPath,
      training: parserTraining
    }))
    .stopOnError(callback)
    .map(JSON.stringify)
    .intersperse('\n')
    .pipe(fs.createWriteStream(path.join(dirs.current, 'lines.ndjson')))
    .on('finish', callback)
}

function makeId (line) {
  if (!line.year || !line.bbox || !line.bbox.length || !line.pageNum) {
    return
  }

  const yearPart = Array.isArray(line.year) ? line.year.join('-') : line.year
  const bboxPart = line.bbox.join('-')

  return `${yearPart}.${line.pageNum}.${bboxPart}`
}

function geocode (config, dirs, tools, callback) {
  // Initialize geocoder
  const geocoderConfig = {
    datasetDir: dirs.getDir(null, 'transform')
  }

  Geocoder(geocoderConfig)
    .then((geocoder) => {
      let count = 1

      H(fs.createReadStream(path.join(dirs.previous, 'lines.ndjson')))
        .split()
        .compact()
        .map(JSON.parse)
        .map((line) => {
          if (count % LOG_EVERY_LINE === 0) {
            console.log(`      Geocoded ${count} lines`)
          }

          const addresses = line.parsed.locations
            .map((location) => location.value)

          const geocoded = addresses
            .map((address) => {
              try {
                const result = geocoder(address)

                return {
                  found: true,
                  result
                }
              } catch (err) {
                return {
                  found: false,
                  error: err.message
                }
              }
            })

          count += 1

          return Object.assign(line, {
            geocoded
          })
        })
        .map(JSON.stringify)
        .intersperse('\n')
        .pipe(fs.createWriteStream(path.join(dirs.current, 'lines.ndjson')))
        .on('finish', callback)
    })
    .catch(callback)
}

function makeMultiPoint (geometries) {
  if (geometries && geometries.length) {
    if (geometries.length === 1) {
      return geometries[0]
    } else {
      return {
        type: 'MultiPoint',
        coordinates: geometries.map((geometry) => geometry.coordinates)
      }
    }
  }
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

    .map((line) => {
      const id = makeId(line)
      if (!id) {
        return {
          type: 'log',
          obj: {
            error: 'Coult not create ID',
            line
          }
        }
      }

      let name
      let occupation
      let locations

      const logs = []
      const relations = []

      const geometries = []
      const addresses = []

      if (line.parsed) {
        name = line.parsed.subjects && line.parsed.subjects[0]
        occupation = line.parsed.occupations && line.parsed.occupations[0]
        locations = line.parsed.locations

        if (line.geocoded) {
          line.geocoded.forEach((geocode) => {
            if (geocode.found) {
              const addressId = geocode.result.properties.address.id

              addresses.push({
                id: addressId,
                name: geocode.result.properties.address.name,
                street: geocode.result.properties.street.name,
                streetId: geocode.result.properties.street.id
              })

              geometries.push(geocode.result.geometry)

              relations.push({
                from: id,
                to: addressId,
                type: 'st:in'
              })
            } else {
              logs.push({
                error: geocode.error
              })
            }
          })
        }
      }

      return [
        {
          type: 'object',
          obj: {
            id,
            type: 'st:Person',
            name,
            validSince: Array.isArray(line.year) ? line.year[0] : line.year,
            validUntil: Array.isArray(line.year) ? line.year[1] : line.year,
            data: {
              volumeUuid: line.uuid,
              pageUuid: line.pageUuid,
              pageNum: line.pageNum,
              bbox: line.bbox,
              text: line.text,
              occupation,
              locations,
              geocoded: addresses
            },
            geometry: makeMultiPoint(geometries)
          }
        },
        ...logs.map((log) => ({
          type: 'log',
          obj: {
            id,
            ...log
          }
        })),
        ...relations.map((relation) => ({
          type: 'relation',
          obj: relation
        }))
      ]
    })
    .flatten()
    .compact()
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
  geocode,
  transform
]
