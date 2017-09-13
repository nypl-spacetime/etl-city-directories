const fs = require('fs')
const path = require('path')
const got = require('got')
const H = require('highland')
const R = require('ramda')
const normalizer = require('@spacetime/nyc-street-normalizer')
const lunr = require('lunr')
const levenshtein = require('fast-levenshtein')
const detectColumns = require('hocr-detect-columns')
const parser = require('@spacetime/city-directory-entry-parser')

const streetsDataset = 'nyc-streets'
// const dir = '/Users/bertspaan/data/city-directories/complete/1854-55/hocr'
const dir = '/Users/bertspaan/data/city-directories/hocr/4ae3cb40-317a-0134-489d-00505686a51c'

function parse (config, dirs, tools, callback) {
  const directoryInfo = {
    startPage: 21,
    endPage: 817,
    columnCount: 2
  }

  const readDir = H.wrapCallback(fs.readdir)

  readDir(dir)
    .flatten()
    .filter(R.endsWith('.hocr'))
    .map((filename) => ({
      filename,
      pageNum: parseInt(filename.split('_')[0]) + 1
    }))
    .filter((page) => page.pageNum >= directoryInfo.startPage && page.pageNum <= directoryInfo.endPage)
    .map((page) => ({
      ...page,
      hocr: fs.readFileSync(path.join(dir, page.filename), 'utf8')
    }))
    .map((page) => {
      const detectedPages = detectColumns(page.hocr, {
        columnCount: directoryInfo.columnCount,
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

function transform (config, dirs, tools, callback) {
      //   console.log('')
      // console.log('location', line.parsed.location)
      // console.log('subject', line.parsed.subject)
      // return line

//   const volume = '1854-1855'
//   const cityDirectoryNdjson = 'http://spacetime-nypl-org.s3.amazonaws.com/city-directories/data/1854-1855.ndjson'
//   const streetsNdjson = path.join(dirs.getDir('nyc-streets', 'transform'), `${streetsDataset}.objects.ndjson`)

//   let i = 0
//   H(fs.createReadStream(streetsNdjson))
//     .split()
//     .compact()
//     .map(JSON.parse)
//     .map(R.prop('name'))
//     .uniq()
//     .map((name) => ({
//       id: i++,
//       name: normalizer(name)
//     }))
//     .stopOnError(callback)
//     .toArray((names) => {
//       const idx = lunr(function () {
//         this.ref('id')
//         this.field('name')

//         names.forEach((name) => this.add(name))
//       })

//       H(got.stream(cityDirectoryNdjson))
//         .split()
//         .compact()
//         .map(JSON.parse)
//         .map((line) => line.attributes_parsed.location.map((location) => ({
//           location,
//           subject: line.attributes_parsed.subject,
//           ocr: line.original_ocr
//         })))
//         .flatten()
//         .map((location) => {
//           const match = /^([\d½]+) (.*)/i.exec(location.location.value)

//           if (!match) {
//             return
//           }

//           let subject
//           let occupation
//           if (location.subject.length) {
//             subject = location.subject[0].value.split(' ').reverse().join(' ')
//             occupation = location.subject[0].occupation
//           }

//           const number = match[1]
//           const street = match[2]

//           const normalized = normalizer(street)

//           const editDistancePerWord = 2
//           const searchStr = normalized.split(' ')
//             .map((word) => {
//               if (word.length <= 3 || word.match(/^\d/)) {
//                 return word
//               }

//               return `${word}~${editDistancePerWord}`
//             })
//             .join(' ')

//           const results = idx.search(searchStr)

//           const bestResults = results
//             .map((result) => names[result.ref])
//             .filter(R.identity)
//             .map((street) => Object.assign(street, {
//               distance: levenshtein.get(normalized, street.name)
//             }))
//             .sort((a, b) => a.distance - b.distance)
//             .filter((street) => street.distance <= 2)

//           if (bestResults.length) {
//             const address = `${number} ${bestResults[0].name}`

//             return {
//               type: 'object',
//               obj: {
//                 id: `${volume}.${location.ocr.id}`,
//                 type: 'st:Person',
//                 name: subject,
//                 validSince: 1854,
//                 validUntil: 1855,
//                 data: {
//                   address,
//                   occupation,
//                   originalAddress: location.location.value
//                 }
//               }
//             }
//           } else {
//             return {
//               type: 'log',
//               obj: {
//                 id: `${volume}.${location.ocr.id}`,
//                 foundStreet: false,
//                 originalAddress: location.location.value,
//                 subject,
//                 occupation
//               }
//             }
//           }
//         })
//         .compact()
//         .flatten()
//         .map(H.curry(tools.writer.writeObject))
//         .nfcall([])
//         .series()
//         .stopOnError(callback)
//         .done(callback)
//     })
  callback()
}

// ==================================== Steps ====================================

module.exports.steps = [
  parse,
  transform
]
