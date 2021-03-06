# NYC Space/Time Directory ETL module: NYC City Directories

[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) module for NYPL’s [NYC Space/Time Direcory](http://spacetime.nypl.org/). This Node.js module downloads, parses, and/or transforms NYC City Directories data, and creates a NYC Space/Time Directory dataset.


## Data

The dataset created by this ETL module’s `transform` step can be found in the [data section of the NYC Space/Time Directory website](http://spacetime.nypl.org/#data-city-directories).

## Details

<table>
<tbody>

<tr>
<td>ID</td>
<td><code>city-directories</code></td>
</tr>

<tr>
<td>Title</td>
<td>NYC City Directories</td>
</tr>

<tr>
<td>Description</td>
<td>City Directories</td>
</tr>

<tr>
<td>License</td>
<td>CC0</td>
</tr>

<tr>
<td>Contributors</td>
<td><ul><li>Bert Spaan (wrangler)</li><li>Nicholas Wolf (wrangler)</li><li>Stephen Balogh (wrangler)</li></ul></td>
</tr>

<tr>
<td>Sources</td>
<td><a href="https://digitalcollections.nypl.org/collections/new-york-city-directories#/?tab=about">Digitized New York city directories</a></td>
</tr>

<tr>
<td>Homepage</td>
<td><a href="https://github.com/nypl-spacetime/city-directories">https://github.com/nypl-spacetime/city-directories</a></td>
</tr>

<tr>
<td>Depends on</td>
<td><code><a href="https://github.com/nypl-spacetime/etl-nyc-streets">nyc-streets</a>.transform</code>, <code><a href="https://github.com/nypl-spacetime/etl-addresses">addresses</a>.transform</code></td>
</tr>
</tbody>
</table>

[JSON Schema](http://json-schema.org/) of Object data:

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "volumeUuid",
    "pageUuid",
    "pageNum",
    "bbox",
    "text"
  ],
  "properties": {
    "volumeUuid": {
      "type": "string",
      "description": "NYPL Digital Collections UUID of City Directory"
    },
    "pageUuid": {
      "type": "string",
      "description": "NYPL Digital Collections UUID of page"
    },
    "pageNum": {
      "type": "integer",
      "description": "Page number"
    },
    "bbox": {
      "type": "array",
      "description": "Position of detected line in pixel in original scanned page",
      "minItems": 4,
      "maxItems": 4,
      "items": {
        "type": "integer"
      }
    },
    "text": {
      "type": "string",
      "description": "Original text from OCR software"
    },
    "occupation": {
      "type": "string",
      "description": "Occupation of person"
    },
    "locations": {
      "type": "array",
      "description": "List of addresses",
      "items": {
        "type": "object",
        "required": [
          "value"
        ]
      }
    },
    "geocoded": {
      "type": "array",
      "description": "Geocoded addresses",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": [
          "id",
          "name",
          "street",
          "streetId"
        ],
        "properties": {
          "id": {
            "type": "string",
            "description": "ID of geocoded address"
          },
          "name": {
            "type": "string",
            "description": "Full geocoded address"
          },
          "street": {
            "type": "string",
            "description": "Street name of geocoded address"
          },
          "streetId": {
            "type": "string",
            "description": "Street ID of geocoded address"
          }
        }
      }
    }
  }
}
```

## Available steps

  - `download`
  - `parse`
  - `geocode`
  - `transform`

## Usage

```
git clone https://github.com/nypl-spacetime/etl-city-directories.git /path/to/etl-modules
cd /path/to/etl-modules/etl-city-directories
npm install

spacetime-etl city-directories[.<step>]
```

See http://github.com/nypl-spacetime/spacetime-etl for information about Space/Time's ETL tool. More Space/Time ETL modules [can be found on GitHub](https://github.com/search?utf8=%E2%9C%93&q=org%3Anypl-spacetime+etl-&type=Repositories&ref=advsearch&l=&l=).

_This README file is generated by [generate-etl-readme](https://github.com/nypl-spacetime/generate-etl-readme)._
