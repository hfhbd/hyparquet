import fs from 'fs'
import { compressors } from './helpers.ts'
import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync } from '../src/metadata.js'
import { parquetRead } from '../src/read.js'
import { toJson } from '../src/utils.js'
import { asyncBufferFromFile } from '../src/node.js'
import { fileToJson } from './helpers.js'

describe('parquetRead test files', () => {
  const files = fs.readdirSync('test/files').filter(f => f.endsWith('.parquet'))

  files.forEach(filename => {

    if (
        filename === 'byte_stream_split.zstd.parquet' ||
        filename === 'delta_length_byte_array.parquet' ||
        filename === 'duckdb5533.parquet' ||
        filename === "nested_structs.rust.parquet"
    ) return

    it(`parse data from ${filename}`, async () => {
      const file = await asyncBufferFromFile(`test/files/${filename}`)
      await parquetRead({
        file,
        compressors,
        onComplete(rows) {
          const base = filename.replace('.parquet', '')
          const expected = fileToJson(`test/files/${base}.json`)
          // stringify and parse to make legal json (NaN, -0, etc.)
          expect(JSON.parse(JSON.stringify(toJson(rows)))).toEqual(expected)
        },
      })
    })

    it(`read the last row from ${filename}`, async () => {
      // this exercises some of the page-skipping optimizations
      const file = await asyncBufferFromFile(`test/files/${filename}`)
      const metadata = await parquetMetadataAsync(file)
      let numRows = Number(metadata.num_rows)
      // repeated_no_annotation has wrong num_rows in metadata:
      if (filename === 'repeated_no_annotation.parquet') numRows = 6
      await parquetRead({
        file,
        compressors,
        rowStart: numRows - 1,
        rowEnd: numRows,
        onComplete(rows) {
          const base = filename.replace('.parquet', '')
          if (rows.length) {
            const expected = [fileToJson(`test/files/${base}.json`).at(-1)]
            expect(toJson(rows)).toEqual(expected)
          }
        },
      })
    })
  })
})
