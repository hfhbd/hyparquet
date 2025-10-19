import fs from 'fs'
import { compressors, fileToJson } from './helpers.ts'
import { describe, expect, it } from 'vitest'
import { parquetRead } from '../src/read.js'
import { toJson } from '../src/utils.js'
import { asyncBufferFromFile } from '../src/node.js'
import { ColumnData } from "../src/types.ts";

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
        onChunk(_: ColumnData): void {},
        onPage(_: ColumnData): void {},
        rowFormat: 'array',
        utf8: true,
        file,
        compressors,
        onComplete(rows) {
          const base = filename.replace('.parquet', '')
          const expected = fileToJson(`test/files/${base}.json`)
          // stringify and parse to make legal json (NaN, -0, etc.)
          expect(JSON.parse(JSON.stringify(toJson(rows)))).toEqual(expected)
        }
      })
    })
  })
})
