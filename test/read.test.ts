import { describe, expect, it, vi } from 'vitest'
import { convertWithDictionary } from '../src/convert.js'
import { parquetMetadataAsync } from '../src/metadata.js'
import { parquetRead, parquetReadObjects } from '../src/read.js'
import { asyncBufferFromFile } from '../src/node.js'
import { countingBuffer } from './helpers.js'
import {ColumnData} from "../src/types.js";

vi.mock('../src/convert.js', { spy: true })

describe('parquetRead', () => {
  it('filter by row overestimate', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    await parquetRead({
      file,
      onComplete(rows) {
        expect(rows).toEqual([
          [1n], [2n], [3n], [4n], [5n], [6n], [7n], [8n], [9n], [10n], [11n], [12n], [13n], [14n], [15n],
        ])
      },
    })
  })

  it('read objects and return a promise', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads individual pages', async () => {
    const file = countingBuffer(await asyncBufferFromFile('test/files/page_indexed.parquet'))
    const pages: ColumnData[] = []

    // check onPage callback
    await parquetRead({
      file,
      onPage(page) {
        pages.push(page)
      },
    })

    const expectedPages = [
      {
        columnName: 'row',
        columnData: Array.from({ length: 100 }, (_, i) => BigInt(i)),
        rowStart: 0,
        rowEnd: 100,
      },
      {
        columnName: 'quality',
        columnData: [
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad', 'bad',
          'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good',
          'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad',
          'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'good', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad',
          'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad',
          'bad', 'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad',
        ],
        rowStart: 0,
        rowEnd: 100,
      },
      {
        columnName: 'row',
        columnData: Array.from({ length: 100 }, (_, i) => BigInt(i + 100)),
        rowStart: 100,
        rowEnd: 200,
      },
      {
        columnName: 'quality',
        columnData: [
          'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad', 'good', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'good', 'bad',
          'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
        ],
        rowStart: 100,
        rowEnd: 200,
      },
    ]

    // expect each page to exist in expected
    for (const expected of expectedPages) {
      const page = pages.find(p => p.columnName === expected.columnName && p.rowStart === expected.rowStart)
      expect(page).toEqual(expected)
    }
    expect(file.fetches).toBe(3) // 1 metadata, 2 rowgroups
    expect(file.bytes).toBe(6421)
  })
})
