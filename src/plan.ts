import {AsyncBuffer, ByteRange, ColumnMetaData, GroupPlan, ParquetReadOptions, QueryPlan} from "./types.js"
import {concat} from './utils.js'

// Combine column chunks into a single byte range if less than 32mb
const columnChunkAggregation: number = 1 << 25 // 32mb

/**
 * Plan which byte ranges to read to satisfy a read request.
 * Metadata must be non-null.
 */
export function parquetPlan({ metadata }: ParquetReadOptions): QueryPlan {
  const rowStart = 0
  if (!metadata) throw new Error('parquetPlan requires metadata')
  const groups: GroupPlan[] = []
  const fetches: ByteRange[] = []

  // find which row groups to read
  let groupStart = 0 // first row index of the current group
  for (const rowGroup of metadata.row_groups) {
    const groupRows = Number(rowGroup.num_rows)
    const groupEnd = groupStart + groupRows
    // if row group overlaps with row range, add it to the plan
    if (groupRows > 0 && groupEnd >= rowStart) {
      const ranges: ByteRange[] = []
      // loop through each column chunk
      for (const { file_path, meta_data } of rowGroup.columns) {
        if (file_path) throw new Error('parquet file_path not supported')
        if (!meta_data) throw new Error('parquet column metadata is undefined')
        // add included columns to the plan
        ranges.push(getColumnRange(meta_data))
      }
      const selectStart = Math.max(rowStart - groupStart, 0)
      const selectEnd = Math.min(Infinity - groupStart, groupRows)
      groups.push({ ranges, rowGroup, groupStart, groupRows, selectStart, selectEnd })

      // map group plan to ranges
      const groupSize = ranges[ranges.length - 1]?.endByte - ranges[0]?.startByte
      if (groupSize < columnChunkAggregation) {
        // full row group
        fetches.push({
          startByte: ranges[0].startByte,
          endByte: ranges[ranges.length - 1].endByte,
        })
      } else if (ranges.length) {
        concat(fetches, ranges)
      }
    }

    groupStart = groupEnd
  }

  return { metadata, fetches, groups }
}

export function getColumnRange({ dictionary_page_offset, data_page_offset, total_compressed_size }: ColumnMetaData): ByteRange {
  const columnOffset = dictionary_page_offset || data_page_offset
  return {
    startByte: Number(columnOffset),
    endByte: Number(columnOffset + total_compressed_size),
  }
}

/**
 * Prefetch byte ranges from an AsyncBuffer.
 */
export function prefetchAsyncBuffer(file: AsyncBuffer, fetches: ByteRange[]): AsyncBuffer {
  // fetch byte ranges from the file
  const promises = fetches.map(({ startByte, endByte }) => file.slice(startByte, endByte))
  return {
    byteLength: file.byteLength,
    slice(start, end = file.byteLength) {
      // find matching slice
      const index = fetches.findIndex(({ startByte, endByte }) => startByte <= start && end <= endByte)
      if (index < 0) throw new Error(`no prefetch for range [${start}, ${end}]`)
      if (fetches[index].startByte !== start || fetches[index].endByte !== end) {
        // slice a subrange of the prefetch
        const startOffset = start - fetches[index].startByte
        const endOffset = end - fetches[index].startByte
        if (promises[index] instanceof Promise) {
          return promises[index].then(buffer => buffer.slice(startOffset, endOffset))
        } else {
          return promises[index].slice(startOffset, endOffset)
        }
      } else {
        return promises[index]
      }
    },
  }
}
