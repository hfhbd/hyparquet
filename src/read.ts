import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { parquetPlan, prefetchAsyncBuffer } from './plan.js'
import { assembleAsync, asyncGroupToRows, readRowGroup } from './rowgroup.js'
import { concat } from './utils.js'
import {AsyncRowGroup, ParquetReadOptions, ParquetReadOptionsWithoutOnComplete, QueryPlan} from "./types.js";

/**
 * @import {AsyncRowGroup, DecodedArray, ParquetReadOptions, BaseParquetReadOptions} from '../src/types.js'
 */
/**
 * Read parquet data rows from a file-like object.
 * Reads the minimal number of row groups and columns to satisfy the request.
 *
 * Returns a void promise when complete.
 * Errors are thrown on the returned promise.
 * Data is returned in callbacks onComplete, onChunk, onPage, NOT the return promise.
 * See parquetReadObjects for a more convenient API.
 *
 * @param {ParquetReadOptions} options read options
 * @returns {Promise<void>} resolves when all requested rows and columns are parsed, all errors are thrown here
 */
export async function parquetRead(options: ParquetReadOptions): Promise<void> {
  // load metadata if not provided
  if (options.metadata === undefined) {
    options.metadata = await parquetMetadataAsync(options.file)
  }

  // read row groups
  const asyncGroups = parquetReadAsync(options)
  const rowStart = 0
  const rowEnd = Infinity
  const { onChunk, onComplete, rowFormat } = options

  // skip assembly if no onComplete or onChunk, but wait for reading to finish
  if (onComplete === undefined && onChunk === undefined) {
    for (const { asyncColumns } of asyncGroups) {
      for (const { data } of asyncColumns) await data
    }
    return
  }

  // assemble struct columns
  const schemaTree = parquetSchema(options.metadata.schema)
  const assembled = asyncGroups.map(arg => assembleAsync(arg, schemaTree))

  // onChunk emit all chunks (don't await)
  if (onChunk !== undefined) {
    for (const asyncGroup of assembled) {
      for (const asyncColumn of asyncGroup.asyncColumns) {
        asyncColumn.data.then(columnDatas => {
          let rowStart = asyncGroup.groupStart
          for (const columnData of columnDatas) {
            onChunk({
              columnName: asyncColumn.pathInSchema[0],
              columnData,
              rowStart,
              rowEnd: rowStart + columnData.length,
            })
            rowStart += columnData.length
          }
        })
      }
    }
  }

  // onComplete transpose column chunks to rows
  if (onComplete !== undefined) {
    // loosen the types to avoid duplicate code
    const rows: any[] = []
    for (const asyncGroup of assembled) {
      // filter to rows in range
      const selectStart = Math.max(rowStart - asyncGroup.groupStart, 0)
      const selectEnd = Math.min((rowEnd !== undefined ? rowEnd : Infinity) - asyncGroup.groupStart, asyncGroup.groupRows)
      // transpose column chunks to rows in output
      const groupData = rowFormat === 'object' ?
        await asyncGroupToRows(asyncGroup.asyncColumns, selectStart, selectEnd, 'object') :
        await asyncGroupToRows(asyncGroup.asyncColumns, selectStart, selectEnd, 'array')
      concat(rows, groupData)
    }
    onComplete(rows)
  } else {
    // wait for all async groups to finish (complete takes care of this)
    for (const { asyncColumns } of assembled) {
      for (const { data } of asyncColumns) await data
    }
  }
}

export function parquetReadAsync(options: ParquetReadOptions): AsyncRowGroup[] {
  if (!options.metadata) throw new Error('parquet requires metadata')
  // TODO: validate options (start, end, columns, etc)

  // prefetch byte ranges
  const plan: QueryPlan = parquetPlan(options.metadata)
  options.file = prefetchAsyncBuffer(options.file, plan.fetches)

  // read row groups
  return plan.groups.map(groupPlan => readRowGroup(options, plan.metadata, groupPlan))
}

/**
 * This is a helper function to read parquet row data as a promise.
 * It is a wrapper around the more configurable parquetRead function.
 *
 * @param {ParquetReadOptionsWithoutOnComplete} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
 */
export function parquetReadObjects(options: ParquetReadOptionsWithoutOnComplete): Promise<Record<string, any>[]> {
  return new Promise((onComplete, reject) => {
    parquetRead({
      ...options,
      rowFormat: 'object', // force object output
      onComplete,
    }).catch(reject)
  })
}
