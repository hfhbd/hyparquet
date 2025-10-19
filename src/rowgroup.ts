import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { DEFAULT_PARSERS } from './convert.js'
import { getColumnRange } from './plan.js'
import { getSchemaPath } from './schema.js'
import {
  AsyncColumn,
  AsyncRowGroup, ColumnDecoder, DecodedArray, FileMetaData,
  GroupPlan,
  ParquetParsers,
  ParquetReadOptions,
  SchemaTree
} from "./types.js"
import { flatten } from './utils.js'

/**
 * @import {AsyncColumn, AsyncRowGroup, DecodedArray, GroupPlan, ParquetParsers, ParquetReadOptions, QueryPlan, RowGroup, SchemaTree} from './types.js'
 */
/**
 * Read a row group from a file-like object.
 *
 * @param {ParquetReadOptions} options
 * @param {FileMetaData} metadata
 * @param {GroupPlan} groupPlan
 * @returns {AsyncRowGroup} resolves to column data
 */
export function readRowGroup(options: ParquetReadOptions, metadata: FileMetaData, groupPlan: GroupPlan): AsyncRowGroup {
  const { file, compressors, utf8 } = options

  const asyncColumns: AsyncColumn[] = []
  const parsers: ParquetParsers = { ...DEFAULT_PARSERS, ...options.parsers }

  // read column data
  for (const { file_path, meta_data } of groupPlan.rowGroup.columns) {
    if (file_path) throw new Error('parquet file_path not supported')
    if (!meta_data) throw new Error('parquet column metadata is undefined')

    const { startByte, endByte } = getColumnRange(meta_data)
    const columnBytes = endByte - startByte

    // skip columns larger than 1gb
    // TODO: stream process the data, returning only the requested rows
    if (columnBytes > 1 << 30) {
      console.warn(`parquet skipping huge column "${meta_data.path_in_schema}" ${columnBytes} bytes`)
      // TODO: set column to new Error('parquet column too large')
      continue
    }

    // wrap awaitable to ensure it's a promise
    const buffer: Promise<ArrayBuffer> = Promise.resolve(file.slice(startByte, endByte))

    // read column data async
    asyncColumns.push({
      pathInSchema: meta_data.path_in_schema,
      data: buffer.then(arrayBuffer => {
        const schemaPath = getSchemaPath(metadata.schema, meta_data.path_in_schema)
        const reader = { view: new DataView(arrayBuffer), offset: 0 }
        const subcolumn = meta_data.path_in_schema.join('.')
        const columnDecoder: ColumnDecoder = {
          columnName: subcolumn,
          type: meta_data.type,
          element: schemaPath[schemaPath.length - 1].element,
          schemaPath,
          codec: meta_data.codec,
          parsers,
        }
        if (utf8 !== undefined) {
          columnDecoder.utf8 = utf8
        }
        if (compressors) {
          columnDecoder.compressors = compressors
        }

        return readColumn(reader, groupPlan, columnDecoder, options.onPage)
      }),
    })
  }

  return { groupStart: groupPlan.groupStart, groupRows: groupPlan.groupRows, asyncColumns }
}

/**
 * @overload
 * @param {AsyncRowGroup} asyncGroup
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @param {'object'} rowFormat
 * @returns {Promise<Record<string, any>[]>} resolves to row data
 */
/**
 * @overload
 * @param {AsyncRowGroup} asyncGroup
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @param {'array'} [rowFormat]
 * @returns {Promise<any[][]>} resolves to row data
 */
/**
 * @param {AsyncColumn[]} asyncColumns
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {'object' | 'array'} [rowFormat]
 * @returns {Promise<Record<string, any>[] | any[][]>} resolves to row data
 */
export async function asyncGroupToRows(asyncColumns: AsyncColumn[], selectStart: number, selectEnd: number, rowFormat: 'object' | 'array'): Promise<Record<string, any>[] | any[][]> {
  // columnData[i] for asyncColumns[i]
  // TODO: do it without flatten
  const columnDatas = await Promise.all(asyncColumns.map(({ data }) => data.then(flatten)))

  // careful mapping of column order for rowFormat: array
  const columnOrder = asyncColumns
    .map(child => child.pathInSchema[0])
  const columnIndexes = columnOrder.map(name => asyncColumns.findIndex(column => column.pathInSchema[0] === name))

  // transpose columns into rows
  const selectCount = selectEnd - selectStart
  if (rowFormat === 'object') {
    const groupData: Record<string, any>[] = new Array(selectCount)
    for (let selectRow = 0; selectRow < selectCount; selectRow++) {
      const row = selectStart + selectRow
      // return each row as an object
      const rowData: Record<string, any> = {}
      for (let i = 0; i < asyncColumns.length; i++) {
        rowData[asyncColumns[i].pathInSchema[0]] = columnDatas[i][row]
      }
      groupData[selectRow] = rowData
    }
    return groupData
  }

  const groupData: any[][] = new Array(selectCount)
  for (let selectRow = 0; selectRow < selectCount; selectRow++) {
    const row = selectStart + selectRow
    // return each row as an array
    const rowData = new Array(asyncColumns.length)
    for (let i = 0; i < columnOrder.length; i++) {
      if (columnIndexes[i] >= 0) {
        rowData[i] = columnDatas[columnIndexes[i]][row]
      }
    }
    groupData[selectRow] = rowData
  }
  return groupData
}

/**
 * Assemble physical columns into top-level columns asynchronously.
 */
export function assembleAsync(asyncRowGroup: AsyncRowGroup, schemaTree: SchemaTree): AsyncRowGroup {
  const { asyncColumns } = asyncRowGroup
  const assembled: AsyncColumn[] = []
  for (const child of schemaTree.children) {
    if (child.children.length) {
      const childColumns = asyncColumns.filter(column => column.pathInSchema[0] === child.element.name)
      if (!childColumns.length) continue

      // wait for all child columns to be read
      const flatData: Map<string, DecodedArray> = new Map()
      const data: Promise<[any]> = Promise.all(childColumns.map(async column => {
        let columnData = await column.data;
        flatData.set(column.pathInSchema.join('.'), flatten(columnData))
        ;
      })).then(() => {
        // assemble the column
        assembleNested(flatData, child)
        const flatColumn = flatData.get(child.path.join('.'))
        if (!flatColumn) throw new Error('parquet column data not assembled')
        return [flatColumn]
      })

      assembled.push({ pathInSchema: child.path, data })
    } else {
      // leaf node, return the column
      const asyncColumn = asyncColumns.find(column => column.pathInSchema[0] === child.element.name)
      if (asyncColumn) {
        assembled.push(asyncColumn)
      }
    }
  }
  return { ...asyncRowGroup, asyncColumns: assembled }
}
