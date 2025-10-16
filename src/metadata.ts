import {
  AsyncBuffer, ColumnChunk,
  FileMetaData, LogicalType, RowGroup,
  SchemaElement, SchemaTree, ThriftObject, TimeUnit
} from './types.ts'
import {getSchemaPath} from './schema.js'
import {deserializeTCompactProtocol} from './thrift.js'

export const defaultInitialFetchSize = 1 << 19 // 512kb

const decoder = new TextDecoder()
function decode(value: Uint8Array): string {
  return decoder.decode(value)
}

/**
 * Read parquet metadata from an async buffer.
 *
 * An AsyncBuffer is like an ArrayBuffer, but the slices are loaded
 * asynchronously, possibly over the network.
 *
 * You must provide the byteLength of the buffer, typically from a HEAD request.
 *
 * In theory, you could use suffix-range requests to fetch the end of the file,
 * and save a round trip. But in practice, this doesn't work because chrome
 * deems suffix-range requests as a not-safe-listed header, and will require
 * a pre-flight. So the byteLength is required.
 *
 * To make this efficient, we initially request the last 512kb of the file,
 * which is likely to contain the metadata. If the metadata length exceeds the
 * initial fetch, 512kb, we request the rest of the metadata from the AsyncBuffer.
 *
 * This ensures that we either make one 512kb initial request for the metadata,
 * or a second request for up to the metadata size.
 *
 * @param asyncBuffer parquet file contents
 * @param initialFetchSize initial fetch size in bytes (default 512kb)
 * @returns parquet metadata object
 */
export async function parquetMetadataAsync(asyncBuffer:AsyncBuffer, initialFetchSize: number = defaultInitialFetchSize) : Promise<FileMetaData> {
  if (!asyncBuffer || !(asyncBuffer.byteLength >= 0)) throw new Error('parquet expected AsyncBuffer')

  // fetch last bytes (footer) of the file
  const footerOffset = Math.max(0, asyncBuffer.byteLength - initialFetchSize)
  const footerBuffer = await asyncBuffer.slice(footerOffset, asyncBuffer.byteLength)

  // Check for parquet magic number "PAR1"
  const footerView = new DataView(footerBuffer)
  if (footerView.getUint32(footerBuffer.byteLength - 4, true) !== 0x31524150) {
    throw new Error('parquet file invalid (footer != PAR1)')
  }

  // Parquet files store metadata at the end of the file
  // Metadata length is 4 bytes before the last PAR1
  const metadataLength = footerView.getUint32(footerBuffer.byteLength - 8, true)
  if (metadataLength > asyncBuffer.byteLength - 8) {
    throw new Error(`parquet metadata length ${metadataLength} exceeds available buffer ${asyncBuffer.byteLength - 8}`)
  }

  // check if metadata size fits inside the initial fetch
  if (metadataLength + 8 > initialFetchSize) {
    // fetch the rest of the metadata
    const metadataOffset = asyncBuffer.byteLength - metadataLength - 8
    const metadataBuffer = await asyncBuffer.slice(metadataOffset, footerOffset)
    // combine initial fetch with the new slice
    const combinedBuffer = new ArrayBuffer(metadataLength + 8)
    const combinedView = new Uint8Array(combinedBuffer)
    combinedView.set(new Uint8Array(metadataBuffer))
    combinedView.set(new Uint8Array(footerBuffer), footerOffset - metadataOffset)
    return parquetMetadata(combinedBuffer)
  } else {
    // parse metadata from the footer
    return parquetMetadata(footerBuffer)
  }
}

/**
 * Read parquet metadata from a buffer synchronously.
 *
 * @param arrayBuffer parquet file footer
 * @returns parquet metadata object
 */
export function parquetMetadata(arrayBuffer: ArrayBuffer): FileMetaData {
  const view = new DataView(arrayBuffer)

  // Validate footer magic number "PAR1"
  if (view.byteLength < 8) {
    throw new Error('parquet file is too short')
  }
  if (view.getUint32(view.byteLength - 4, true) !== 0x31524150) {
    throw new Error('parquet file invalid (footer != PAR1)')
  }

  // Parquet files store metadata at the end of the file
  // Metadata length is 4 bytes before the last PAR1
  const metadataLengthOffset = view.byteLength - 8
  const metadata_length = view.getUint32(metadataLengthOffset, true)
  if (metadata_length > view.byteLength - 8) {
    // {metadata}, metadata_length, PAR1
    throw new Error(`parquet metadata length ${metadata_length} exceeds available buffer ${view.byteLength - 8}`)
  }

  const metadataOffset = metadataLengthOffset - metadata_length
  const reader = { view, offset: metadataOffset }
  const metadata = deserializeTCompactProtocol(reader)

  // Parse metadata from thrift data
  const version = metadata[1] as number
  const schema: SchemaElement[] = (metadata[2] as ThriftObject).map((field): SchemaElement => {
    let fieldO = field as ThriftObject
    return {
      type: fieldO[1] as number | undefined,
      type_length: fieldO[2] as number | undefined,
      repetition_type: fieldO[3] as number | undefined,
      name: decode(fieldO[4] as Uint8Array),
      num_children: fieldO[5] as number | undefined,
      converted_type: fieldO[6] as number | undefined,
      scale: fieldO[7] as number | undefined,
      precision: fieldO[8] as number | undefined,
      logical_type: logicalType(fieldO[10]),
    };
  })
  const num_rows = metadata[3] as bigint
  const row_groups = (metadata[4] as ThriftObject).map(function (rowGroup): RowGroup {
    const rowGroupField = rowGroup as ThriftObject
    const rowGroupField1 = rowGroupField[1] as ThriftObject
    return {
      columns: rowGroupField1.map(function (column): ColumnChunk {
        const columnField = column as ThriftObject
        const columnField3 = columnField[3] as ThriftObject | undefined
        const filePath = columnField[1] as Uint8Array | undefined
        return {
          file_path: filePath && decode(filePath),
          file_offset: columnField[2] as bigint,
          meta_data: columnField3 && {
            type: columnField3[1] as number,
            path_in_schema: (columnField3[3] as Uint8Array[]).map(decode),
            codec: columnField3[4] as number,
            num_values: columnField3[5] as bigint,
            total_uncompressed_size: columnField3[6] as bigint,
            total_compressed_size: columnField3[7] as bigint,
            data_page_offset: columnField3[9] as bigint,
            index_page_offset: columnField3[10] as bigint | undefined,
            dictionary_page_offset: columnField3[11] as bigint | undefined,
          },
          offset_index_offset: columnField[4] as bigint |undefined,
          offset_index_length: columnField[5]as number |undefined,
          column_index_offset: columnField[6]as bigint |undefined,
          column_index_length: columnField[7]as number |undefined,
        };
      }),
      total_byte_size: rowGroupField[2] as bigint,
      num_rows: rowGroupField[3] as bigint,
      file_offset: rowGroupField[5] as bigint | undefined,
      total_compressed_size: rowGroupField[6] as bigint | undefined,
    };
  })
  const createdByArray = metadata[6] as Uint8Array | undefined
  const created_by = createdByArray && decode(createdByArray)

  return {
    version,
    schema,
    num_rows,
    row_groups,
    created_by,
    metadata_length,
  }
}

/**
 * Return a tree of schema elements from parquet metadata.
 *
 * @param {SchemaElement[]} schema parquet metadata object
 * @returns {SchemaTree} tree of schema elements
 */
export function parquetSchema(schema: SchemaElement[]): SchemaTree {
  return getSchemaPath(schema, [])[0]!
}

function logicalType(logicalType: any): LogicalType | undefined {
  if (logicalType?.field_1) return { type: 'STRING' }
  if (logicalType?.field_2) return { type: 'MAP' }
  if (logicalType?.field_3) return { type: 'LIST' }
  if (logicalType?.field_4) return { type: 'ENUM' }
  if (logicalType?.field_5) return {
    type: 'DECIMAL',
    scale: logicalType.field_5.field_1,
    precision: logicalType.field_5.field_2,
  }
  if (logicalType?.field_6) return { type: 'DATE' }
  if (logicalType?.field_7) return {
    type: 'TIME',
    isAdjustedToUTC: logicalType.field_7.field_1,
    unit: timeUnit(logicalType.field_7.field_2),
  }
  if (logicalType?.field_8) return {
    type: 'TIMESTAMP',
    isAdjustedToUTC: logicalType.field_8.field_1,
    unit: timeUnit(logicalType.field_8.field_2),
  }
  if (logicalType?.field_10) return {
    type: 'INTEGER',
    bitWidth: logicalType.field_10.field_1,
    isSigned: logicalType.field_10.field_2,
  }
  if (logicalType?.field_11) return { type: 'NULL' }
  if (logicalType?.field_12) return { type: 'JSON' }
  if (logicalType?.field_13) return { type: 'BSON' }
  if (logicalType?.field_14) return { type: 'UUID' }
  if (logicalType?.field_15) return { type: 'FLOAT16' }
  if (logicalType?.field_16) return { type: 'VARIANT' }
  if (logicalType?.field_17) return undefined
  if (logicalType?.field_18) return undefined
  return logicalType
}

function timeUnit(unit: any): TimeUnit {
  if (unit.field_1) return TimeUnit.MILLIS
  if (unit.field_2) return TimeUnit.MICROS
  if (unit.field_3) return TimeUnit.NANOS
  throw new Error('parquet time unit required')
}
