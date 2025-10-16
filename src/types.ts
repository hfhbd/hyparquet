
/**
 * Custom parsers for columns
 */
export interface ParquetParsers {
  timestampFromMilliseconds(millis: bigint): any
  timestampFromMicroseconds(micros: bigint): any
  timestampFromNanoseconds(nanos: bigint): any
  dateFromDays(days: number): any
  stringFromBytes(bytes: Uint8Array): any
}

/**
 * Parquet query options for reading data
 */
export interface BaseParquetReadOptions {
  file: AsyncBuffer // file-like object containing parquet data
  metadata?: FileMetaData // parquet metadata, will be parsed if not provided
  onChunk?: (chunk: ColumnData) => void // called when a column chunk is parsed. chunks may contain data outside the requested range.
  onPage?: (chunk: ColumnData) => void // called when a data page is parsed. pages may contain data outside the requested range.
  compressors?: Compressors // custom decompressors
  utf8?: boolean // decode byte arrays as utf8 strings (default true)
  parsers?: ParquetParsers // custom parsers to decode advanced types
}

interface ArrayRowFormat {
  rowFormat?: 'array' // format of each row passed to the onComplete function. Can be omitted, as it's the default.
  onComplete?: (rows: any[][]) => void // called when all requested rows and columns are parsed
}
interface ObjectRowFormat {
  rowFormat: 'object' // format of each row passed to the onComplete function
  onComplete?: (rows: Record<string, any>[]) => void // called when all requested rows and columns are parsed
}
export type ParquetReadOptions = BaseParquetReadOptions & (ArrayRowFormat | ObjectRowFormat)

/**
 * A run of column data
 */
export interface ColumnData {
  columnName: string
  columnData: DecodedArray
  rowStart: number
  rowEnd: number // exclusive
}

/**
 * File-like object that can read slices of a file asynchronously.
 */
export interface AsyncBuffer {
  byteLength: number
  slice(start: number, end?: number): Awaitable<ArrayBuffer>
}
export type Awaitable<T> = T | Promise<T>
export interface ByteRange {
  startByte: number
  endByte: number // exclusive
}

export interface DataReader {
  view: DataView
  offset: number
}

// Parquet file metadata types
export interface FileMetaData {
  version: number
  schema: SchemaElement[]
  num_rows: bigint
  row_groups: RowGroup[]
  created_by?: string
  // column_orders?: ColumnOrder[]
  // encryption_algorithm?: EncryptionAlgorithm
  // footer_signing_key_metadata?: Uint8Array
  metadata_length: number
}

export interface SchemaTree {
  children: SchemaTree[]
  count: number
  element: SchemaElement
  path: string[]
}

export interface SchemaElement {
  type?: ParquetType
  type_length?: number
  repetition_type?: FieldRepetitionType
  name: string
  num_children?: number
  converted_type?: ConvertedType
  scale?: number
  precision?: number
  logical_type?: LogicalType
}

export enum ParquetType {
    BOOLEAN,
    INT32,
    INT64,
    INT96, // deprecated
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY,
}

export enum FieldRepetitionType {
    REQUIRED,
    OPTIONAL,
    REPEATED
}

export enum ConvertedType {
    UTF8,
    MAP,
    MAP_KEY_VALUE,
    LIST,
    ENUM,
    DECIMAL,
    DATE,
    TIME_MILLIS,
    TIME_MICROS,
    TIMESTAMP_MILLIS,
    TIMESTAMP_MICROS,
    UINT_8,
    UINT_16,
    UINT_32,
    UINT_64,
    INT_8,
    INT_16,
    INT_32,
    INT_64,
    JSON,
    BSON,
    INTERVAL
}

export enum TimeUnit { MILLIS, MICROS, NANOS }

export type LogicalType =
  | { type: 'STRING' }
  | { type: 'MAP' }
  | { type: 'LIST' }
  | { type: 'ENUM' }
  | { type: 'DATE' }
  | { type: 'INTERVAL' }
  | { type: 'NULL' }
  | { type: 'JSON' }
  | { type: 'BSON' }
  | { type: 'UUID' }
  | { type: 'FLOAT16' }
  | { type: 'VARIANT' }
  | { type: 'DECIMAL', precision: number, scale: number }
  | { type: 'TIME', isAdjustedToUTC: boolean, unit: TimeUnit }
  | { type: 'TIMESTAMP', isAdjustedToUTC: boolean, unit: TimeUnit }
  | { type: 'INTEGER', bitWidth: number, isSigned: boolean }

export interface RowGroup {
  columns: ColumnChunk[]
  total_byte_size: bigint
  num_rows: bigint
  file_offset?: bigint
  total_compressed_size?: bigint
}

export interface ColumnChunk {
  file_path?: string
  file_offset: bigint
  meta_data?: ColumnMetaData
  offset_index_offset?: bigint
  offset_index_length?: number
  column_index_offset?: bigint
  column_index_length?: number
}

export interface ColumnMetaData {
  type: ParquetType
  path_in_schema: string[]
  codec: CompressionCodec
  num_values: bigint
  total_uncompressed_size: bigint
  total_compressed_size: bigint
  data_page_offset: bigint
  index_page_offset?: bigint
  dictionary_page_offset?: bigint
}

export enum Encoding {
    PLAIN,
    GROUP_VAR_INT, // deprecated
    PLAIN_DICTIONARY,
    RLE,
    BIT_PACKED, // deprecated
    DELTA_BINARY_PACKED,
    DELTA_LENGTH_BYTE_ARRAY,
    DELTA_BYTE_ARRAY,
    RLE_DICTIONARY,
    BYTE_STREAM_SPLIT
}

export enum CompressionCodec {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
    LZ4_RAW
}
export type Compressors = {
  [K in CompressionCodec]?: (input: Uint8Array, outputLength: number) => Uint8Array
}

export enum PageType {
  DATA_PAGE,
  INDEX_PAGE,
  DICTIONARY_PAGE,
  DATA_PAGE_V2
}

// Parquet file header types
export interface PageHeader {
  type: PageType
  uncompressed_page_size: number
  compressed_page_size: number
  data_page_header?: DataPageHeader
  dictionary_page_header?: DictionaryPageHeader
  data_page_header_v2?: DataPageHeaderV2
}

export interface DataPageHeader {
  num_values: number
  encoding: Encoding
}

export interface DictionaryPageHeader {
  num_values: number
}

export interface DataPageHeaderV2 {
  num_values: number
  num_nulls: number
  num_rows: number
  encoding: Encoding
  definition_levels_byte_length: number
  repetition_levels_byte_length: number
  is_compressed?: boolean
}

export interface DataPage {
  definitionLevels: number[] | undefined
  repetitionLevels: number[]
  dataPage: DecodedArray
}

export type DecodedArray =
  | Uint8Array
  | Uint32Array
  | Int32Array
  | BigInt64Array
  | BigUint64Array
  | Float32Array
  | Float64Array
  | any[]

export type ThriftObject = { [ key: `field_${number}` ]: ThriftType }
export type ThriftType = boolean | number | bigint | Uint8Array | ThriftType[] | ThriftObject

/**
 * Query plan for which byte ranges to read.
 */
export interface QueryPlan {
  metadata: FileMetaData
  fetches: ByteRange[] // byte ranges to fetch
  groups: GroupPlan[] // byte ranges by row group
}
// Plan for one group
export interface GroupPlan {
  ranges: ByteRange[]
  rowGroup: RowGroup // row group metadata
  groupStart: number // row index of the first row in the group
  selectStart: number // row index in the group to start reading
  selectEnd: number // row index in the group to stop reading
  groupRows: number
}

export interface ColumnDecoder {
  columnName: string
  type: ParquetType
  element: SchemaElement
  schemaPath: SchemaTree[]
  codec: CompressionCodec
  parsers: ParquetParsers
  compressors?: Compressors
  utf8?: boolean
}

export interface RowGroupSelect {
  groupStart: number // row index of the first row in the group
  selectStart: number // row index in the group to start reading
  selectEnd: number // row index in the group to stop reading
  groupRows: number
}

export interface AsyncColumn {
  pathInSchema: string[]
  data: Promise<DecodedArray[]>
}
export interface AsyncRowGroup {
  groupStart: number
  groupRows: number
  asyncColumns: AsyncColumn[]
}
