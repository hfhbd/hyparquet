
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
  readonly onChunk?: (chunk: ColumnData) => void // called when a column chunk is parsed. chunks may contain data outside the requested range.
  readonly onPage?: (chunk: ColumnData) => void // called when a data page is parsed. pages may contain data outside the requested range.
  readonly compressors?: Compressors // custom decompressors
  readonly utf8?: boolean // decode byte arrays as utf8 strings (default true)
  readonly parsers?: ParquetParsers // custom parsers to decode advanced types
}

interface ArrayRowFormat {
  readonly rowFormat?: 'array' // format of each row passed to the onComplete function. Can be omitted, as it's the default.
  readonly onComplete?: (rows: any[][]) => void // called when all requested rows and columns are parsed
}
interface ObjectRowFormat {
  readonly rowFormat: 'object' // format of each row passed to the onComplete function
  readonly onComplete?: (rows: Record<string, any>[]) => void // called when all requested rows and columns are parsed
}
export type ParquetReadOptions = BaseParquetReadOptions & (ArrayRowFormat | ObjectRowFormat)

/**
 * Parquet query options without onComplete callback
 */
export type ParquetReadOptionsWithoutOnComplete = BaseParquetReadOptions & {
  readonly rowFormat?: 'array' | 'object'
}

/**
 * A run of column data
 */
export interface ColumnData {
  readonly columnName: string
  readonly columnData: DecodedArray
  readonly rowStart: number
  readonly rowEnd: number // exclusive
}

/**
 * File-like object that can read slices of a file asynchronously.
 */
export interface AsyncBuffer {
  readonly byteLength: number
  slice(start: number, end: number | undefined): Awaitable<ArrayBuffer>
}
export type Awaitable<T> = T | Promise<T>
export interface ByteRange {
  readonly startByte: number
  readonly endByte: number // exclusive
}

export interface DataReader {
  readonly view: DataView
  offset: number
}

// Parquet file metadata types
export interface FileMetaData {
  readonly version: number
  readonly schema: SchemaElement[]
  readonly num_rows: bigint
  readonly row_groups: RowGroup[]
  readonly created_by: string | undefined
  readonly metadata_length: number
}

export interface SchemaTree {
  readonly children: SchemaTree[]
  readonly count: number
  readonly element: SchemaElement
  readonly path: string[]
}

export interface SchemaElement {
  readonly type?: ParquetType
  readonly type_length?: number
  readonly repetition_type?: FieldRepetitionType | undefined
  readonly name: string
  readonly num_children?: number
  readonly converted_type?: ConvertedType
  readonly scale?: number
  readonly precision?: number
  readonly logical_type: LogicalType | undefined
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
  readonly columns: ColumnChunk[]
  readonly total_byte_size: bigint
  readonly num_rows: bigint
  readonly file_offset: bigint | undefined
  readonly total_compressed_size: bigint | undefined
}

export interface ColumnChunk {
  readonly file_path: string | undefined
  readonly file_offset: bigint
  readonly meta_data: ColumnMetaData | undefined
  readonly offset_index_offset: bigint | undefined
  readonly offset_index_length: number | undefined
  readonly column_index_offset: bigint | undefined
  readonly column_index_length: number | undefined
}

export interface ColumnMetaData {
  readonly type: ParquetType
  readonly path_in_schema: string[]
  readonly codec: CompressionCodec
  readonly num_values: bigint
  readonly total_uncompressed_size: bigint
  readonly total_compressed_size: bigint
  readonly data_page_offset: bigint
  readonly index_page_offset: bigint | undefined
  readonly dictionary_page_offset: bigint | undefined
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
  readonly type: PageType
  readonly uncompressed_page_size: number
  readonly compressed_page_size: number
  readonly data_page_header: DataPageHeader | undefined
  readonly dictionary_page_header: DictionaryPageHeader | undefined
  readonly data_page_header_v2: DataPageHeaderV2 | undefined
}

export interface DataPageHeader {
  readonly num_values: number
  readonly encoding: Encoding
}

export interface DictionaryPageHeader {
  readonly num_values: number
}

export interface DataPageHeaderV2 {
  readonly num_values: number
  readonly num_nulls: number
  readonly num_rows: number
  readonly encoding: Encoding
  readonly definition_levels_byte_length: number
  readonly repetition_levels_byte_length: number
  readonly is_compressed: boolean | undefined
}

export interface DataPage {
  readonly definitionLevels: number[] | undefined
  readonly repetitionLevels: number[]
  readonly dataPage: DecodedArray
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

export type ThriftObject = ThriftType[]
export type ThriftType = boolean | number | bigint | Uint8Array | ThriftType[] | ThriftObject

/**
 * Query plan for which byte ranges to read.
 */
export interface QueryPlan {
  readonly metadata: FileMetaData
  readonly fetches: ByteRange[] // byte ranges to fetch
  readonly groups: GroupPlan[] // byte ranges by row group
}
// Plan for one group
export interface GroupPlan {
  readonly ranges: ByteRange[]
  readonly rowGroup: RowGroup // row group metadata
  readonly groupStart: number // row index of the first row in the group
  readonly selectStart: number // row index in the group to start reading
  readonly selectEnd: number // row index in the group to stop reading
  readonly groupRows: number
}

export interface ColumnDecoder {
  readonly columnName?: string | undefined,
  readonly type?: ParquetType | undefined
  readonly element: SchemaElement
  readonly schemaPath?: SchemaTree[] | undefined
  readonly codec?: CompressionCodec | undefined
  readonly parsers?: ParquetParsers | undefined
  compressors?: Compressors | undefined
  utf8?: boolean | undefined
}

export interface RowGroupSelect {
  readonly groupStart: number // row index of the first row in the group
  readonly selectStart: number // row index in the group to start reading
  readonly selectEnd: number // row index in the group to stop reading
  readonly groupRows: number
}

export interface AsyncColumn {
  readonly pathInSchema: string[]
  readonly data: Promise<DecodedArray[]>
}
export interface AsyncRowGroup {
  readonly groupStart: number
  readonly groupRows: number
  readonly asyncColumns: AsyncColumn[]
}
