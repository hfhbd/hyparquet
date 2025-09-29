/** @type {import('../src/types.d.ts').ParquetType[]} */
export const ParquetType = [
  'BOOLEAN',
  'INT32',
  'INT64',
  'INT96', // deprecated
  'FLOAT',
  'DOUBLE',
  'BYTE_ARRAY',
  'FIXED_LEN_BYTE_ARRAY',
]

/** @type {import('../src/types.d.ts').Encoding[]} */
export const Encoding = [
  'PLAIN',
  'GROUP_VAR_INT', // deprecated
  'PLAIN_DICTIONARY',
  'RLE',
  'BIT_PACKED', // deprecated
  'DELTA_BINARY_PACKED',
  'DELTA_LENGTH_BYTE_ARRAY',
  'DELTA_BYTE_ARRAY',
  'RLE_DICTIONARY',
  'BYTE_STREAM_SPLIT',
]

/** @type {import('../src/types.d.ts').FieldRepetitionType[]} */
export const FieldRepetitionType = [
  'REQUIRED',
  'OPTIONAL',
  'REPEATED',
]

/** @type {import('../src/types.d.ts').ConvertedType[]} */
export const ConvertedType = [
  'UTF8',
  'MAP',
  'MAP_KEY_VALUE',
  'LIST',
  'ENUM',
  'DECIMAL',
  'DATE',
  'TIME_MILLIS',
  'TIME_MICROS',
  'TIMESTAMP_MILLIS',
  'TIMESTAMP_MICROS',
  'UINT_8',
  'UINT_16',
  'UINT_32',
  'UINT_64',
  'INT_8',
  'INT_16',
  'INT_32',
  'INT_64',
  'JSON',
  'BSON',
  'INTERVAL',
]

/** @type {import('../src/types.d.ts').CompressionCodec[]} */
export const CompressionCodec = [
  'UNCOMPRESSED',
  'SNAPPY',
  'GZIP',
  'LZO',
  'BROTLI',
  'LZ4',
  'ZSTD',
  'LZ4_RAW',
]

/** @type {import('../src/types.d.ts').PageType[]} */
export const PageType = [
  'DATA_PAGE',
  'INDEX_PAGE',
  'DICTIONARY_PAGE',
  'DATA_PAGE_V2',
]

/** @type {import('../src/types.d.ts').BoundaryOrder[]} */
export const BoundaryOrder = [
  'UNORDERED',
  'ASCENDING',
  'DESCENDING',
]

/** @type {import('../src/types.d.ts').EdgeInterpolationAlgorithm[]} */
export const EdgeInterpolationAlgorithm = [
  'SPHERICAL',
  'VINCENTY',
  'THOMAS',
  'ANDOYER',
  'KARNEY',
]
