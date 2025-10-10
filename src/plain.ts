import {DataReader, DecodedArray, ParquetType} from "./types.js"

/**
 * Read `count` values of the given type from the reader.view.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {ParquetType} type - parquet type of the data
 * @param {number} count - number of values to read
 * @param {number | undefined} fixedLength - length of each fixed length byte array
 * @returns {DecodedArray} array of values
 */
export function readPlain(reader: DataReader, type: ParquetType, count: number, fixedLength: number | undefined): DecodedArray {
  if (count === 0) return []
  if (type === ParquetType.BOOLEAN) {
    return readPlainBoolean(reader, count)
  } else if (type === ParquetType.INT32) {
    return readPlainInt32(reader, count)
  } else if (type === ParquetType.INT64) {
    return readPlainInt64(reader, count)
  } else if (type === ParquetType.INT96) {
    return readPlainInt96(reader, count)
  } else if (type === ParquetType.FLOAT) {
    return readPlainFloat(reader, count)
  } else if (type === ParquetType.DOUBLE) {
    return readPlainDouble(reader, count)
  } else if (type === ParquetType.BYTE_ARRAY) {
    return readPlainByteArray(reader, count)
  } else if (type === ParquetType.FIXED_LEN_BYTE_ARRAY) {
    if (!fixedLength) throw new Error('parquet missing fixed length')
    return readPlainByteArrayFixed(reader, count, fixedLength)
  } else {
    throw new Error(`parquet unhandled type: ${type}`)
  }
}

/**
 * Read `count` boolean values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {boolean[]}
 */
function readPlainBoolean(reader: DataReader, count: number): boolean[] {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const byteOffset = reader.offset + (i / 8 | 0)
    const bitOffset = i % 8
    const byte = reader.view.getUint8(byteOffset)
    values[i] = (byte & 1 << bitOffset) !== 0
  }
  reader.offset += Math.ceil(count / 8)
  return values
}

/**
 * Read `count` int32 values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Int32Array}
 */
function readPlainInt32(reader: DataReader, count: number): Int32Array {
  const values = (reader.view.byteOffset + reader.offset) % 4
    ? new Int32Array(align(reader.view.buffer, reader.view.byteOffset + reader.offset, count * 4))
    : new Int32Array(reader.view.buffer, reader.view.byteOffset + reader.offset, count)
  reader.offset += count * 4
  return values
}

/**
 * Read `count` int64 values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {BigInt64Array}
 */
function readPlainInt64(reader: DataReader, count: number): BigInt64Array {
  const values = (reader.view.byteOffset + reader.offset) % 8
    ? new BigInt64Array(align(reader.view.buffer, reader.view.byteOffset + reader.offset, count * 8))
    : new BigInt64Array(reader.view.buffer, reader.view.byteOffset + reader.offset, count)
  reader.offset += count * 8
  return values
}

/**
 * Read `count` int96 values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {bigint[]}
 */
function readPlainInt96(reader: DataReader, count: number): bigint[] {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const low = reader.view.getBigInt64(reader.offset + i * 12, true)
    const high = reader.view.getInt32(reader.offset + i * 12 + 8, true)
    values[i] = BigInt(high) << 64n | low
  }
  reader.offset += count * 12
  return values
}

/**
 * Read `count` float values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Float32Array}
 */
function readPlainFloat(reader: DataReader, count: number): Float32Array {
  const values = (reader.view.byteOffset + reader.offset) % 4
    ? new Float32Array(align(reader.view.buffer, reader.view.byteOffset + reader.offset, count * 4))
    : new Float32Array(reader.view.buffer, reader.view.byteOffset + reader.offset, count)
  reader.offset += count * 4
  return values
}

/**
 * Read `count` double values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Float64Array}
 */
function readPlainDouble(reader: DataReader, count: number): Float64Array {
  const values = (reader.view.byteOffset + reader.offset) % 8
    ? new Float64Array(align(reader.view.buffer, reader.view.byteOffset + reader.offset, count * 8))
    : new Float64Array(reader.view.buffer, reader.view.byteOffset + reader.offset, count)
  reader.offset += count * 8
  return values
}

/**
 * Read `count` byte array values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Uint8Array[]}
 */
function readPlainByteArray(reader: DataReader, count: number): Uint8Array[] {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const length = reader.view.getUint32(reader.offset, true)
    reader.offset += 4
    values[i] = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length)
    reader.offset += length
  }
  return values
}

/**
 * Read a fixed length byte array.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @param {number} fixedLength
 * @returns {Uint8Array[]}
 */
function readPlainByteArrayFixed(reader: DataReader, count: number, fixedLength: number): Uint8Array[] {
  // assert(reader.view.byteLength - reader.offset >= count * fixedLength)
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    values[i] = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, fixedLength)
    reader.offset += fixedLength
  }
  return values
}

/**
 * Create a new buffer with the offset and size.
 *
 * @import {DataReader, DecodedArray, ParquetType} from '../src/types.d.ts'
 * @param {ArrayBufferLike} buffer
 * @param {number} offset
 * @param {number} size
 * @returns {ArrayBuffer}
 */
function align(buffer: ArrayBufferLike, offset: number, size: number): ArrayBuffer {
  const aligned = new ArrayBuffer(size)
  new Uint8Array(aligned).set(new Uint8Array(buffer, offset, size))
  return aligned
}
