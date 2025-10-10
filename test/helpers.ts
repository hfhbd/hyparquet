import fs from 'fs'
import {AsyncBuffer, DataReader} from "../src/types.js";
import {
  decompressBrotli,
  decompressGzip, decompressLz4,
  decompressLz4Raw,
  decompressSnappy,
  decompressZstd
} from "hyparquet-compressors";

/**
 * Read file and parse as JSON
 */
export function fileToJson(filePath: string): any {
  const buffer = fs.readFileSync(filePath)
  return JSON.parse(buffer.toString())
}

/**
 * Make a DataReader from bytes
 */
export function reader(bytes: number[]): DataReader {
  return { view: new DataView(new Uint8Array(bytes).buffer), offset: 0 }
}

/**
 * Wraps an AsyncBuffer to count the number of fetches made
 *
 * @param {AsyncBuffer} asyncBuffer
 * @returns {AsyncBuffer & {fetches: number, bytes: number}}
 */
export function countingBuffer(asyncBuffer: AsyncBuffer): AsyncBuffer & { fetches: number; bytes: number; } {
  return {
    ...asyncBuffer,
    fetches: 0,
    bytes: 0,
    slice(start, end) {
      this.fetches++
      this.bytes += (end ?? asyncBuffer.byteLength) - start
      return asyncBuffer.slice(start, end)
    },
  }
}

export enum CompressionCodec {
  UNCOMPRESSED,
  SNAPPY, GZIP,LZO, BROTLI, LZ4, ZSTD, LZ4_RAW
}
export type Compressors = {
  [K in CompressionCodec]?: (input: Uint8Array, outputLength: number) => Uint8Array
}

// Example implementation with placeholder compressor functions
export const compressors: Compressors = {
  [CompressionCodec.UNCOMPRESSED]: (input: Uint8Array, outputLength: number) => input,
  [CompressionCodec.SNAPPY]: (input: Uint8Array, outputLength: number) => {
    return decompressSnappy(input, outputLength)
  },
  [CompressionCodec.GZIP]: (input: Uint8Array, outputLength: number) => {
    return decompressGzip(input, outputLength)
  },
  [CompressionCodec.LZO]: (input: Uint8Array, outputLength: number) => {
    throw new Error("LZO is not supported")
  },
  [CompressionCodec.BROTLI]: (input: Uint8Array, outputLength: number) => {
    return decompressBrotli(input, outputLength)
  },
  [CompressionCodec.LZ4]: (input: Uint8Array, outputLength: number) => {
    return decompressLz4(input, outputLength)
  },
  [CompressionCodec.ZSTD]: (input: Uint8Array, outputLength: number) => {
    return decompressZstd(input, outputLength)
  },
  [CompressionCodec.LZ4_RAW]: (input: Uint8Array, outputLength: number) => {
    // Implement LZ4_RAW compression here
    return decompressLz4Raw(input, outputLength)
  }
}
