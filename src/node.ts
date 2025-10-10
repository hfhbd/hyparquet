import { createReadStream, promises as fs } from 'fs'
import {AsyncBuffer} from "./types.js";

/**
 * Construct an AsyncBuffer for a local file using node fs package.
 */
export async function asyncBufferFromFile(filename: string): Promise<AsyncBuffer> {
  const { size } = await fs.stat(filename)
  return {
    byteLength: size,
    slice(start, end) {
      // read file slice
      const reader = createReadStream(filename, { start, end })
      return new Promise((resolve, reject) => {
        const chunks: any[] = []
        reader.on('data', chunk => chunks.push(chunk))
        reader.on('error', reject)
        reader.on('end', () => {
          const buffer = Buffer.concat(chunks)
          resolve(buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength))
        })
      })
    },
  }
}
