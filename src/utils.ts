import {AsyncBuffer, DecodedArray} from "./types.js";

/**
 * Replace bigint, date, etc. with legal JSON types.
 *
 * @param obj object to convert
 * @returns converted object
 */
export function toJson(obj: any): unknown {
  if (obj === undefined) return null
  if (typeof obj === 'bigint') return Number(obj)
  if (Array.isArray(obj)) return obj.map(toJson)
  if (obj instanceof Uint8Array) return Array.from(obj)
  if (obj instanceof Date) return obj.toISOString()
  if (obj instanceof Object) {
    const newObj: Record<string, unknown> = {}
    for (const key of Object.keys(obj)) {
      if (obj[key] === undefined) continue
      newObj[key] = toJson(obj[key])
    }
    return newObj
  }
  return obj
}

/**
 * Concatenate two arrays fast.
 */
export function concat(aaa: any[], bbb: DecodedArray) {
  const chunk = 10000
  for (let i = 0; i < bbb.length; i += chunk) {
    aaa.push(...bbb.slice(i, i + chunk))
  }
}

/**
 * Get the byte length of a URL using a HEAD request.
 * If requestInit is provided, it will be passed to fetch.
 */
export async function byteLengthFromUrl(url: string, requestInit?: RequestInit, customFetch?: typeof globalThis.fetch): Promise<number> {
  const fetch = customFetch !== undefined ? customFetch : globalThis.fetch
  return await fetch(url, { ...requestInit, method: 'HEAD' })
    .then(res => {
      if (!res.ok) throw new Error(`fetch head failed ${res.status}`)
      const length = res.headers.get('Content-Length')
      if (!length) throw new Error('missing content length')
      return parseInt(length)
    })
}

/**
 * Construct an AsyncBuffer for a URL.
 * If byteLength is not provided, will make a HEAD request to get the file size.
 * If fetch is provided, it will be used instead of the global fetch.
 * If requestInit is provided, it will be passed to fetch.
 */
export async function asyncBufferFromUrl({ url, byteLength, requestInit, fetch: customFetch }: { url: string; byteLength?: number; fetch?: typeof globalThis.fetch; requestInit?: RequestInit; }): Promise<AsyncBuffer> {
  if (!url) throw new Error('missing url')
  const fetch = customFetch !== undefined ? customFetch : globalThis.fetch
  // byte length from HEAD request
  byteLength ||= await byteLengthFromUrl(url, requestInit, fetch)

  /**
   * A promise for the whole buffer, if range requests are not supported.
   */
  let buffer: Promise<ArrayBuffer> | undefined = undefined
  const init = requestInit || {}

  return {
    byteLength,
    async slice(start, end) {
      if (buffer) {
        return buffer.then(buffer => buffer.slice(start, end))
      }

      const headers = new Headers(init.headers)
      const endStr = end === undefined ? '' : end - 1
      headers.set('Range', `bytes=${start}-${endStr}`)

      const res = await fetch(url, { ...init, headers })
      if (!res.ok || !res.body) throw new Error(`fetch failed ${res.status}`)

      if (res.status === 200) {
        // Endpoint does not support range requests and returned the whole object
        buffer = res.arrayBuffer()
        return buffer.then(buffer => buffer.slice(start, end))
      } else if (res.status === 206) {
        // The endpoint supports range requests and sent us the requested range
        return res.arrayBuffer()
      } else {
        throw new Error(`fetch received unexpected status code ${res.status}`)
      }
    },
  }
}
/**
 * Flatten a list of lists into a single list.
 */
export function flatten(chunks: DecodedArray[]): DecodedArray {
  if (!chunks) return []
  if (chunks.length === 1) return chunks[0]
  const output: any[] = []
  for (const chunk of chunks) {
    concat(output, chunk)
  }
  return output
}
