// TCompactProtocol types
import {DataReader, ThriftObject, ThriftType} from "./types.js";

export const CompactType = {
  STOP: 0,
  TRUE: 1,
  FALSE: 2,
  BYTE: 3,
  I16: 4,
  I32: 5,
  I64: 6,
  DOUBLE: 7,
  BINARY: 8,
  LIST: 9,
  SET: 10,
  MAP: 11,
  STRUCT: 12,
  UUID: 13,
}

/**
 * Parse TCompactProtocol
 */
export function deserializeTCompactProtocol(reader: DataReader): ThriftObject {
  let lastFid = 0
  const values: ThriftType[] = []

  while (reader.offset < reader.view.byteLength) {
    // Parse each field based on its type and add to the result object
    const [type, fid, newLastFid] = readFieldBegin(reader, lastFid)
    lastFid = newLastFid

    if (type === CompactType.STOP) {
      break
    }

    // Handle the field based on its type
    values[fid] = readElement(reader, type)
  }

  return values
}

/**
 * Read a single element based on its type
 */
function readElement(reader: DataReader, type: number) : ThriftType {
  switch (type) {
  case CompactType.TRUE:
    return true
  case CompactType.FALSE:
    return false
  case CompactType.BYTE:
    // read byte directly
    return reader.view.getInt8(reader.offset++)
  case CompactType.I16:
  case CompactType.I32:
    return readZigZag(reader)
  case CompactType.I64:
    return readZigZagBigInt(reader)
  case CompactType.DOUBLE: {
    const value = reader.view.getFloat64(reader.offset, true)
    reader.offset += 8
    return value
  }
  case CompactType.BINARY: {
    const stringLength = readVarInt(reader)
    const strBytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, stringLength)
    reader.offset += stringLength
    return strBytes
  }
  case CompactType.LIST: {
    const byte = reader.view.getUint8(reader.offset++)
    const elemType = byte & 0x0f
    let listSize = byte >> 4
    if (listSize === 15) {
      listSize = readVarInt(reader)
    }
    const boolType = elemType === CompactType.TRUE || elemType === CompactType.FALSE
    const values = new Array(listSize)
    for (let i = 0; i < listSize; i++) {
      values[i] = boolType ? readElement(reader, CompactType.BYTE) === 1 : readElement(reader, elemType)
    }
    return values
  }
  case CompactType.STRUCT: {
    const structValues: ThriftType[] = []
    let lastFid = 0
    while (true) {
      const [fieldType, fid, newLastFid] = readFieldBegin(reader, lastFid)
      lastFid = newLastFid
      if (fieldType === CompactType.STOP) {
        break
      }
      structValues[fid] = readElement(reader, fieldType)
    }
    return structValues
  }
  // TODO: MAP, SET, UUID
  default:
    throw new Error(`thrift unhandled type: ${type}`)
  }
}

/**
 * Var int aka Unsigned LEB128.
 * Reads groups of 7 low bits until high bit is 0.
 */
export function readVarInt(reader: DataReader): number {
  let result = 0
  let shift = 0
  while (true) {
    const byte = reader.view.getUint8(reader.offset++)
    result |= (byte & 0x7f) << shift
    if (!(byte & 0x80)) {
      return result
    }
    shift += 7
  }
}

/**
 * Read a varint as a bigint.
 */
function readVarBigInt(reader: DataReader): bigint {
  let result = 0n
  let shift = 0n
  while (true) {
    const byte = reader.view.getUint8(reader.offset++)
    result |= BigInt(byte & 0x7f) << shift
    if (!(byte & 0x80)) {
      return result
    }
    shift += 7n
  }
}

/**
 * Values of type int32 and int64 are transformed to a zigzag int.
 * A zigzag int folds positive and negative numbers into the positive number space.
 */
export function readZigZag(reader: DataReader): number {
  const zigzag = readVarInt(reader)
  // convert zigzag to int
  return zigzag >>> 1 ^ -(zigzag & 1)
}

/**
 * A zigzag int folds positive and negative numbers into the positive number space.
 * This version returns a BigInt.
 */
export function readZigZagBigInt(reader: DataReader): bigint {
  const zigzag = readVarBigInt(reader)
  // convert zigzag to int
  return zigzag >> 1n ^ -(zigzag & 1n)
}

/**
 * Read field type and field id
 *
 * @returns [type, fid, newLastFid]
 */
function readFieldBegin(reader: DataReader, lastFid: number): [number, number, number] {
  const byte = reader.view.getUint8(reader.offset++)
  const type = byte & 0x0f
  if (type === CompactType.STOP) {
    // STOP also ends a struct
    return [0, 0, lastFid]
  }
  const delta = byte >> 4
  const fid = delta ? lastFid + delta : readZigZag(reader)
  return [type, fid, fid]
}
