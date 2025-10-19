package app.softwork.hyparquet

import java.nio.ByteBuffer

// TCompactProtocol types
object CompactType {
    const val STOP = 0
    const val TRUE = 1
    const val FALSE = 2
    const val BYTE = 3
    const val I16 = 4
    const val I32 = 5
    const val I64 = 6
    const val DOUBLE = 7
    const val BINARY = 8
    const val LIST = 9
    const val SET = 10
    const val MAP = 11
    const val STRUCT = 12
    const val UUID = 13
}

/**
 * Parse TCompactProtocol
 */
fun deserializeTCompactProtocol(reader: DataReader): ThriftObject {
    var lastFid = 0
    val values = mutableListOf<ThriftType?>()

    while (reader.offset < reader.view.limit()) {
        // Parse each field based on its type and add to the result object
        val (type, fid, newLastFid) = readFieldBegin(reader, lastFid)
        lastFid = newLastFid

        if (type == CompactType.STOP) {
            break
        }

        // Handle the field based on its type
        // Expand list to accommodate fid
        while (values.size <= fid) {
            values.add(null)
        }
        values[fid] = readElement(reader, type)
    }

    return values
}

/**
 * Read a single element based on its type
 */
private fun readElement(reader: DataReader, type: Int): ThriftType {
    return when (type) {
        CompactType.TRUE -> ThriftType.BooleanType(true)
        CompactType.FALSE -> ThriftType.BooleanType(false)
        CompactType.BYTE -> {
            // read byte directly
            val value = reader.view.get(reader.offset).toInt()
            reader.offset++
            ThriftType.IntType(value)
        }
        CompactType.I16, CompactType.I32 -> {
            ThriftType.IntType(readZigZag(reader))
        }
        CompactType.I64 -> {
            ThriftType.LongType(readZigZagLong(reader))
        }
        CompactType.DOUBLE -> {
            val value = reader.view.getDouble(reader.offset)
            reader.offset += 8
            ThriftType.IntType(value.toInt()) // TODO: proper double handling
        }
        CompactType.BINARY -> {
            val stringLength = readVarInt(reader)
            val strBytes = ByteArray(stringLength)
            reader.view.position(reader.offset)
            reader.view.get(strBytes)
            reader.offset += stringLength
            ThriftType.ByteArrayType(strBytes)
        }
        CompactType.LIST -> {
            val byte = reader.view.get(reader.offset).toInt() and 0xFF
            reader.offset++
            val elemType = byte and 0x0f
            var listSize = byte shr 4
            if (listSize == 15) {
                listSize = readVarInt(reader)
            }
            val boolType = elemType == CompactType.TRUE || elemType == CompactType.FALSE
            val values = mutableListOf<ThriftType>()
            for (i in 0 until listSize) {
                val elem = if (boolType) {
                    val byteVal = readElement(reader, CompactType.BYTE)
                    ThriftType.BooleanType((byteVal as ThriftType.IntType).value == 1)
                } else {
                    readElement(reader, elemType)
                }
                values.add(elem)
            }
            ThriftType.ListType(values)
        }
        CompactType.STRUCT -> {
            val structValues = mutableListOf<ThriftType?>()
            var lastFid = 0
            while (true) {
                val (fieldType, fid, newLastFid) = readFieldBegin(reader, lastFid)
                lastFid = newLastFid
                if (fieldType == CompactType.STOP) {
                    break
                }
                // Expand list to accommodate fid
                while (structValues.size <= fid) {
                    structValues.add(null)
                }
                structValues[fid] = readElement(reader, fieldType)
            }
            ThriftType.ObjectType(structValues)
        }
        // TODO: MAP, SET, UUID
        else -> throw Error("thrift unhandled type: $type")
    }
}

/**
 * Var int aka Unsigned LEB128.
 * Reads groups of 7 low bits until high bit is 0.
 */
fun readVarInt(reader: DataReader): Int {
    var result = 0
    var shift = 0
    while (true) {
        val byte = reader.view.get(reader.offset).toInt() and 0xFF
        reader.offset++
        result = result or ((byte and 0x7f) shl shift)
        if ((byte and 0x80) == 0) {
            return result
        }
        shift += 7
    }
}

/**
 * Read a varint as a long.
 */
private fun readVarLong(reader: DataReader): Long {
    var result = 0L
    var shift = 0
    while (true) {
        val byte = reader.view.get(reader.offset).toLong() and 0xFF
        reader.offset++
        result = result or ((byte and 0x7f) shl shift)
        if ((byte and 0x80) == 0L) {
            return result
        }
        shift += 7
    }
}

/**
 * Values of type int32 and int64 are transformed to a zigzag int.
 * A zigzag int folds positive and negative numbers into the positive number space.
 */
fun readZigZag(reader: DataReader): Int {
    val zigzag = readVarInt(reader)
    // convert zigzag to int
    return (zigzag ushr 1) xor -(zigzag and 1)
}

/**
 * A zigzag int folds positive and negative numbers into the positive number space.
 * This version returns a Long.
 */
fun readZigZagLong(reader: DataReader): Long {
    val zigzag = readVarLong(reader)
    // convert zigzag to long
    return (zigzag ushr 1) xor -(zigzag and 1)
}

// Alias for compatibility
fun readZigZagBigInt(reader: DataReader): Long = readZigZagLong(reader)

/**
 * Read field type and field id
 *
 * @returns [type, fid, newLastFid]
 */
private fun readFieldBegin(reader: DataReader, lastFid: Int): Triple<Int, Int, Int> {
    val byte = reader.view.get(reader.offset).toInt() and 0xFF
    reader.offset++
    val type = byte and 0x0f
    if (type == CompactType.STOP) {
        // STOP also ends a struct
        return Triple(0, 0, lastFid)
    }
    val delta = byte shr 4
    val fid = if (delta > 0) lastFid + delta else readZigZag(reader)
    return Triple(type, fid, fid)
}
