package hyparquet

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
fun deserializeTCompactProtocol(reader: DataReader): Map<String, ThriftType> {
    var lastFid = 0
    val value = mutableMapOf<String, ThriftType>()

    while (reader.offset < reader.view.limit()) {
        // Parse each field based on its type and add to the result object
        val (type, fid, newLastFid) = readFieldBegin(reader, lastFid)
        lastFid = newLastFid

        if (type == CompactType.STOP) {
            break
        }

        // Handle the field based on its type
        value["field_$fid"] = readElement(reader, type)
    }

    return value
}

/**
 * Read a single element based on its type
 */
fun readElement(reader: DataReader, type: Int): ThriftType {
    return when (type) {
        CompactType.TRUE -> ThriftType.BooleanType(true)
        CompactType.FALSE -> ThriftType.BooleanType(false)
        CompactType.BYTE -> {
            val byte = reader.view.get(reader.offset)
            reader.offset++
            ThriftType.NumberType(byte.toDouble())
        }
        CompactType.I16, CompactType.I32 -> {
            ThriftType.NumberType(readZigZag(reader).toDouble())
        }
        CompactType.I64 -> {
            ThriftType.BigIntType(readZigZagLong(reader))
        }
        CompactType.DOUBLE -> {
            reader.view.position(reader.offset)
            val double = reader.view.double
            reader.offset += 8
            ThriftType.NumberType(double)
        }
        CompactType.BINARY -> {
            val length = readVarInt(reader)
            val bytes = ByteArray(length)
            for (i in 0 until length) {
                bytes[i] = reader.view.get(reader.offset + i)
            }
            reader.offset += length
            ThriftType.ByteArrayType(bytes)
        }
        CompactType.LIST -> {
            val listHeader = reader.view.get(reader.offset)
            reader.offset++
            val elementType = listHeader.toInt() and 0x0f
            val size = if ((listHeader.toInt() and 0xf0) shr 4 == 15) {
                readVarInt(reader)
            } else {
                (listHeader.toInt() and 0xf0) shr 4
            }
            val list = mutableListOf<ThriftType>()
            repeat(size) {
                list.add(readElement(reader, elementType))
            }
            ThriftType.ListType(list)
        }
        CompactType.STRUCT -> {
            val structValues = mutableMapOf<String, ThriftType>()
            var lastFid = 0
            while (true) {
                val (fieldType, fid, newLastFid) = readFieldBegin(reader, lastFid)
                lastFid = newLastFid
                if (fieldType == CompactType.STOP) {
                    break
                }
                structValues["field_$fid"] = readElement(reader, fieldType)
            }
            ThriftType.ObjectType(structValues)
        }
        else -> throw Error("thrift unhandled type: $type")
    }
}

/**
 * Read field begin
 */
private fun readFieldBegin(reader: DataReader, lastFid: Int): Triple<Int, Int, Int> {
    if (reader.offset >= reader.view.limit()) {
        return Triple(CompactType.STOP, 0, lastFid)
    }
    
    val byte = reader.view.get(reader.offset)
    reader.offset++
    val typeAndDelta = byte.toInt() and 0xff
    val type = typeAndDelta and 0x0f
    val delta = (typeAndDelta and 0xf0) shr 4
    val fid = if (delta == 0) {
        readZigZag(reader)
    } else {
        lastFid + delta
    }
    return Triple(type, fid, fid)
}

/**
 * Var int aka Unsigned LEB128.
 * Reads groups of 7 low bits until high bit is 0.
 */
fun readVarInt(reader: DataReader): Int {
    var result = 0
    var shift = 0
    while (reader.offset < reader.view.limit()) {
        val byte = reader.view.get(reader.offset)
        reader.offset++
        result = result or ((byte.toInt() and 0x7f) shl shift)
        if ((byte.toInt() and 0x80) == 0) {
            return result
        }
        shift += 7
    }
    throw Error("Unexpected end of buffer reading varint")
}

/**
 * Read a varint as a long.
 */
fun readVarLong(reader: DataReader): Long {
    var result = 0L
    var shift = 0
    while (reader.offset < reader.view.limit()) {
        val byte = reader.view.get(reader.offset)
        reader.offset++
        result = result or ((byte.toLong() and 0x7f) shl shift)
        if ((byte.toInt() and 0x80) == 0) {
            return result
        }
        shift += 7
    }
    throw Error("Unexpected end of buffer reading varlong")
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
 * ZigZag decode for long values
 */
fun readZigZagLong(reader: DataReader): Long {
    val zigzag = readVarLong(reader)
    // convert zigzag to long
    return (zigzag ushr 1) xor -(zigzag and 1L)
}

