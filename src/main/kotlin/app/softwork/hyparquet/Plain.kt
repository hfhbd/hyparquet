package app.softwork.hyparquet

import java.nio.ByteBuffer
import kotlin.math.ceil

/**
 * Read `count` values of the given type from the reader.view.
 *
 * @param reader - buffer to read data from
 * @param type - parquet type of the data
 * @param count - number of values to read
 * @param fixedLength - length of each fixed length byte array
 * @returns array of values
 */
fun readPlain(reader: DataReader, type: ParquetType, count: Int, fixedLength: Int?): DecodedArray {
    if (count == 0) return DecodedArray.AnyArrayType(emptyList())
    return when (type) {
        ParquetType.BOOLEAN -> readPlainBoolean(reader, count)
        ParquetType.INT32 -> readPlainInt32(reader, count)
        ParquetType.INT64 -> readPlainInt64(reader, count)
        ParquetType.INT96 -> readPlainInt96(reader, count)
        ParquetType.FLOAT -> readPlainFloat(reader, count)
        ParquetType.DOUBLE -> readPlainDouble(reader, count)
        ParquetType.BYTE_ARRAY -> readPlainByteArray(reader, count)
        ParquetType.FIXED_LEN_BYTE_ARRAY -> {
            if (fixedLength == null) throw Error("parquet missing fixed length")
            readPlainByteArrayFixed(reader, count, fixedLength)
        }
    }
}

/**
 * Read `count` boolean values.
 *
 * @param reader
 * @param count
 * @returns
 */
fun readPlainBoolean(reader: DataReader, count: Int): DecodedArray {
    val values = BooleanArray(count)
    for (i in 0 until count) {
        val byteOffset = reader.offset + (i / 8)
        val bitOffset = i % 8
        val byte = reader.view.get(byteOffset).toInt() and 0xFF
        values[i] = (byte and (1 shl bitOffset)) != 0
    }
    reader.offset += ceil(count / 8.0).toInt()
    return DecodedArray.AnyArrayType(values.toList())
}

/**
 * Read `count` int32 values.
 *
 * @param reader
 * @param count
 * @returns
 */
fun readPlainInt32(reader: DataReader, count: Int): DecodedArray {
    val values = IntArray(count)
    reader.view.position(reader.offset)
    for (i in 0 until count) {
        values[i] = reader.view.getInt()
    }
    reader.offset += count * 4
    return DecodedArray.IntArrayType(values)
}

/**
 * Read `count` int64 values.
 *
 * @param reader
 * @param count
 * @returns
 */
fun readPlainInt64(reader: DataReader, count: Int): DecodedArray {
    val values = LongArray(count)
    reader.view.position(reader.offset)
    for (i in 0 until count) {
        values[i] = reader.view.getLong()
    }
    reader.offset += count * 8
    return DecodedArray.LongArrayType(values)
}

/**
 * Read `count` int96 values.
 *
 * @param reader
 * @param count
 * @returns
 */
fun readPlainInt96(reader: DataReader, count: Int): DecodedArray {
    val values = Array<Long>(count) { 0L }
    for (i in 0 until count) {
        reader.view.position(reader.offset + i * 12)
        val low = reader.view.getLong()
        val high = reader.view.getInt()
        values[i] = (high.toLong() shl 64) or low
    }
    reader.offset += count * 12
    return DecodedArray.AnyArrayType(values.toList())
}

/**
 * Read `count` float values.
 *
 * @param reader
 * @param count
 * @returns
 */
fun readPlainFloat(reader: DataReader, count: Int): DecodedArray {
    val values = FloatArray(count)
    reader.view.position(reader.offset)
    for (i in 0 until count) {
        values[i] = reader.view.getFloat()
    }
    reader.offset += count * 4
    return DecodedArray.FloatArrayType(values)
}

/**
 * Read `count` double values.
 *
 * @param reader
 * @param count
 * @returns
 */
fun readPlainDouble(reader: DataReader, count: Int): DecodedArray {
    val values = DoubleArray(count)
    reader.view.position(reader.offset)
    for (i in 0 until count) {
        values[i] = reader.view.getDouble()
    }
    reader.offset += count * 8
    return DecodedArray.DoubleArrayType(values)
}

/**
 * Read `count` byte array values.
 *
 * @param reader
 * @param count
 * @returns
 */
fun readPlainByteArray(reader: DataReader, count: Int): DecodedArray {
    val values = Array<ByteArray?>(count) { null }
    for (i in 0 until count) {
        reader.view.position(reader.offset)
        val length = reader.view.getInt()
        reader.offset += 4
        values[i] = ByteArray(length)
        reader.view.get(values[i]!!)
        reader.offset += length
    }
    return DecodedArray.AnyArrayType(values.toList())
}

/**
 * Read a fixed length byte array.
 *
 * @param reader
 * @param count
 * @param fixedLength
 * @returns
 */
fun readPlainByteArrayFixed(reader: DataReader, count: Int, fixedLength: Int): DecodedArray {
    val values = Array<ByteArray?>(count) { null }
    for (i in 0 until count) {
        values[i] = ByteArray(fixedLength)
        reader.view.position(reader.offset)
        reader.view.get(values[i]!!)
        reader.offset += fixedLength
    }
    return DecodedArray.AnyArrayType(values.toList())
}
