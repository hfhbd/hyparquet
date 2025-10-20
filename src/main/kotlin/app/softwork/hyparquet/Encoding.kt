package app.softwork.hyparquet

import java.nio.ByteBuffer
import kotlin.math.ceil

/**
 * Minimum bits needed to store value.
 */
fun bitWidth(value: Int): Int {
    return 32 - value.countLeadingZeroBits()
}

/**
 * Read values from a run-length encoded/bit-packed hybrid encoding.
 *
 * If length is zero, then read int32 length at the start.
 *
 * @param reader
 * @param width - bitwidth
 * @param output
 * @param length - length of the encoded data
 */
fun readRleBitPackedHybrid(reader: DataReader, width: Int, output: IntArray, length: Int? = null) {
    val actualLength = length ?: run {
        val len = reader.view.getInt(reader.offset)
        reader.offset += 4
        len
    }
    val startOffset = reader.offset
    var seen = 0
    while (seen < output.size) {
        val header = readVarInt(reader)
        if (header and 1 != 0) {
            // bit-packed
            seen = readBitPacked(reader, header, width, output, seen)
        } else {
            // rle
            val count = header ushr 1
            readRle(reader, count, width, output, seen)
            seen += count
        }
    }
    reader.offset = startOffset + actualLength // duckdb writes an empty block
}

/**
 * Run-length encoding: read value with bitWidth and repeat it count times.
 *
 * @param reader
 * @param count
 * @param bitWidth
 * @param output
 * @param seen
 */
fun readRle(reader: DataReader, count: Int, bitWidth: Int, output: IntArray, seen: Int) {
    val width = (bitWidth + 7) shr 3
    var value = 0
    for (i in 0 until width) {
        value = value or ((reader.view.get(reader.offset).toInt() and 0xFF) shl (i shl 3))
        reader.offset++
    }
    // assert(value < 1 << bitWidth)

    // repeat value count times
    for (i in 0 until count) {
        output[seen + i] = value
    }
}

/**
 * Read a bit-packed run of the rle/bitpack hybrid.
 * Supports width > 8 (crossing bytes).
 *
 * @param reader
 * @param header - bit-pack header
 * @param bitWidth
 * @param output
 * @param seen
 * @returns total output values so far
 */
fun readBitPacked(reader: DataReader, header: Int, bitWidth: Int, output: IntArray, seen: Int): Int {
    var count = (header shr 1) shl 3 // values to read
    val mask = (1 shl bitWidth) - 1

    var data = if (reader.offset < reader.view.limit()) {
        reader.view.get(reader.offset).toInt() and 0xFF
    } else if (mask > 0) {
        // sometimes out-of-bounds reads are masked out
        throw Error("parquet bitpack offset ${reader.offset} out of range")
    } else {
        reader.offset++
        0
    }
    reader.offset++
    var left = 8
    var right = 0
    var seenCount = seen

    // read values
    while (count > 0) {
        // if we have crossed a byte boundary, shift the data
        if (right > 8) {
            right -= 8
            left -= 8
            data = data ushr 8
        } else if (left - right < bitWidth) {
            // if we don't have bitWidth number of bits to read, read next byte
            data = data or ((reader.view.get(reader.offset).toInt() and 0xFF) shl left)
            reader.offset++
            left += 8
        } else {
            if (seenCount < output.size) {
                // emit value
                output[seenCount++] = (data shr right) and mask
            }
            count--
            right += bitWidth
        }
    }

    return seenCount
}

/**
 * @param reader
 * @param count
 * @param type
 * @param typeLength
 * @returns
 */
fun byteStreamSplit(reader: DataReader, count: Int, type: ParquetType, typeLength: Int?): DecodedArray {
    val width = byteWidth(type, typeLength)
    val bytes = ByteArray(count * width)
    for (b in 0 until width) {
        for (i in 0 until count) {
            bytes[i * width + b] = reader.view.get(reader.offset)
            reader.offset++
        }
    }
    // interpret bytes as typed array
    val buffer = ByteBuffer.wrap(bytes)
    return when (type) {
        ParquetType.FLOAT -> {
            val array = FloatArray(count)
            for (i in 0 until count) {
                array[i] = buffer.getFloat()
            }
            DecodedArray.FloatArrayType(array)
        }
        ParquetType.DOUBLE -> {
            val array = DoubleArray(count)
            for (i in 0 until count) {
                array[i] = buffer.getDouble()
            }
            DecodedArray.DoubleArrayType(array)
        }
        ParquetType.INT32 -> {
            val array = IntArray(count)
            for (i in 0 until count) {
                array[i] = buffer.getInt()
            }
            DecodedArray.IntArrayType(array)
        }
        ParquetType.INT64 -> {
            val array = LongArray(count)
            for (i in 0 until count) {
                array[i] = buffer.getLong()
            }
            DecodedArray.LongArrayType(array)
        }
        ParquetType.FIXED_LEN_BYTE_ARRAY -> {
            // split into arrays of typeLength
            val split = Array(count) { i ->
                bytes.sliceArray(i * width until (i + 1) * width)
            }
            DecodedArray.AnyArrayType(split.toList())
        }
        else -> throw Error("parquet byte_stream_split unsupported type: $type")
    }
}

/**
 * @param type
 * @param typeLength
 * @returns
 */
fun byteWidth(type: ParquetType, typeLength: Int?): Int {
    return when (type) {
        ParquetType.INT32, ParquetType.FLOAT -> 4
        ParquetType.INT64, ParquetType.DOUBLE -> 8
        ParquetType.FIXED_LEN_BYTE_ARRAY -> typeLength ?: throw Error("parquet byteWidth missing type_length")
        else -> throw Error("parquet unsupported type: $type")
    }
}
