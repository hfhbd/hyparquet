package app.softwork.hyparquet

import kotlin.math.ceil

/**
 * @param reader
 * @param count number of values to read
 * @param output
 */
fun deltaBinaryUnpack(reader: DataReader, count: Int, output: IntArray) {
    val blockSize = readVarInt(reader)
    val miniblockPerBlock = readVarInt(reader)
    readVarInt(reader) // assert(=== count)
    var value = readZigZagBigInt(reader) // first value
    var outputIndex = 0
    output[outputIndex++] = value.toInt()

    val valuesPerMiniblock = blockSize / miniblockPerBlock

    while (outputIndex < count) {
        // new block
        val minDelta = readZigZagBigInt(reader)
        val bitWidths = ByteArray(miniblockPerBlock)
        for (i in 0 until miniblockPerBlock) {
            bitWidths[i] = reader.view.get(reader.offset).toByte()
            reader.offset++
        }

        for (i in 0 until miniblockPerBlock) {
            if (outputIndex >= count) break
            // new miniblock
            val bitWidth = bitWidths[i].toLong()
            if (bitWidth > 0) {
                var bitpackPos = 0L
                var miniblockCount = valuesPerMiniblock
                val mask = (1L shl bitWidth.toInt()) - 1L
                while (miniblockCount > 0 && outputIndex < count) {
                    var bits = (reader.view.get(reader.offset).toLong() and 0xFF) shr bitpackPos.toInt() and mask
                    bitpackPos += bitWidth
                    while (bitpackPos >= 8) {
                        bitpackPos -= 8
                        reader.offset++
                        if (bitpackPos > 0) {
                            bits = bits or ((reader.view.get(reader.offset).toLong() and 0xFF) shl (bitWidth - bitpackPos).toInt() and mask)
                        }
                    }
                    val delta = minDelta + bits
                    value += delta
                    output[outputIndex++] = value.toInt()
                    miniblockCount--
                }
                if (miniblockCount > 0) {
                    // consume leftover miniblock
                    reader.offset += ceil((miniblockCount * bitWidth + bitpackPos) / 8.0).toInt()
                }
            } else {
                for (j in 0 until valuesPerMiniblock) {
                    if (outputIndex >= count) break
                    value += minDelta
                    output[outputIndex++] = value.toInt()
                }
            }
        }
    }
}

fun deltaBinaryUnpack(reader: DataReader, count: Int, output: LongArray) {
    val blockSize = readVarInt(reader)
    val miniblockPerBlock = readVarInt(reader)
    readVarInt(reader) // assert(=== count)
    var value = readZigZagBigInt(reader) // first value
    var outputIndex = 0
    output[outputIndex++] = value

    val valuesPerMiniblock = blockSize / miniblockPerBlock

    while (outputIndex < count) {
        // new block
        val minDelta = readZigZagBigInt(reader)
        val bitWidths = ByteArray(miniblockPerBlock)
        for (i in 0 until miniblockPerBlock) {
            bitWidths[i] = reader.view.get(reader.offset).toByte()
            reader.offset++
        }

        for (i in 0 until miniblockPerBlock) {
            if (outputIndex >= count) break
            // new miniblock
            val bitWidth = bitWidths[i].toLong()
            if (bitWidth > 0) {
                var bitpackPos = 0L
                var miniblockCount = valuesPerMiniblock
                val mask = (1L shl bitWidth.toInt()) - 1L
                while (miniblockCount > 0 && outputIndex < count) {
                    var bits = (reader.view.get(reader.offset).toLong() and 0xFF) shr bitpackPos.toInt() and mask
                    bitpackPos += bitWidth
                    while (bitpackPos >= 8) {
                        bitpackPos -= 8
                        reader.offset++
                        if (bitpackPos > 0) {
                            bits = bits or ((reader.view.get(reader.offset).toLong() and 0xFF) shl (bitWidth - bitpackPos).toInt() and mask)
                        }
                    }
                    val delta = minDelta + bits
                    value += delta
                    output[outputIndex++] = value
                    miniblockCount--
                }
                if (miniblockCount > 0) {
                    // consume leftover miniblock
                    reader.offset += ceil((miniblockCount * bitWidth + bitpackPos) / 8.0).toInt()
                }
            } else {
                for (j in 0 until valuesPerMiniblock) {
                    if (outputIndex >= count) break
                    value += minDelta
                    output[outputIndex++] = value
                }
            }
        }
    }
}

/**
 * @param reader
 * @param count
 * @param output
 */
fun deltaLengthByteArray(reader: DataReader, count: Int, output: Array<ByteArray?>) {
    val lengths = IntArray(count)
    deltaBinaryUnpack(reader, count, lengths)
    for (i in 0 until count) {
        val length = lengths[i]
        output[i] = ByteArray(length)
        reader.view.position(reader.offset)
        reader.view.get(output[i]!!)
        reader.offset += length
    }
}

fun deltaByteArray(reader: DataReader, count: Int, output: Array<ByteArray?>) {
    val prefixData = IntArray(count)
    deltaBinaryUnpack(reader, count, prefixData)
    val suffixData = IntArray(count)
    deltaBinaryUnpack(reader, count, suffixData)

    for (i in 0 until count) {
        val suffixLength = suffixData[i]
        val suffix = ByteArray(suffixLength)
        reader.view.position(reader.offset)
        reader.view.get(suffix)
        
        if (prefixData[i] > 0) {
            // copy from previous value
            val prefixLength = prefixData[i]
            output[i] = ByteArray(prefixLength + suffixLength)
            output[i - 1]?.let { prev ->
                System.arraycopy(prev, 0, output[i]!!, 0, prefixLength)
            }
            System.arraycopy(suffix, 0, output[i]!!, prefixLength, suffixLength)
        } else {
            output[i] = suffix
        }
        reader.offset += suffixLength
    }
}
