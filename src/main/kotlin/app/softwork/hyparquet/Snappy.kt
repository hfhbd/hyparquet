package app.softwork.hyparquet

/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Zhipeng Jia
 * https://github.com/zhipeng-jia/snappyjs
 */

private val WORD_MASK = intArrayOf(0, 0xff, 0xffff, 0xffffff, -1)

/**
 * Copy bytes from one array to another
 *
 * @param fromArray source array
 * @param fromPos source position
 * @param toArray destination array
 * @param toPos destination position
 * @param length number of bytes to copy
 */
private fun copyBytes(fromArray: ByteArray, fromPos: Int, toArray: ByteArray, toPos: Int, length: Int) {
    System.arraycopy(fromArray, fromPos, toArray, toPos, length)
}

/**
 * Decompress snappy data.
 * Accepts an output buffer to avoid allocating a new buffer for each call.
 *
 * @param input compressed data
 * @param output output buffer
 */
fun snappyUncompress(input: ByteArray, output: ByteArray) {
    val inputLength = input.size
    val outputLength = output.size
    var pos = 0
    var outPos = 0

    // skip preamble (contains uncompressed length as varint)
    while (pos < inputLength) {
        val c = input[pos].toInt() and 0xFF
        pos++
        if (c < 128) {
            break
        }
    }
    if (outputLength > 0 && pos >= inputLength) {
        throw Error("invalid snappy length header")
    }

    while (pos < inputLength) {
        val c = input[pos].toInt() and 0xFF
        var len: Int
        pos++

        if (pos >= inputLength) {
            throw Error("missing eof marker")
        }

        // There are two types of elements, literals and copies (back references)
        if ((c and 0x3) == 0) {
            // Literals are uncompressed data stored directly in the byte stream
            len = (c ushr 2) + 1
            // Longer literal length is encoded in multiple bytes
            if (len > 60) {
                if (pos + 3 >= inputLength) {
                    throw Error("snappy error literal pos + 3 >= inputLength")
                }
                val lengthSize = len - 60 // length bytes - 1
                len = (input[pos].toInt() and 0xFF) +
                      ((input[pos + 1].toInt() and 0xFF) shl 8) +
                      ((input[pos + 2].toInt() and 0xFF) shl 16) +
                      ((input[pos + 3].toInt() and 0xFF) shl 24)
                len = (len and WORD_MASK[lengthSize]) + 1
                pos += lengthSize
            }
            if (pos + len > inputLength) {
                throw Error("snappy error literal exceeds input length")
            }
            copyBytes(input, pos, output, outPos, len)
            pos += len
            outPos += len
        } else {
            // Copy elements
            var offset = 0 // offset back from current position to read
            when (c and 0x3) {
                1 -> {
                    // Copy with 1-byte offset
                    len = ((c ushr 2) and 0x7) + 4
                    offset = (input[pos].toInt() and 0xFF) + ((c ushr 5) shl 8)
                    pos++
                }
                2 -> {
                    // Copy with 2-byte offset
                    if (inputLength <= pos + 1) {
                        throw Error("snappy error end of input")
                    }
                    len = (c ushr 2) + 1
                    offset = (input[pos].toInt() and 0xFF) + ((input[pos + 1].toInt() and 0xFF) shl 8)
                    pos += 2
                }
                3 -> {
                    // Copy with 4-byte offset
                    if (inputLength <= pos + 3) {
                        throw Error("snappy error end of input")
                    }
                    len = (c ushr 2) + 1
                    offset = (input[pos].toInt() and 0xFF) +
                             ((input[pos + 1].toInt() and 0xFF) shl 8) +
                             ((input[pos + 2].toInt() and 0xFF) shl 16) +
                             ((input[pos + 3].toInt() and 0xFF) shl 24)
                    pos += 4
                }
                else -> {
                    len = 0
                    offset = 0
                }
            }
            if (offset == 0 || offset.toDouble().isNaN()) {
                throw Error("invalid offset $offset pos $pos inputLength $inputLength")
            }
            if (offset > outPos) {
                throw Error("cannot copy from before start of buffer")
            }
            copyBytes(output, outPos - offset, output, outPos, len)
            outPos += len
        }
    }

    if (outPos != outputLength) throw Error("premature end of input")
}
