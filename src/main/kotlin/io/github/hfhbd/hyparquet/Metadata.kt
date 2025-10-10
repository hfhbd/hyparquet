package io.github.hfhbd.hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.math.pow

const val DEFAULT_INITIAL_FETCH_SIZE = 1 shl 19 // 512kb

/**
 * Read parquet metadata from an async buffer.
 */
suspend fun parquetMetadataAsync(
    asyncBuffer: AsyncBuffer,
    parsers: ParquetParsers = DefaultParsers,
    initialFetchSize: Int = DEFAULT_INITIAL_FETCH_SIZE
): FileMetaData {
    if (asyncBuffer.byteLength < 0) throw IllegalArgumentException("Invalid AsyncBuffer")

    // Fetch last bytes (footer) of the file
    val footerOffset = maxOf(0, asyncBuffer.byteLength - initialFetchSize)
    val footerBuffer = asyncBuffer.slice(footerOffset, asyncBuffer.byteLength)

    // Check for parquet magic number "PAR1"
    if (footerBuffer.size < 8) {
        throw IllegalArgumentException("Parquet file is too short")
    }

    val footerView = ByteBuffer.wrap(footerBuffer).order(ByteOrder.LITTLE_ENDIAN)
    if (footerView.getInt(footerBuffer.size - 4) != 0x31524150) {
        throw IllegalArgumentException("Parquet file invalid (footer != PAR1)")
    }

    // Parquet files store metadata at the end of the file
    // Metadata length is 4 bytes before the last PAR1
    val metadataLength = footerView.getInt(footerBuffer.size - 8)
    if (metadataLength > asyncBuffer.byteLength - 8) {
        throw IllegalArgumentException(
            "Parquet metadata length $metadataLength exceeds available buffer ${asyncBuffer.byteLength - 8}"
        )
    }

    // Check if metadata size fits inside the initial fetch
    return if (metadataLength + 8 > initialFetchSize) {
        // Fetch the rest of the metadata
        val metadataOffset = asyncBuffer.byteLength - metadataLength - 8
        val metadataBuffer = asyncBuffer.slice(metadataOffset, footerOffset)
        
        // Combine initial fetch with the new slice
        val combinedBuffer = ByteArray(metadataLength + 8)
        metadataBuffer.copyInto(combinedBuffer, 0)
        footerBuffer.copyInto(combinedBuffer, footerOffset - metadataOffset)
        
        parquetMetadata(combinedBuffer, parsers)
    } else {
        // Parse metadata from the footer
        parquetMetadata(footerBuffer, parsers)
    }
}

/**
 * Read parquet metadata from a buffer synchronously.
 */
fun parquetMetadata(
    arrayBuffer: ByteArray,
    parsers: ParquetParsers = DefaultParsers
): FileMetaData {
    val view = ByteBuffer.wrap(arrayBuffer).order(ByteOrder.LITTLE_ENDIAN)

    // Validate footer magic number "PAR1"
    if (view.capacity() < 8) {
        throw IllegalArgumentException("Parquet file is too short")
    }
    if (view.getInt(view.capacity() - 4) != 0x31524150) {
        throw IllegalArgumentException("Parquet file invalid (footer != PAR1)")
    }

    // Parquet files store metadata at the end of the file
    // Metadata length is 4 bytes before the last PAR1
    val metadataLengthOffset = view.capacity() - 8
    val metadataLength = view.getInt(metadataLengthOffset)
    if (metadataLength > view.capacity() - 8) {
        throw IllegalArgumentException(
            "Parquet metadata length $metadataLength exceeds buffer size ${view.capacity() - 8}"
        )
    }

    // Parse thrift metadata
    val metadataOffset = view.capacity() - metadataLength - 8
    val metadataBuffer = arrayBuffer.sliceArray(metadataOffset until metadataOffset + metadataLength)
    
    // Parse using thrift deserializer (this would need to be implemented)
    val thriftMetadata = deserializeTCompactProtocol(metadataBuffer)
    
    // Convert thrift metadata to our FileMetaData structure
    return convertThriftToFileMetaData(thriftMetadata, parsers)
}

/**
 * Convert metadata value based on schema and parsers
 */
fun convertMetadata(
    value: ByteArray?,
    schema: SchemaElement,
    parsers: ParquetParsers
): Any? {
    if (value == null) return null
    
    val type = schema.type
    val convertedType = schema.convertedType
    val logicalType = schema.logicalType

    when (type) {
        ParquetType.BOOLEAN -> return value[0] == 1.toByte()
        ParquetType.BYTE_ARRAY -> return parsers.stringFromBytes(value)
        ParquetType.FLOAT -> {
            if (value.size == 4) {
                val buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN)
                return buffer.getFloat()
            }
        }
        ParquetType.DOUBLE -> {
            if (value.size == 8) {
                val buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN)
                return buffer.getDouble()
            }
        }
        ParquetType.INT32 -> {
            if (convertedType == ConvertedType.DATE) {
                val buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN)
                return parsers.dateFromDays(buffer.getInt())
            }
            if (value.size == 4) {
                val buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN)
                return buffer.getInt()
            }
        }
        ParquetType.INT64 -> {
            if (logicalType is LogicalType.TIMESTAMP) {
                val buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN)
                return parsers.timestampFromMilliseconds(buffer.getLong())
            }
            if (value.size == 8) {
                val buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN)
                return buffer.getLong()
            }
        }
        ParquetType.FIXED_LEN_BYTE_ARRAY -> return value
        else -> {
            // Handle other cases
            when (convertedType) {
                ConvertedType.DECIMAL -> {
                    return parseDecimal(value) * 10.0.pow(-(schema.scale ?: 0).toDouble())
                }
                else -> {
                    if (logicalType is LogicalType.FLOAT16) {
                        return parseFloat16(value)
                    }
                }
            }
        }
    }
    
    return value
}

/**
 * Return a tree of schema elements from parquet metadata.
 */
fun parquetSchema(metadata: FileMetaData): SchemaTree {
    return getSchemaPath(metadata.schema, emptyList())[0]
}

// Placeholder functions that would need to be implemented
private fun deserializeTCompactProtocol(buffer: ByteArray): Map<String, Any> {
    // This would need to implement Thrift compact protocol deserialization
    // For now, throw an exception to indicate it needs implementation
    throw NotImplementedError("Thrift deserialization not yet implemented")
}

private fun convertThriftToFileMetaData(thriftData: Map<String, Any>, parsers: ParquetParsers): FileMetaData {
    // This would convert from thrift format to our FileMetaData structure
    // For now, throw an exception to indicate it needs implementation
    throw NotImplementedError("Thrift to FileMetaData conversion not yet implemented")
}

private fun getSchemaPath(schema: List<SchemaElement>, path: List<String>): List<SchemaTree> {
    // This would need to implement schema tree building
    // For now, throw an exception to indicate it needs implementation
    throw NotImplementedError("Schema path building not yet implemented")
}