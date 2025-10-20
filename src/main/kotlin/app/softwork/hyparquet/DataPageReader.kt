package app.softwork.hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Read data page header from reader
 */
fun readDataPageHeader(reader: DataReader): PageHeader {
    val metadata = deserializeTCompactProtocol(reader)
    
    val type = (metadata.getOrNull(1) as? ThriftType.IntType)?.value?.let { PageType.values()[it] } ?: PageType.DATA_PAGE
    val uncompressed_page_size = (metadata.getOrNull(2) as? ThriftType.IntType)?.value ?: 0
    val compressed_page_size = (metadata.getOrNull(3) as? ThriftType.IntType)?.value ?: 0
    
    val data_page_header = if (type == PageType.DATA_PAGE) {
        (metadata.getOrNull(5) as? ThriftType.ObjectType)?.value?.let { parseDataPageHeader(it) }
    } else null
    
    val dictionary_page_header = if (type == PageType.DICTIONARY_PAGE) {
        (metadata.getOrNull(6) as? ThriftType.ObjectType)?.value?.let { parseDictionaryPageHeader(it) }
    } else null
    
    val data_page_header_v2 = if (type == PageType.DATA_PAGE_V2) {
        (metadata.getOrNull(7) as? ThriftType.ObjectType)?.value?.let { parseDataPageHeaderV2(it) }
    } else null
    
    return PageHeader(type, uncompressed_page_size, compressed_page_size, data_page_header, dictionary_page_header, data_page_header_v2)
}

private fun parseDataPageHeader(data: ThriftObject): DataPageHeader {
    val num_values = (data.getOrNull(1) as? ThriftType.IntType)?.value ?: 0
    val encoding = (data.getOrNull(2) as? ThriftType.IntType)?.value?.let { Encoding.values()[it] } ?: Encoding.PLAIN
    return DataPageHeader(num_values, encoding)
}

private fun parseDictionaryPageHeader(data: ThriftObject): DictionaryPageHeader {
    val num_values = (data.getOrNull(1) as? ThriftType.IntType)?.value ?: 0
    return DictionaryPageHeader(num_values)
}

private fun parseDataPageHeaderV2(data: ThriftObject): DataPageHeaderV2 {
    val num_values = (data.getOrNull(1) as? ThriftType.IntType)?.value ?: 0
    val num_nulls = (data.getOrNull(2) as? ThriftType.IntType)?.value ?: 0
    val num_rows = (data.getOrNull(3) as? ThriftType.IntType)?.value ?: 0
    val encoding = (data.getOrNull(4) as? ThriftType.IntType)?.value?.let { Encoding.values()[it] } ?: Encoding.PLAIN
    val definition_levels_byte_length = (data.getOrNull(5) as? ThriftType.IntType)?.value ?: 0
    val repetition_levels_byte_length = (data.getOrNull(6) as? ThriftType.IntType)?.value ?: 0
    val is_compressed = (data.getOrNull(7) as? ThriftType.BooleanType)?.value
    
    return DataPageHeaderV2(num_values, num_nulls, num_rows, encoding, definition_levels_byte_length, repetition_levels_byte_length, is_compressed)
}

/**
 * Decompress a data page if needed
 */
fun decompressPage(compressedBytes: ByteArray, codec: CompressionCodec, uncompressedSize: Int, compressors: Compressors?): ByteArray {
    return when (codec) {
        CompressionCodec.UNCOMPRESSED -> compressedBytes
        CompressionCodec.SNAPPY -> {
            val output = ByteArray(uncompressedSize)
            snappyUncompress(compressedBytes, output)
            output
        }
        else -> {
            val decompressor = compressors?.get(codec)
                ?: throw Error("parquet codec $codec not supported")
            decompressor(compressedBytes, uncompressedSize)
        }
    }
}

/**
 * Read and decode a data page
 */
fun readDataPage(
    reader: DataReader,
    header: PageHeader,
    columnDecoder: ColumnDecoder,
    schemaPath: List<SchemaTree>
): DataPage {
    val element = columnDecoder.element
    val type = element.type ?: throw Error("parquet element type required")
    
    // Read definition and repetition levels
    val maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
    val maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
    
    val definitionLevels = if (maxDefinitionLevel > 0) {
        val levels = IntArray(header.data_page_header?.num_values ?: 0)
        readRleBitPackedHybrid(reader, bitWidth(maxDefinitionLevel), levels)
        levels
    } else null
    
    val repetitionLevels = if (maxRepetitionLevel > 0) {
        val levels = IntArray(header.data_page_header?.num_values ?: 0)
        readRleBitPackedHybrid(reader, bitWidth(maxRepetitionLevel), levels)
        levels
    } else {
        IntArray(header.data_page_header?.num_values ?: 0)
    }
    
    // Read data
    val encoding = header.data_page_header?.encoding ?: Encoding.PLAIN
    val count = header.data_page_header?.num_values ?: 0
    val dataPage = when (encoding) {
        Encoding.PLAIN -> readPlain(reader, type, count, element.type_length)
        else -> readPlain(reader, type, count, element.type_length) // Simplified
    }
    
    return DataPage(definitionLevels, repetitionLevels, dataPage)
}
