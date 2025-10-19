package app.softwork.hyparquet

/**
 * Read column chunk from file
 */
suspend fun readColumn(
    file: AsyncBuffer,
    columnMetaData: ColumnMetaData,
    columnDecoder: ColumnDecoder,
    schemaPath: List<SchemaTree>,
    rowGroupSelect: RowGroupSelect
): List<DecodedArray> {
    val codec = columnMetaData.codec
    val compressors = columnDecoder.compressors
    val result = mutableListOf<DecodedArray>()
    
    // Read column chunk data
    val columnOffset = columnMetaData.dictionary_page_offset ?: columnMetaData.data_page_offset
    val columnSize = columnMetaData.total_compressed_size
    val columnData = file.slice(columnOffset, columnOffset + columnSize)
    
    val buffer = java.nio.ByteBuffer.wrap(columnData).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    val reader = DataReader(buffer, 0)
    
    var dictionary: DecodedArray? = null
    var rowsRead = 0
    val targetRows = rowGroupSelect.selectEnd - rowGroupSelect.selectStart
    
    // Read pages until we have enough data
    while (reader.offset < buffer.limit() && rowsRead < targetRows) {
        val pageHeader = readDataPageHeader(reader)
        
        when (pageHeader.type) {
            PageType.DICTIONARY_PAGE -> {
                // Read dictionary page
                val compressedBytes = ByteArray(pageHeader.compressed_page_size)
                buffer.position(reader.offset)
                buffer.get(compressedBytes)
                reader.offset += pageHeader.compressed_page_size
                
                val decompressedBytes = decompressPage(
                    compressedBytes,
                    codec,
                    pageHeader.uncompressed_page_size,
                    compressors
                )
                
                val dictReader = DataReader(
                    java.nio.ByteBuffer.wrap(decompressedBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN),
                    0
                )
                
                val type = columnDecoder.element.type ?: throw Error("parquet element type required")
                dictionary = readPlain(dictReader, type, pageHeader.dictionary_page_header?.num_values ?: 0, columnDecoder.element.type_length)
            }
            
            PageType.DATA_PAGE, PageType.DATA_PAGE_V2 -> {
                // Read data page
                val compressedBytes = ByteArray(pageHeader.compressed_page_size)
                buffer.position(reader.offset)
                buffer.get(compressedBytes)
                reader.offset += pageHeader.compressed_page_size
                
                val decompressedBytes = decompressPage(
                    compressedBytes,
                    codec,
                    pageHeader.uncompressed_page_size,
                    compressors
                )
                
                val pageReader = DataReader(
                    java.nio.ByteBuffer.wrap(decompressedBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN),
                    0
                )
                
                val dataPage = readDataPage(pageReader, pageHeader, columnDecoder, schemaPath)
                
                // Convert data
                val encoding = pageHeader.data_page_header?.encoding ?: Encoding.PLAIN
                val convertedData = convertWithDictionary(dataPage.dataPage, dictionary, encoding, columnDecoder)
                result.add(convertedData)
                
                rowsRead += pageHeader.data_page_header?.num_values ?: 0
            }
            
            else -> {
                // Skip unknown page types
                reader.offset += pageHeader.compressed_page_size
            }
        }
    }
    
    return result
}
