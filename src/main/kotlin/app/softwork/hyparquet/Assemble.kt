package app.softwork.hyparquet

/**
 * Assemble struct columns from nested columns
 */
fun assembleAsync(asyncRowGroup: AsyncRowGroup, schemaTree: SchemaTree): AsyncRowGroup {
    // For now, return as-is (simplified implementation)
    // A full implementation would reassemble nested structures
    return asyncRowGroup
}

/**
 * Transpose column-oriented data to row-oriented data
 */
suspend fun asyncGroupToRows(
    asyncColumns: List<AsyncColumn>,
    selectStart: Int,
    selectEnd: Int,
    rowFormat: RowFormat
): List<Any> {
    val rows = mutableListOf<Any>()
    
    if (asyncColumns.isEmpty()) return rows
    
    // Load all column data
    val columnDataList = asyncColumns.map { column ->
        val dataChunks = column.data()
        val flatData = flatten(dataChunks)
        column.pathInSchema to flatData
    }
    
    // Determine number of rows
    val numRows = if (columnDataList.isNotEmpty()) {
        getArrayLength(columnDataList[0].second)
    } else {
        0
    }
    
    val actualSelectEnd = minOf(selectEnd, numRows)
    
    // Transpose to rows
    for (rowIndex in selectStart until actualSelectEnd) {
        val row: Any = when (rowFormat) {
            RowFormat.OBJECT -> {
                val rowMap = mutableMapOf<String, Any?>()
                for ((pathInSchema, data) in columnDataList) {
                    val columnName = pathInSchema.lastOrNull() ?: "unknown"
                    val value = getArrayElement(data, rowIndex)
                    rowMap[columnName] = value
                }
                rowMap
            }
            RowFormat.ARRAY -> {
                val rowArray = mutableListOf<Any?>()
                for ((_, data) in columnDataList) {
                    val value = getArrayElement(data, rowIndex)
                    rowArray.add(value)
                }
                rowArray
            }
        }
        rows.add(row)
    }
    
    return rows
}

private fun getArrayLength(array: DecodedArray): Int {
    return when (array) {
        is DecodedArray.ByteArrayType -> array.array.size
        is DecodedArray.IntArrayType -> array.array.size
        is DecodedArray.LongArrayType -> array.array.size
        is DecodedArray.FloatArrayType -> array.array.size
        is DecodedArray.DoubleArrayType -> array.array.size
        is DecodedArray.AnyArrayType -> array.array.size
    }
}

private fun getArrayElement(array: DecodedArray, index: Int): Any? {
    return when (array) {
        is DecodedArray.ByteArrayType -> array.array[index]
        is DecodedArray.IntArrayType -> array.array[index]
        is DecodedArray.LongArrayType -> array.array[index]
        is DecodedArray.FloatArrayType -> array.array[index]
        is DecodedArray.DoubleArrayType -> array.array[index]
        is DecodedArray.AnyArrayType -> array.array.getOrNull(index)
    }
}
