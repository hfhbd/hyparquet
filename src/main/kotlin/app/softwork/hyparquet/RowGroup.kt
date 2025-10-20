package app.softwork.hyparquet

/**
 * Read a row group from the file
 */
fun readRowGroup(
    options: ParquetReadOptions,
    metadata: FileMetaData,
    groupPlan: GroupPlan
): AsyncRowGroup {
    val rowGroup = groupPlan.rowGroup
    val asyncColumns = mutableListOf<AsyncColumn>()
    
    // Get schema tree
    val schemaTree = parquetSchema(metadata.schema)
    
    // Read each column in the row group
    for (column in rowGroup.columns) {
        val metaData = column.meta_data ?: continue
        val path_in_schema = metaData.path_in_schema
        
        // Get schema path for this column
        val schemaPath = try {
            getSchemaPath(metadata.schema, path_in_schema)
        } catch (e: Exception) {
            continue
        }
        
        // Create column decoder
        val columnDecoder = ColumnDecoder(
            columnName = path_in_schema.lastOrNull(),
            type = metaData.type,
            element = schemaPath.last().element,
            schemaPath = schemaPath,
            codec = metaData.codec,
            parsers = options.parsers,
            compressors = options.compressors,
            utf8 = options.utf8
        )
        
        val rowGroupSelect = RowGroupSelect(
            groupStart = groupPlan.groupStart,
            selectStart = groupPlan.selectStart,
            selectEnd = groupPlan.selectEnd,
            groupRows = groupPlan.groupRows
        )
        
        // Create async column
        val asyncColumn = AsyncColumn(
            pathInSchema = path_in_schema,
            data = suspend {
                readColumn(options.file, metaData, columnDecoder, schemaPath, rowGroupSelect, options.onPage)
            }
        )
        
        asyncColumns.add(asyncColumn)
    }
    
    return AsyncRowGroup(
        groupStart = groupPlan.groupStart,
        groupRows = groupPlan.groupRows,
        asyncColumns = asyncColumns
    )
}
