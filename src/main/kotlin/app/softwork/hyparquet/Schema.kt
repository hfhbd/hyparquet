package app.softwork.hyparquet

/**
 * Build a tree from the schema elements.
 *
 * @param schema
 * @param rootIndex index of the root element
 * @param path path to the element
 * @returns tree of schema elements
 */
fun schemaTree(schema: List<SchemaElement>, rootIndex: Int, path: List<String>): SchemaTree {
    val element = schema[rootIndex]
    val children = mutableListOf<SchemaTree>()
    var count = 1

    // Read the specified number of children
    element.num_children?.let { numChildren ->
        while (children.size < numChildren) {
            val childElement = schema[rootIndex + count]
            val child = schemaTree(schema, rootIndex + count, path + childElement.name)
            count += child.count
            children.add(child)
        }
    }

    return SchemaTree(count = count, element = element, children = children, path = path)
}

/**
 * Get schema elements from the root to the given element name.
 *
 * @param schema
 * @param name path to the element
 * @returns list of schema elements
 */
fun getSchemaPath(schema: List<SchemaElement>, name: List<String>): List<SchemaTree> {
    var tree = schemaTree(schema, 0, emptyList())
    val path = mutableListOf(tree)
    for (part in name) {
        val child = tree.children.find { it.element.name == part }
            ?: throw Error("parquet schema element not found: $name")
        path.add(child)
        tree = child
    }
    return path
}

/**
 * Get the max repetition level for a given schema path.
 *
 * @param schemaPath
 * @returns max repetition level
 */
fun getMaxRepetitionLevel(schemaPath: List<SchemaTree>): Int {
    var maxLevel = 0
    for (schemaElement in schemaPath) {
        if (schemaElement.element.repetition_type == FieldRepetitionType.REPEATED) {
            maxLevel++
        }
    }
    return maxLevel
}

/**
 * Get the max definition level for a given schema path.
 *
 * @param schemaPath
 * @returns max definition level
 */
fun getMaxDefinitionLevel(schemaPath: List<SchemaTree>): Int {
    var maxLevel = 0
    for (schemaElement in schemaPath.drop(1)) {
        if (schemaElement.element.repetition_type != FieldRepetitionType.REQUIRED) {
            maxLevel++
        }
    }
    return maxLevel
}

/**
 * Check if a column is list-like.
 *
 * @param schema
 * @returns true if list-like
 */
fun isListLike(schema: SchemaTree?): Boolean {
    if (schema == null) return false
    if (schema.element.converted_type != ConvertedType.LIST) return false
    if (schema.children.size > 1) return false

    val firstChild = schema.children[0]
    if (firstChild.children.size > 1) return false
    return firstChild.element.repetition_type == FieldRepetitionType.REPEATED
}

/**
 * Check if a column is map-like.
 *
 * @param schema
 * @returns true if map-like
 */
fun isMapLike(schema: SchemaTree?): Boolean {
    if (schema == null) return false
    if (schema.element.converted_type != ConvertedType.MAP) return false
    if (schema.children.size > 1) return false

    val firstChild = schema.children[0]
    if (firstChild.children.size != 2) return false
    if (firstChild.element.repetition_type != FieldRepetitionType.REPEATED) return false

    val keyChild = firstChild.children.find { it.element.name == "key" }
    if (keyChild != null && keyChild.element.repetition_type == FieldRepetitionType.REPEATED) return false

    val valueChild = firstChild.children.find { it.element.name == "value" }
    return valueChild?.element?.repetition_type != FieldRepetitionType.REPEATED
}

/**
 * Returns true if a column is non-nested.
 *
 * @param schemaPath
 * @returns
 */
fun isFlatColumn(schemaPath: List<SchemaTree>): Boolean {
    if (schemaPath.size != 2) return false
    val column = schemaPath[1]
    if (column.element.repetition_type == FieldRepetitionType.REPEATED) return false
    return column.children.isEmpty()
}
