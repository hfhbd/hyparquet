package app.softwork.hyparquet

import kotlin.test.Test
import kotlin.test.assertEquals

class SchemaTreeTest {
    
    @Test
    fun `parse schema tree from simple parquet file`() {
        // Test basic schema tree structure
        val schema = listOf(
            SchemaElement(name = "schema", num_children = 1, repetition_type = FieldRepetitionType.REQUIRED, logical_type = null),
            SchemaElement(name = "numbers", repetition_type = FieldRepetitionType.OPTIONAL, type = ParquetType.INT64, logical_type = null)
        )
        
        val schemaTree = parquetSchema(schema)
        
        assertEquals("schema", schemaTree.element.name)
        assertEquals(1, schemaTree.children.size)
        assertEquals("numbers", schemaTree.children[0].element.name)
    }
    
    @Test
    fun `schema tree has correct structure`() {
        val schema = listOf(
            SchemaElement(name = "root", num_children = 2, repetition_type = null, logical_type = null),
            SchemaElement(name = "field1", repetition_type = FieldRepetitionType.OPTIONAL, logical_type = null),
            SchemaElement(name = "field2", repetition_type = FieldRepetitionType.REQUIRED, logical_type = null)
        )
        
        val tree = schemaTree(schema, 0, emptyList())
        
        assertEquals("root", tree.element.name)
        assertEquals(2, tree.children.size)
        assertEquals(3, tree.count)
    }
}
