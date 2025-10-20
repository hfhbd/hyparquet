package app.softwork.hyparquet

import kotlin.test.Test
import kotlin.test.assertEquals

class SchemaTest {
    
    @Test
    fun `basic schema tree building`() {
        val schema = listOf(
            SchemaElement(name = "root", num_children = 1, repetition_type = null, logical_type = null),
            SchemaElement(name = "field1", repetition_type = FieldRepetitionType.OPTIONAL, logical_type = null)
        )
        
        val tree = schemaTree(schema, 0, emptyList())
        assertEquals("root", tree.element.name)
        assertEquals(1, tree.children.size)
    }
    
    @Test
    fun `getMaxRepetitionLevel with no repeated fields`() {
        val path = listOf(
            SchemaTree(emptyList(), 1, SchemaElement(name = "root", repetition_type = null, logical_type = null), emptyList()),
            SchemaTree(emptyList(), 1, SchemaElement(name = "field", repetition_type = FieldRepetitionType.OPTIONAL, logical_type = null), listOf("field"))
        )
        assertEquals(0, getMaxRepetitionLevel(path))
    }
    
    @Test
    fun `getMaxDefinitionLevel with optional field`() {
        val path = listOf(
            SchemaTree(emptyList(), 1, SchemaElement(name = "root", repetition_type = null, logical_type = null), emptyList()),
            SchemaTree(emptyList(), 1, SchemaElement(name = "field", repetition_type = FieldRepetitionType.OPTIONAL, logical_type = null), listOf("field"))
        )
        assertEquals(1, getMaxDefinitionLevel(path))
    }
    
    @Test
    fun `isFlatColumn returns true for simple column`() {
        val path = listOf(
            SchemaTree(emptyList(), 1, SchemaElement(name = "root", repetition_type = null, logical_type = null), emptyList()),
            SchemaTree(emptyList(), 1, SchemaElement(name = "field", repetition_type = FieldRepetitionType.OPTIONAL, logical_type = null), listOf("field"))
        )
        assertEquals(true, isFlatColumn(path))
    }
}
