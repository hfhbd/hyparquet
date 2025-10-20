package app.softwork.hyparquet

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AssembleTest {
    
    @Test
    fun `assembleAsync returns same row group`() {
        val schemaTree = SchemaTree(
            emptyList(),
            1,
            SchemaElement(name = "root", repetition_type = null, logical_type = null),
            emptyList()
        )
        
        val asyncRowGroup = AsyncRowGroup(
            groupStart = 0,
            groupRows = 100,
            asyncColumns = emptyList()
        )
        
        val result = assembleAsync(asyncRowGroup, schemaTree)
        
        assertEquals(100, result.groupRows)
        assertEquals(0, result.groupStart)
    }
    
    @Test
    fun `flatten handles empty list`() {
        val result = flatten(null)
        assertTrue(result is DecodedArray.AnyArrayType)
        assertEquals(0, (result as DecodedArray.AnyArrayType).array.size)
    }
    
    @Test
    fun `flatten handles single chunk`() {
        val chunk = DecodedArray.IntArrayType(intArrayOf(1, 2, 3))
        val result = flatten(listOf(chunk))
        assertEquals(chunk, result)
    }
    
    @Test
    fun `flatten combines multiple chunks`() {
        val chunks = listOf(
            DecodedArray.IntArrayType(intArrayOf(1, 2)),
            DecodedArray.IntArrayType(intArrayOf(3, 4))
        )
        val result = flatten(chunks)
        assertTrue(result is DecodedArray.AnyArrayType)
        assertEquals(4, (result as DecodedArray.AnyArrayType).array.size)
    }
}
