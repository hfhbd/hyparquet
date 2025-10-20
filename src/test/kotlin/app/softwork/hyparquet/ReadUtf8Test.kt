package app.softwork.hyparquet

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ReadUtf8Test {
    
    @Test
    fun `default utf8 behavior`() = runTest {
        val file = asyncBufferFromFile("test/files/strings.parquet")
        val rows = parquetReadObjects(BaseParquetReadOptions(file = file))
        
        assertEquals(4, rows.size)
        assertEquals("alpha", (rows[0] as? Map<*, *>)?.get("bytes"))
        assertEquals("alpha", (rows[0] as? Map<*, *>)?.get("c_utf8"))
        assertEquals("alpha", (rows[0] as? Map<*, *>)?.get("l_utf8"))
        
        assertEquals("bravo", (rows[1] as? Map<*, *>)?.get("bytes"))
        assertEquals("charlie", (rows[2] as? Map<*, *>)?.get("bytes"))
        assertEquals("delta", (rows[3] as? Map<*, *>)?.get("bytes"))
    }
    
    @Test
    fun `utf8 equals true`() = runTest {
        val file = asyncBufferFromFile("test/files/strings.parquet")
        val rows = parquetReadObjects(BaseParquetReadOptions(file = file, utf8 = true))
        
        assertEquals(4, rows.size)
        assertEquals("alpha", (rows[0] as? Map<*, *>)?.get("bytes"))
        assertEquals("alpha", (rows[0] as? Map<*, *>)?.get("c_utf8"))
        assertEquals("bravo", (rows[1] as? Map<*, *>)?.get("bytes"))
        assertEquals("charlie", (rows[2] as? Map<*, *>)?.get("bytes"))
        assertEquals("delta", (rows[3] as? Map<*, *>)?.get("bytes"))
    }
    
    @Test
    fun `utf8 equals false`() = runTest {
        val file = asyncBufferFromFile("test/files/strings.parquet")
        val rows = parquetReadObjects(BaseParquetReadOptions(file = file, utf8 = false))
        
        assertEquals(4, rows.size)
        
        // bytes column should be ByteArray
        val bytesValue0 = (rows[0] as? Map<*, *>)?.get("bytes")
        assertTrue(bytesValue0 is ByteArray, "bytes should be ByteArray when utf8=false")
        if (bytesValue0 is ByteArray) {
            assertEquals("alpha", bytesValue0.decodeToString())
        }
        
        // c_utf8 and l_utf8 should still be strings
        assertEquals("alpha", (rows[0] as? Map<*, *>)?.get("c_utf8"))
        assertEquals("alpha", (rows[0] as? Map<*, *>)?.get("l_utf8"))
        
        val bytesValue1 = (rows[1] as? Map<*, *>)?.get("bytes")
        if (bytesValue1 is ByteArray) {
            assertEquals("bravo", bytesValue1.decodeToString())
        }
    }
}
