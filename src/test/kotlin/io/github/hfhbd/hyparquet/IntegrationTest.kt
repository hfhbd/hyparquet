package io.github.hfhbd.hyparquet

import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class HyparquetApiTest {

    @Test
    fun testMainApiExists() {
        // Test that the main API object exists and has the expected methods
        assertNotNull(Hyparquet)
        
        // Test that we can create AsyncBuffer from byte array
        val data = byteArrayOf(1, 2, 3, 4, 5)
        val buffer = Hyparquet.fromByteArray(data)
        assertNotNull(buffer)
        assertTrue(buffer.byteLength == 5)
    }

    @Test
    fun testCustomParsers() {
        // Test that we can create custom parsers
        val customParsers = object : ParquetParsers {
            override fun timestampFromMilliseconds(millis: Long) = java.util.Date(millis)
            override fun timestampFromMicroseconds(micros: Long) = java.util.Date(micros / 1000)
            override fun timestampFromNanoseconds(nanos: Long) = java.util.Date(nanos / 1_000_000)
            override fun dateFromDays(days: Int) = java.util.Date(days * 86400000L)
            override fun stringFromBytes(bytes: ByteArray) = String(bytes, Charsets.UTF_8)
        }
        
        assertNotNull(customParsers)
        val result = customParsers.stringFromBytes("test".toByteArray())
        assertTrue(result == "test")
    }
}

class IntegrationTest {

    @Test
    fun testWorkflowStructure() {
        // This test verifies the basic workflow structure without actually parsing parquet files
        // Since we don't have real parquet files in this test environment, we just test the structure
        
        val data = byteArrayOf(1, 2, 3, 4, 5)
        val buffer = Hyparquet.fromByteArray(data)
        
        // Verify we can create read options
        val readOptions = ParquetReadOptions(
            file = buffer,
            columns = listOf("column1", "column2"),
            rowStart = 0,
            rowEnd = 100,
            parsers = DefaultParsers,
            rowFormat = RowFormat.OBJECT
        )
        
        assertNotNull(readOptions)
        assertTrue(readOptions.columns?.size == 2)
        assertTrue(readOptions.rowStart == 0)
        assertTrue(readOptions.rowEnd == 100)
        assertTrue(readOptions.rowFormat == RowFormat.OBJECT)
    }
}