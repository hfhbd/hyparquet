package app.softwork.hyparquet

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

class ReadFilesTest {
    
    private val parquetFiles = getParquetTestFiles()
    
    // Files to skip (as in the original TypeScript test)
    private val skipFiles = setOf(
        "byte_stream_split.zstd.parquet",
        "delta_length_byte_array.parquet",
        "duckdb5533.parquet",
        "nested_structs.rust.parquet"
    )
    
    @Test
    fun `parse data from all parquet test files`() = runTest {
        for (filename in parquetFiles) {
            if (filename in skipFiles) {
                continue
            }
            
            val file = asyncBufferFromFile("test/files/$filename")
            
            parquetRead(ParquetReadOptions(
                file = file,
                onChunk = { _ -> },
                onPage = { _ -> },
                rowFormat = RowFormat.ARRAY,
                utf8 = true,
                compressors = testCompressors,
                onComplete = { rows ->
                    val base = filename.replace(".parquet", "")
                    val expectedFile = File("test/files/$base.json")
                    
                    if (expectedFile.exists()) {
                        val expected = Json.parseToJsonElement(expectedFile.readText())
                        // stringify and parse to make legal json (NaN, -0, etc.)
                        val result = toJson(rows)
                        val resultJson = Json.parseToJsonElement(Json.encodeToString(kotlinx.serialization.json.JsonElement.serializer(), result))
                        assertEquals(expected, resultJson, "Failed for file: $filename")
                    }
                }
            ))
        }
    }
}
