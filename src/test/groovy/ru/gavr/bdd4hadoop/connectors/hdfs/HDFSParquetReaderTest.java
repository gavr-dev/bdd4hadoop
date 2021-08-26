package ru.gavr.bdd4hadoop.connectors.hdfs;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import static org.junit.Assert.*;

public class HDFSParquetReaderTest {

    @Test
    public void readDataMoreFiveParquet() throws Exception {
        final List<Map<String, Object>> values = HDFSParquetReader.readData("src/test/resources/test_files/file.parquet");
        assertEquals(values.size() * values.get(0).size(), 20 * 12);
    }

}