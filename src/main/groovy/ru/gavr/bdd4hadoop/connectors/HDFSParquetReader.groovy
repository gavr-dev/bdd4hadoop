package ru.gavr.bdd4hadoop.connectors


import groovy.util.logging.Commons
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.tools.read.SimpleReadSupport
import org.apache.parquet.tools.read.SimpleRecord

@Commons
class HDFSParquetReader {

    static Integer LIMIT = 20

    static String getPath(String input){
        Integer indexValue = input.lastIndexOf("/")
        String changedPath = input.substring(0, indexValue)
        return changedPath
    }

    static int countDataRows(String input) throws Exception {
        ParquetReader<SimpleRecord> reader = null
        int result = 0
        try {
            reader = ParquetReader.builder(new SimpleReadSupport(), new Path(input)).build()
            while (reader.read() != null) {
                result ++
            }
        } finally {
            if (reader != null){
                reader.close()
            }
        }
        return result
    }

    static List<Map<String, Object>> readData(String input) throws Exception {
      ParquetReader<SimpleRecord> reader = null
      List<Map<String, Object>> resultList = new ArrayList<>()
        try {
            reader = ParquetReader.builder(new SimpleReadSupport(), new Path(input)).build()
            for (int i = 0; i < LIMIT; i++){
                SimpleRecord recordVal = reader.read()
                if (recordVal == null){
                    log.info("There are only " + String.valueOf(i) + " rows")
                    break
                }
                Map<String, Object> rowMap = new HashMap()
                for (SimpleRecord.NameValue nameValue: recordVal.values) {
                    rowMap.put(nameValue.name, nameValue.value)
                }
                resultList.add(rowMap)
            }
        } finally {
            if (reader != null){
                reader.close()
            }
        }
      return resultList
    }

}
