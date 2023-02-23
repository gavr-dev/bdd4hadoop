package ru.gavr.bdd4hadoop.assertions;

import org.apache.parquet.tools.read.SimpleRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AssertionsTableTest {

    private List<Map<String, Object>> received;

    @BeforeEach
    public void init() throws Exception {
        received = new ArrayList<>();
        Map<String, Object> rowMap;
        Object[] objects = new Object[] {
                137106,
                new byte[]{42},
                "*",
                7,
                9223372036854775807L,
                false,
                2.0562848119712998,
                new byte[] {-8,-128,-113,124,-103,39,0,0,-120,-122,37,0},
                new byte[] {0,0,0,0,0,0,0,0,-116,61,37,0},
                "12345",
                "abc",
                "",
                new byte[0]
        };
        byte[] base64Bytes = Base64.getEncoder().encode("test".getBytes());
        SimpleRecord simpleRecord = new SimpleRecord();
        simpleRecord.add("name_first", "Ivan");
        simpleRecord.add("name_second", "Ivanov");
        for (Object o:objects) {
            rowMap = new HashMap<>();
            rowMap.put("value", o);
            rowMap.put("null_column", null);
            rowMap.put("str_column", "8706939");
            rowMap.put("int_column", 14234);
            rowMap.put("long_column", 1423423423423L);
            rowMap.put("boolean_column", true);
            rowMap.put("double_column", 2.5);
            rowMap.put("date_column", new byte[] {-8,-128,-113,124,-103,39,0,0,-120,-122,37,0});
            rowMap.put("base64Column", base64Bytes);
            rowMap.put("emptyStr", "");
            rowMap.put("emptyArr", new byte[0]);
            rowMap.put("simplerecord_column", simpleRecord);
            received.add(rowMap);
        }
    }

    @Test
    public void cellEqualsTrueTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellEquals(received, 1, "value", "*");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 2, "value", "*");
        Assertions.assertTrue((Boolean) results.get(0));
    }

    @Test
    public void cellIsEmptyStrTrueTest() {
        ArrayList results = (ArrayList) AssertionsTable.isCellEmptyStr(received, 11, "value");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.isCellEmptyStr(received, 12, "value");
        Assertions.assertTrue((Boolean) results.get(0));
    }

    @Test
    public void cellIsEmptyStrFalseTest() {
        ArrayList results = (ArrayList) AssertionsTable.isCellEmptyStr(received, 1, "value");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.isCellEmptyStr(received, 2, "value");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.isCellEmptyStr(received, 1000, "value");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't row number");

        results = (ArrayList) AssertionsTable.isCellEmptyStr(received, 1, "smth");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field");

        results = (ArrayList) AssertionsTable.isCellEmptyStr(new ArrayList<>(), 0, "value");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("received data is empty");
    }

    @Test
    public void cellEqualsFalseTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellEquals(received, 1, "value", "not");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 2, "value", "not");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 1000, "value", "not");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't row number");

        results = (ArrayList) AssertionsTable.cellEquals(received, 1, "smth", "not");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field");

        results = (ArrayList) AssertionsTable.cellEquals(new ArrayList<>(), 0, "value", "not");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("received data is empty");
    }

    @Test
    public void cellNotEqualsTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellNotEquals(received, 1, "value", "not");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellNotEquals(received, 2, "value", "not");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellNotEquals(received, 1000, "value", "not");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't row number");

        results = (ArrayList) AssertionsTable.cellNotEquals(received, 1, "smth", "not");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field");

        results = (ArrayList) AssertionsTable.cellNotEquals(new ArrayList<>(), 0, "value", "not");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("received data is empty");
    }

    @Test
    public void cellEqualsIntTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellEquals(received, 3,"value", 7);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 3,"value", 8);
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 1000, "value", 8);
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void cellEqualsLongTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellEquals(received, 4, "value", 9223372036854775807L);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 4, "value",8L);
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 1000, "value", 8L);
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void cellEqualsBooleanTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellEquals(received, 5, "value", false);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 5, "value", true);
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 1000, "value", true);
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void cellEqualsDoubleTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellEquals(received, 6, "value",2.0562848119712998);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 6, "value",3.75);
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEquals(received, 1000, "value",4.5);
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void cellEqualsDateTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellEqualsDate(received, 7, "value","2021-02-26T15:05:40.173259");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEqualsDate(received, 7, "value","2021-04-26T15:05:40.173");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEqualsDate(received, 8, "value","1970-01-01T03:00:00.0");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEqualsDate(received, 8, "value","1970-01-01T05:00:00.0");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEqualsDate(received, 1000, "value","1970-01-01T03:00:00.0");
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void cellIsNullTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellIsNull(received, 7, "null_column");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellIsNull(received, 7, "value");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellIsNull(received, 7, "nonxictent_column");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"nonxictent_column\" in row \"7\"");
    }

    @Test
    public void cellStartsWithTest() {
        ArrayList results = (ArrayList) AssertionsTable.cellStartsWith(received, 9, "value","12");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellStartsWith(received, 9, "value","12345");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellStartsWith(received, 10, "value","abcdf");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellStartsWith(received, 10, "value","b");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("starts with \"a\"");

        results = (ArrayList) AssertionsTable.cellStartsWith(received, 1000, "value","*");
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void columnEqualsStr() {
        ArrayList results = (ArrayList) AssertionsTable.columnEquals(received, "str_column", "8706939");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnEquals(received, "nonxictent_column", "8706939");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"nonxictent_column\" in row \"0\"");

        results = (ArrayList) AssertionsTable.columnEquals(received, "str_column", "abc");
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void isColumnEmptyStr() {
        ArrayList results = (ArrayList) AssertionsTable.isColumnEmptyStr(received, "emptyStr");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.isColumnEmptyStr(received, "nonxictent_column");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"nonxictent_column\" in row \"0\"");

        results = (ArrayList) AssertionsTable.isColumnEmptyStr(received, "emptyArr");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.isColumnEmptyStr(received, "str_column");
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void columnEqualsInt() {
        ArrayList results = (ArrayList) AssertionsTable.columnEquals(received, "int_column", 14234);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnEquals(received, "nonxictent_column", 14234);
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"nonxictent_column\" in row \"0\"");

        results = (ArrayList) AssertionsTable.columnEquals(received, "int_column", 1421234);
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void columnEqualsLong() {
        ArrayList results = (ArrayList) AssertionsTable.columnEquals(received, "long_column", 1423423423423L);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnEquals(received, "nonxictent_column", 1423423423423L);
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"nonxictent_column\" in row \"0\"");

        results = (ArrayList) AssertionsTable.columnEquals(received, "long_column", 0L);
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void columnEqualsBoolean() {
        ArrayList results = (ArrayList) AssertionsTable.columnEquals(received, "boolean_column", true);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnEquals(received, "nonxictent_column", true);
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"nonxictent_column\" in row \"0\"");

        results = (ArrayList) AssertionsTable.columnEquals(received, "boolean_column", false);
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void columnEqualsDouble() {
        ArrayList results = (ArrayList) AssertionsTable.columnEquals(received, "double_column", 2.5);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnEquals(received, "nonxictent_column", 2.5);
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"nonxictent_column\" in row \"0\"");

        results = (ArrayList) AssertionsTable.columnEquals(received, "double_column", 2.56);
        Assertions.assertFalse((Boolean) results.get(0));

        received.get(7).put("double_column", 0);
        results = (ArrayList) AssertionsTable.columnEquals(received, "double_column", 2.5);
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("received data in row \"7\" field \"double_column\" equals \"0.0\"");
    }

    @Test
    public void columnEqualsDate() {
        ArrayList results = (ArrayList) AssertionsTable.columnEqualsDate(received, "date_column", "2021-02-26T15:05:40.173259");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnEqualsDate(received, "nonxictent_column", "2021-02-26T15:05:40.173259");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"nonxictent_column\" in row \"0\"");

        results = (ArrayList) AssertionsTable.columnEqualsDate(received, "date_column", "2000-02-26T15:05:40.173259");
        Assertions.assertFalse((Boolean) results.get(0));
    }

    @Test
    public void columnIsNullTest() {
        ArrayList results = (ArrayList) AssertionsTable.columnIsNull(received, "null_column");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnIsNull(received, "value");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnIsNull(received, "nonxictent_column");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"nonxictent_column\" in row \"0\"");
    }

    @Test
    public void columnStartsWith() {
        ArrayList results = (ArrayList) AssertionsTable.columnStartsWith(received, "str_column", "870");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnStartsWith(received, "str_column", "870693924324234");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnStartsWith(received, "str_column", "7");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("starts with \"8\"");
    }

    @Test
    public void columnContains() {
        for (int i = 0; i < received.size() / 2; i++) {
            received.get(i).put("column_contains_str", "abc");
            received.get(i).put("column_contains_int", i);
        }
        for (int i = received.size() / 2; i < received.size(); i++) {
            received.get(i).put("column_contains_str", "erf");
            received.get(i).put("column_contains_int", i);
        }

        ArrayList results = (ArrayList) AssertionsTable.columnContains(received, "column_contains_str", "abc");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnContains(received, "column_contains_str", null);
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data column \"column_contains_str\" contains \"null\"");
        assertThat((String) results.get(2)).contains("received data column \"column_contains_str\" doesn't contain \"null\". Contains: [abc, erf]");

        results = (ArrayList) AssertionsTable.columnContains(received, "column_contains_str", "hello");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data column \"column_contains_str\" contains \"hello\"");
        assertThat((String) results.get(2)).contains("received data column \"column_contains_str\" doesn't contain \"hello\". Contains: [abc, erf]");

        results = (ArrayList) AssertionsTable.columnContains(received, "column_contains_int", 2);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnContains(received, "column_contains_int", 6);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnContains(received, "column_contains_int", 100);
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data column \"column_contains_int\" contains \"100\"");
        assertThat((String) results.get(2)).contains("received data column \"column_contains_int\" doesn't contain \"100\". Contains:");
    }

    @Test
    public void columnNotContains() {
        for (int i = 0; i < received.size() / 2; i++) {
            received.get(i).put("column_contains_str", "abc");
            received.get(i).put("column_contains_int", i);
        }
        for (int i = received.size() / 2; i < received.size(); i++) {
            received.get(i).put("column_contains_str", "erf");
            received.get(i).put("column_contains_int", i);
        }

        ArrayList results = (ArrayList) AssertionsTable.columnNotContains(received, "column_contains_str", null);
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnNotContains(received, "column_contains_str", "123");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.columnNotContains(received, "column_contains_str", "abc");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data column \"column_contains_str\" doesn't contain \"abc\"");
        assertThat((String) results.get(2)).contains("received data column \"column_contains_str\" contain \"abc\". Contains: [abc]");
    }

    @Test
    public void isContainsFieldTest() {
        ArrayList results = (ArrayList) AssertionsTable.isContainsField(received, "value");
        Assertions.assertTrue((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data contains field");
        Assertions.assertTrue(((String) results.get(2)).isEmpty());

        results = (ArrayList) AssertionsTable.isContainsField(received, "not");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data contains field");
        assertThat((String) results.get(2)).contains("received data does not contain field");

        results = (ArrayList) AssertionsTable.isContainsField(new ArrayList<>(), "value");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data contains field");
        assertThat((String) results.get(2)).contains("received data is empty");
    }

    @Test
    public void isNotContainsFieldTest() {
        ArrayList results = (ArrayList) AssertionsTable.isNotContainsField(received, "value");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data does not contain field");
        assertThat((String) results.get(2)).contains("received data contains field");

        results = (ArrayList) AssertionsTable.isNotContainsField(received, "not");
        Assertions.assertTrue((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data does not contain field");

        results = (ArrayList) AssertionsTable.isNotContainsField(new ArrayList<>(), "value");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received data does not contain field");
        assertThat((String) results.get(2)).contains("received data is empty");
    }

    @Test
    public void countRowsTest() {
        ArrayList results = (ArrayList) AssertionsTable.countRows(received, 13);
        assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.countRows(received, 5);
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("received data has 13 rows");

        results = (ArrayList) AssertionsTable.countRows(new ArrayList<>(), 5);
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("received data is empty");

    }

    @Test
    public void cellEqualsBase64Test() {
        ArrayList results = (ArrayList) AssertionsTable.cellEqualsDecodeBase64(received, 1, "base64Column", "test");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEqualsDecodeBase64(received, 1, "base64Column", "not");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellEqualsDecodeBase64(received, 1, "badBase64Column", "test");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field \"badBase64Column\" in row \"1\"");

        results = (ArrayList) AssertionsTable.cellEqualsDecodeBase64(received, 1000, "base64Column", "test");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't row number");

        results = (ArrayList) AssertionsTable.cellEqualsDecodeBase64(received, 1, "base64Column2", "test");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field");

        results = (ArrayList) AssertionsTable.cellEqualsDecodeBase64(new ArrayList<>(), 0, "null_column", "test");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("received data is empty");
    }


    @Test
    public void cellStartsWithBase64Test() {
        ArrayList results = (ArrayList) AssertionsTable.cellStartsWithDecodeBase64(received, 1, "base64Column", "te");
        Assertions.assertTrue((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellStartsWithDecodeBase64(received, 1, "base64Column", "not");
        Assertions.assertFalse((Boolean) results.get(0));

        results = (ArrayList) AssertionsTable.cellStartsWithDecodeBase64(received, 1, "badBase64Column", "test");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field badBase64Column in row \"1\"");

        results = (ArrayList) AssertionsTable.cellStartsWithDecodeBase64(received, 1000, "base64Column", "test");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't row number");

        results = (ArrayList) AssertionsTable.cellStartsWithDecodeBase64(received, 1, "base64Column2", "test");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("there isn't field");

        results = (ArrayList) AssertionsTable.cellStartsWithDecodeBase64(new ArrayList<>(), 0, "null_column", "test");
        Assertions.assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(2)).contains("received data is empty");
    }

    @Test
    public void columnEqualsMap() {
        ArrayList results = (ArrayList) AssertionsTable.columnEquals(received, "simplerecord_column", "[name_first: Ivan, name_second: Ivanov]");
        Assertions.assertTrue((Boolean) results.get(0));
    }

    @Test
    public void tableEquals() throws Exception {

        Map<String, Object> row1 = new HashMap<>();
        row1.put("col_int4", 10);
        row1.put("col_text", "row1");

        Map<String, Object> row2 = new HashMap<>();
        row2.put("col_int4", 20);
        row2.put("col_text", "row2");

        Map<String, Object> row3 = new HashMap<>();
        row3.put("col_int4", 30);
        row3.put("col_text", "row3");

        List<Map<String, Object>> expected = new ArrayList<>();
        expected.add(row1);
        expected.add(row2);
        expected.add(row3);

        List<Map<String, Object>> received = new ArrayList<>();
        received.add(row3);
        received.add(row2);
        received.add(row1);

        ArrayList results = (ArrayList) AssertionsTable.tableEquals(received, expected);
        Assertions.assertTrue((Boolean) results.get(0));

        received.clear();
        received.add(row1);
        received.add(row2);

        results = (ArrayList) AssertionsTable.tableEquals(received, expected);
        Assertions.assertFalse((Boolean) results.get(0));

        received.clear();
        received.add(row1);
        received.add(row2);
        received.add(row2);

        results = (ArrayList) AssertionsTable.tableEquals(received, expected);
        Assertions.assertFalse((Boolean) results.get(0));

        expected.clear();
        expected.add(row1);
        expected.add(row1);
        expected.add(row2);

        results = (ArrayList) AssertionsTable.tableEquals(received, expected);
        Assertions.assertFalse((Boolean) results.get(0));
    }

}